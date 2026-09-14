// Result streaming: a result larger than one DuckDB vector (2048 rows) must
// arrive as many Arrow record batches, in order, complete, and with every
// column converted correctly, through both the ad-hoc and prepared paths.
// Oracles are closed-form expressions of the row index, so the check does not
// depend on the server to compute its own answer key.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr int64_t kRows = 100000;
// 2026-01-01 00:00:00 as microseconds since the Unix epoch (TIMESTAMP is naive).
constexpr int64_t kBaseTimestampMicros = 1767225600LL * 1000000LL;

class ResultStreamingFixture
    : public gizmosql::testing::ServerTestFixture<ResultStreamingFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "result_streaming_tester.db",
        .port = 31640,
        .health_port = 31641,
        .username = "tester",
        .password = "tester",
        .init_sql_commands =
            "CREATE TABLE stream_rows AS "
            "SELECT i AS id, 'row-' || i AS label, "
            "       CASE WHEN i % 7 = 0 THEN NULL ELSE (i * 1.5)::DOUBLE END AS value, "
            "       TIMESTAMP '2026-01-01 00:00:00' + to_seconds(i) AS ts, "
            "       (i % 3 = 0) AS flag "
            "FROM range(100000) t(i);",
    };
  }

 protected:
  std::unique_ptr<FlightSqlClient> sql_client_;
  arrow::flight::FlightCallOptions call_options_;

  void SetUp() override {
    ASSERT_TRUE(IsServerReady()) << "Server not ready";
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(
                                   location, arrow::flight::FlightClientOptions{}));
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options_.headers.push_back(bearer);
    sql_client_ = std::make_unique<FlightSqlClient>(std::move(client));
  }

  struct StreamStats {
    int64_t batches = 0;
    int64_t rows = 0;
    int64_t max_batch_rows = 0;
  };

  // Drains a DoGet stream batch by batch (never ToTable, which would hide the
  // batching) and verifies the id column is the contiguous sequence
  // first_id, first_id + 1, ... across batch boundaries.
  static StreamStats DrainAndCheckIds(arrow::flight::FlightStreamReader& stream,
                                      int64_t first_id) {
    StreamStats stats;
    int64_t expected = first_id;
    while (true) {
      auto chunk = stream.Next();
      EXPECT_TRUE(chunk.ok()) << chunk.status().ToString();
      if (!chunk.ok() || chunk->data == nullptr) break;
      const auto& batch = *chunk->data;
      ++stats.batches;
      stats.rows += batch.num_rows();
      stats.max_batch_rows = std::max(stats.max_batch_rows, batch.num_rows());
      ASSERT_TYPE_OK(batch, 0, arrow::Type::INT64);
      auto ids = std::static_pointer_cast<arrow::Int64Array>(batch.column(0));
      for (int64_t k = 0; k < ids->length(); ++k, ++expected) {
        if (ids->Value(k) != expected) {
          ADD_FAILURE() << "id out of sequence in batch " << stats.batches << " row " << k
                        << ": got " << ids->Value(k) << " expected " << expected;
          return stats;
        }
      }
    }
    return stats;
  }

  static void ASSERT_TYPE_OK(const arrow::RecordBatch& batch, int column,
                             arrow::Type::type expected) {
    ASSERT_EQ(batch.column(column)->type_id(), expected)
        << "column " << column << " is " << batch.column(column)->type()->ToString();
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<ResultStreamingFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<ResultStreamingFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<ResultStreamingFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<ResultStreamingFixture>::config_{};

TEST_F(ResultStreamingFixture, AdHocQueryStreamsManyBatchesWithEveryColumnIntact) {
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client_->Execute(
                     call_options_,
                     "SELECT id, label, value, ts, flag FROM stream_rows ORDER BY id"));
  ASSERT_EQ(info->endpoints().size(), 1u);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));

  int64_t batches = 0, rows = 0, max_batch_rows = 0, nulls = 0;
  double value_sum = 0;
  while (true) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto chunk, stream->Next());
    if (chunk.data == nullptr) break;
    const auto& batch = *chunk.data;
    ++batches;
    max_batch_rows = std::max(max_batch_rows, batch.num_rows());
    ASSERT_EQ(batch.num_columns(), 5);
    ASSERT_EQ(batch.column(0)->type_id(), arrow::Type::INT64);
    ASSERT_TRUE(batch.column(1)->type_id() == arrow::Type::STRING ||
                batch.column(1)->type_id() == arrow::Type::LARGE_STRING)
        << batch.column(1)->type()->ToString();
    ASSERT_EQ(batch.column(2)->type_id(), arrow::Type::DOUBLE);
    ASSERT_EQ(batch.column(3)->type_id(), arrow::Type::TIMESTAMP);
    ASSERT_EQ(batch.column(4)->type_id(), arrow::Type::BOOL);
    auto ids = std::static_pointer_cast<arrow::Int64Array>(batch.column(0));
    auto values = std::static_pointer_cast<arrow::DoubleArray>(batch.column(2));
    auto ts = std::static_pointer_cast<arrow::TimestampArray>(batch.column(3));
    auto flags = std::static_pointer_cast<arrow::BooleanArray>(batch.column(4));
    for (int64_t k = 0; k < batch.num_rows(); ++k, ++rows) {
      const int64_t id = ids->Value(k);
      ASSERT_EQ(id, rows) << "batch " << batches << " row " << k;
      if (id % 7 == 0) {
        ASSERT_TRUE(values->IsNull(k)) << "id " << id;
        ++nulls;
      } else {
        ASSERT_FALSE(values->IsNull(k)) << "id " << id;
        ASSERT_DOUBLE_EQ(values->Value(k), id * 1.5);
        value_sum += values->Value(k);
      }
      ASSERT_EQ(ts->Value(k), kBaseTimestampMicros + id * 1000000LL) << "id " << id;
      ASSERT_EQ(flags->Value(k), id % 3 == 0) << "id " << id;
    }
    // Labels: first and last row of every batch, so every batch boundary is
    // checked without a hundred thousand string conversions.
    for (int64_t k : {int64_t{0}, batch.num_rows() - 1}) {
      ASSERT_ARROW_OK_AND_ASSIGN(auto label, batch.column(1)->GetScalar(k));
      ASSERT_EQ(label->ToString(), "row-" + std::to_string(ids->Value(k)));
    }
  }
  EXPECT_EQ(rows, kRows);
  EXPECT_EQ(nulls, (kRows + 6) / 7);
  // Sum of id*1.5 over ids not divisible by 7: closed form.
  double expected_sum = 0;
  for (int64_t i = 0; i < kRows; ++i)
    if (i % 7 != 0) expected_sum += i * 1.5;
  EXPECT_DOUBLE_EQ(value_sum, expected_sum);
  // Many batches, none larger than one DuckDB vector (STANDARD_VECTOR_SIZE).
  EXPECT_GT(batches, 10) << "result was not streamed in batches";
  EXPECT_LE(max_batch_rows, 2048);
}

TEST_F(ResultStreamingFixture, PreparedQueryStreamsManyBatchesInOrder) {
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(
          call_options_, "SELECT id, label FROM stream_rows WHERE id >= ? ORDER BY id"));
  auto bind = [](int64_t n) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.Append(n).ok());
    std::shared_ptr<arrow::Array> values;
    EXPECT_TRUE(builder.Finish(&values).ok());
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("p", arrow::int64())}), 1,
                                    {values});
  };

  // Full table through the prepared path.
  ASSERT_ARROW_OK(prepared->SetParameters(bind(0)));
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, prepared->Execute(call_options_));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
  auto stats = DrainAndCheckIds(*stream, 0);
  EXPECT_EQ(stats.rows, kRows);
  EXPECT_GT(stats.batches, 10);
  EXPECT_LE(stats.max_batch_rows, 2048);

  // Re-bind and re-execute the same handle: a different, still multi-batch tail.
  ASSERT_ARROW_OK(prepared->SetParameters(bind(90000)));
  ASSERT_ARROW_OK_AND_ASSIGN(auto tail, prepared->Execute(call_options_));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tail_stream, sql_client_->DoGet(call_options_, tail->endpoints()[0].ticket));
  auto tail_stats = DrainAndCheckIds(*tail_stream, 90000);
  EXPECT_EQ(tail_stats.rows, 10000);
  EXPECT_GT(tail_stats.batches, 1);
  ASSERT_ARROW_OK(prepared->Close(call_options_));
}
