#include "duckdb_type_info.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/sql/types.h>
#include <arrow/record_batch.h>
#include <arrow/util/rows_to_batches.h>

#include "../common/include/detail/flight_sql_fwd.h"

namespace sql = flight::sql;

namespace gizmosql::ddb
{
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoGetTypeInfoResult()
    {
        auto schema = sql::SqlSchema::GetXdbcTypeInfoSchema();
        using ValueType =
            std::variant<bool, int32_t, std::nullptr_t, const char*, std::vector<const char*>>;

        auto VariantConverter = [](arrow::ArrayBuilder& array_builder, const ValueType& value)
        {
            if (std::holds_alternative<bool>(value))
            {
                return dynamic_cast<arrow::BooleanBuilder&>(array_builder)
                    .Append(std::get<bool>(value));
            }
            else if (std::holds_alternative<int32_t>(value))
            {
                return dynamic_cast<arrow::Int32Builder&>(array_builder)
                    .Append(std::get<int32_t>(value));
            }
            else if (std::holds_alternative<std::nullptr_t>(value))
            {
                return array_builder.AppendNull();
            }
            else if (std::holds_alternative<const char*>(value))
            {
                return dynamic_cast<arrow::StringBuilder&>(array_builder)
                    .Append(std::get<const char*>(value));
            }
            else
            {
                auto& list_builder = dynamic_cast<arrow::ListBuilder&>(array_builder);
                ARROW_RETURN_NOT_OK(list_builder.Append());
                auto value_builder =
                    dynamic_cast<arrow::StringBuilder*>(list_builder.value_builder());
                for (const auto& v : std::get<std::vector<const char*>>(value))
                {
                    ARROW_RETURN_NOT_OK(value_builder->Append(v));
                }
                return arrow::Status::OK();
            }
        };

        std::vector<std::vector<ValueType>> rows = {
            {
                "boolean", 16, 1, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "boolean", 0, 0, 16, 0, 0, 0
            },
            {
                "tinyint", -6, 3, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "tinyint", 0, 0, -6, 0, 0, 0
            },
            {
                "smallint", 5, 5, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "smallint", 0, 0, 5, 0, 0, 0
            },
            {
                "integer", 4, 10, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "integer", 0, 0, 4, 0, 0, 0
            },
            {
                "bigint", -5, 19, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "bigint", 0, 0, -5, 0, 0, 0
            },
            {
                "hugeint", -5, 38, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "hugeint", 0, 0, -5, 0, 0, 0
            },
            {
                "float", 6, 7, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3, false,
                false, false, "float", 0, 0, 6, 0, 0, 0
            },
            {
                "double", 8, 15, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "double", 0, 0, 8, 0, 0, 0
            },
            {
                "decimal", 3, 38, nullptr, nullptr, std::vector<const char*>({}), 1, false, 3,
                false, false, false, "decimal", 0, 0, 3, 0, 0, 0
            },
            {
                "varchar", 12, 255, "'", "'", std::vector<const char*>({"length"}), 1, false, 3,
                false, false, false, "varchar", 0, 0, 12, 0, 0, 0
            },
            {
                "text", -1, 65536, "'", "'", std::vector<const char*>({"length"}), 1, false, 3,
                false, false, false, "text", 0, 0, -1, 0, 0, 0
            },
            {
                "date", 91, 10, "'", "'", std::vector<const char*>({}), 1, false, 3, false, false,
                false, "date", 0, 0, 91, 0, 0, 0
            },
            {
                "time", 92, 8, "'", "'", std::vector<const char*>({}), 1, false, 3, false, false,
                false, "time", 0, 0, 92, 0, 0, 0
            },
            {
                "timestamp", 93, 32, "'", "'", std::vector<const char*>({}), 1, false, 3, false,
                false, false, "timestamp", 0, 0, 93, 0, 0, 0
            },
        };

        ARROW_ASSIGN_OR_RAISE(auto reader, RowsToBatches(schema, rows, VariantConverter));
        return reader->Next();
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoGetTypeInfoResult(
        int data_type_filter)
    {
        ARROW_ASSIGN_OR_RAISE(auto record_batch, DoGetTypeInfoResult());

        std::vector<int> data_type_vector{16, -6, 5, 4, -5, -5, 6, 8, 3, 12, -1, 91, 92, 93};

        auto pair = std::equal_range(data_type_vector.begin(), data_type_vector.end(),
                                     data_type_filter);

        return record_batch->Slice(pair.first - data_type_vector.begin(),
                                   pair.second - pair.first);
    }
} // namespace gizmosql::ddb