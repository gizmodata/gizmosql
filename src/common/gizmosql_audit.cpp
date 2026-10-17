// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gizmosql_audit.h"

#include <chrono>
#include <cstdlib>
#include <map>
#include <mutex>
#include <sstream>

#include <arrow/util/logger.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

namespace gizmosql {

namespace {

constexpr const char* kChain = "audit_chain";
constexpr const char* kSeq = "audit_seq";
constexpr const char* kPrev = "audit_prev";
constexpr const char* kHash = "audit_hash";
constexpr const char* kKeys = "audit_keys";
constexpr const char* kAlg = "audit_alg";
const std::string kGenesis(64, '0');

std::string Hex(const unsigned char* bytes, size_t n) {
  static const char* digits = "0123456789abcdef";
  std::string out;
  out.reserve(n * 2);
  for (size_t i = 0; i < n; ++i) {
    out.push_back(digits[bytes[i] >> 4]);
    out.push_back(digits[bytes[i] & 0xf]);
  }
  return out;
}

std::string Digest(const std::string& key, const std::string& data) {
  unsigned char out[EVP_MAX_MD_SIZE];
  unsigned int len = 0;
  if (key.empty()) {
    EVP_Digest(data.data(), data.size(), out, &len, EVP_sha256(), nullptr);
  } else {
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(), out, &len);
  }
  return Hex(out, len);
}

std::string ValueText(const FieldValue& value) {
  struct {
    std::string operator()(const std::string& s) const { return s; }
    std::string operator()(int64_t v) const { return std::to_string(v); }
    std::string operator()(double v) const { return nlohmann::json(v).dump(); }
    std::string operator()(bool v) const { return v ? "true" : "false"; }
  } visit;
  return std::visit(visit, value);
}

// What a record's digest covers: the previous digest, the message, and each
// covered field as length-prefixed key and value, in key order.
std::string Canonical(const std::string& prev, std::string_view msg,
                      const std::map<std::string, std::string>& fields) {
  std::string out = prev;
  out += '\n';
  out += std::to_string(msg.size());
  out += ':';
  out.append(msg.data(), msg.size());
  out += '\n';
  for (const auto& [key, value] : fields) {
    out += std::to_string(key.size()) + ':' + key + std::to_string(value.size()) + ':' + value +
           '\n';
  }
  return out;
}

struct ChainState {
  std::mutex mu;
  std::string id;
  std::string key;
  int64_t seq = 0;
  std::string prev = kGenesis;
};

ChainState& Chain() {
  static ChainState state;
  return state;
}

std::string NewChainId() {
  unsigned char bytes[8];
  if (RAND_bytes(bytes, sizeof(bytes)) != 1) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    for (size_t i = 0; i < sizeof(bytes); ++i) bytes[i] = static_cast<unsigned char>(now >> (8 * i));
  }
  return Hex(bytes, sizeof(bytes));
}

// Add the chain fields to `fields` and its digest.
void Seal(FieldList& fields, std::string_view msg) {
  auto& chain = Chain();
  std::lock_guard<std::mutex> lock(chain.mu);
  if (chain.id.empty()) chain.id = NewChainId();
  const int64_t seq = ++chain.seq;
  fields.push_back({kChain, chain.id});
  fields.push_back({kSeq, seq});
  fields.push_back({kPrev, chain.prev});
  fields.push_back({kAlg, chain.key.empty() ? "sha256" : "hmac-sha256"});
  // The JSON log keeps the last value of a repeated key.
  std::map<std::string, std::string> covered;
  for (const auto& f : fields) covered[f.key] = ValueText(f.value);
  std::string keys;
  for (const auto& [k, v] : covered) keys += (keys.empty() ? "" : ",") + k;
  covered[kKeys] = keys + "," + kKeys;
  fields.push_back({kKeys, covered[kKeys]});
  chain.prev = Digest(chain.key, Canonical(chain.prev, msg, covered));
  fields.push_back({kHash, chain.prev});
}

bool Enabled(arrow::util::ArrowLogLevel level) {
  auto logger = arrow::util::LoggerRegistry::GetDefaultLogger();
  return logger && logger->is_enabled() && level >= logger->severity_threshold();
}

std::vector<std::string> Split(const std::string& s, char sep) {
  std::vector<std::string> out;
  std::string part;
  std::istringstream in(s);
  while (std::getline(in, part, sep)) out.push_back(part);
  return out;
}

std::string JsonText(const nlohmann::json& v) {
  if (v.is_string()) return v.get<std::string>();
  if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
  if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
  return v.dump();
}

}  // namespace

std::string AuditChainKeyFromEnvironment() {
  const char* value = std::getenv("GIZMOSQL_AUDIT_CHAIN_KEY");
  return value ? std::string(value) : std::string();
}

void StartAuditChain(const std::string& key) {
  {
    auto& chain = Chain();
    std::lock_guard<std::mutex> lock(chain.mu);
    chain.id = NewChainId();
    chain.key = key;
    chain.seq = 0;
    chain.prev = kGenesis;
  }
  LogAuditRecord(arrow::util::ArrowLogLevel::ARROW_INFO, __FILE__, __LINE__,
                 "Access-control audit chain started",
                 {{"kind", "access_control"}, {"keyed", key.empty() ? "false" : "true"}});
}

void LogAuditRecord(arrow::util::ArrowLogLevel level, const char* file, int line,
                    std::string_view msg, FieldList fields) {
  auto logger = arrow::util::LoggerRegistry::GetDefaultLogger();
  if (!Enabled(level)) return;
  Seal(fields, msg);
  LogWithFieldsUnchecked(logger.get(), level, file, line, msg, fields);
}

AuditChainReport VerifyAuditLog(std::istream& in, const std::string& key, bool partial) {
  AuditChainReport report;
  struct Seen {
    int64_t first = 0, last = 0, records = 0;
    std::string prev;
  };
  std::map<std::string, Seen> chains;
  std::map<std::string, std::map<int64_t, std::pair<int64_t, nlohmann::json>>> by_chain;
  std::string text;
  int64_t line_number = 0;
  while (std::getline(in, text)) {
    ++line_number;
    if (text.empty() || text[0] != '{') continue;
    nlohmann::json record = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (record.is_discarded() || !record.is_object() || !record.contains(kHash)) continue;
    const auto where = "line " + std::to_string(line_number);
    if (!record.contains(kChain) || !record[kChain].is_string() || !record.contains(kSeq) ||
        !record[kSeq].is_number_integer()) {
      report.problems.push_back(where + ": an audit record without its chain id or sequence");
      continue;
    }
    const auto chain = record[kChain].get<std::string>();
    const auto seq = record[kSeq].get<int64_t>();
    auto& records = by_chain[chain];
    if (records.count(seq)) {
      report.problems.push_back(where + ": chain " + chain + " has sequence " +
                                std::to_string(seq) + " twice (also line " +
                                std::to_string(records[seq].first) + ")");
      continue;
    }
    records[seq] = {line_number, std::move(record)};
  }
  for (auto& [chain, records] : by_chain) {
    std::string prev;
    int64_t expected = records.begin()->first;
    if (expected != 1 && !partial) {
      report.problems.push_back("chain " + chain + " starts at sequence " +
                                std::to_string(expected) + ": its first records are missing");
    }
    for (auto& [seq, entry] : records) {
      auto& [line, record] = entry;
      const auto where = "line " + std::to_string(line) + " (chain " + chain + ", sequence " +
                         std::to_string(seq) + ")";
      ++report.records;
      if (seq != expected) {
        report.problems.push_back(where + ": sequence " + std::to_string(expected) +
                                  (seq - expected > 1 ? " to " + std::to_string(seq - 1) : "") +
                                  " is missing");
      }
      expected = seq + 1;
      const std::string claimed_prev = record.value(kPrev, "");
      if (!prev.empty() && claimed_prev != prev) {
        report.problems.push_back(where + ": does not follow the record before it");
      } else if (prev.empty() && seq == 1 && claimed_prev != kGenesis) {
        report.problems.push_back(where + ": the first record does not start the chain");
      }
      const std::string alg = record.value(kAlg, "");
      if (!key.empty() && alg != "hmac-sha256") {
        report.problems.push_back(where + ": not sealed with the key");
      } else if (key.empty() && alg == "hmac-sha256") {
        report.problems.push_back(where + ": sealed with a key; pass GIZMOSQL_AUDIT_CHAIN_KEY to check it");
      }
      std::map<std::string, std::string> covered;
      bool complete = true;
      for (const auto& k : Split(record.value(kKeys, ""), ',')) {
        if (!record.contains(k)) {
          report.problems.push_back(where + ": the field " + k + " is missing");
          complete = false;
          continue;
        }
        covered[k] = JsonText(record[k]);
      }
      if (!covered.count(kKeys) || !covered.count(kChain) || !covered.count(kSeq) ||
          !covered.count(kPrev)) {
        report.problems.push_back(where + ": the record does not cover its chain fields");
        complete = false;
      }
      const std::string digest =
          Digest(key, Canonical(claimed_prev, record.value("msg", ""), covered));
      const std::string claimed = record.value(kHash, "");
      if (complete && digest != claimed) {
        report.problems.push_back(where + ": was changed after it was written");
      }
      prev = claimed;
    }
    report.chains.push_back(chain + ": sequence " + std::to_string(records.begin()->first) + " to " +
                            std::to_string(records.rbegin()->first));
  }
  return report;
}

}  // namespace gizmosql
