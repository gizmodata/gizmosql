#pragma once

#include <arrow/util/logger.h>  // Arrow v21+
#include <optional>
#include <sstream>
#include <string>
#include <variant>
#include <vector>
#include <regex>
#include <string_view>

namespace gizmosql {

static inline std::string_view lstrip_ws(std::string_view s) {
  size_t i = 0;
  while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n'))
    ++i;
  return s.substr(i);
}

// Optional: strip *leading* SQL comments so logs can’t be tricked with a comment prefix
static inline std::string_view strip_leading_comments(std::string_view s) {
  s = lstrip_ws(s);
  for (;;) {
    if (s.substr(0, 2) == "--") {
      auto nl = s.find('\n');
      if (nl == std::string_view::npos) return {};
      s = lstrip_ws(s.substr(nl + 1));
    } else if (s.substr(0, 2) == "/*") {
      auto end = s.find("*/");
      if (end == std::string_view::npos) return {};
      s = lstrip_ws(s.substr(end + 2));
    } else {
      break;
    }
  }
  return s;
}

static inline bool should_redact_secret_sql(std::string_view sql) {
  sql = strip_leading_comments(sql);

  // Matches (case-insensitive):
  //   CREATE SECRET ...
  //   CREATE OR REPLACE SECRET ...
  //   CREATE PERSISTENT SECRET ...
  //   CREATE OR REPLACE PERSISTENT SECRET ...
  // ^\s*CREATE\s+(OR\s+REPLACE\s+)?(PERSISTENT\s+)?SECRET\b
  static const std::regex secret_re(
      R"(^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:PERSISTENT\s+)?SECRET\b)",
      std::regex::icase | std::regex::optimize);

  return std::regex_search(sql.begin(), sql.end(), secret_re);
}

static inline std::string redact_sql_for_logs(std::string_view sql) {
  return should_redact_secret_sql(sql) ? "[REDACTED: secret DDL]" : std::string(sql);
}

enum class LogFormat { kText, kJson };

struct LogConfig {
  LogFormat format = LogFormat::kText;  // runtime: text or json
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_INFO;
  std::optional<std::string> file_path{};  // if set, write to file; else stderr
  std::optional<std::string> component{};  // optional tag (e.g., "flight_server")
  bool show_source = false;
  bool flush_each_line = true;
};

using FieldValue = std::variant<std::string, int64_t, double, bool>;

struct Field {
  std::string key;
  FieldValue value;
};

using FieldList = std::vector<Field>;

// Initialize the global logger (install into LoggerRegistry)
void InitLogging(const LogConfig& cfg);

// Adjust the severity threshold at runtime
void SetLogLevel(arrow::util::ArrowLogLevel level);

// New: structured logging entry point
void LogWithFields(arrow::util::ArrowLogLevel level, const char* file, int line,
                   std::string_view msg, const FieldList& fields = {});

template <typename F>
class ScopeGuard {
 public:
  explicit ScopeGuard(F f) : f_(std::move(f)), active_(true) {}
  ~ScopeGuard() {
    if (active_) {
      f_();
    }
  }

  void Dismiss() { active_ = false; }

 private:
  F f_;
  bool active_;
};

template <typename F>
ScopeGuard<F> MakeScopeGuard(F f) {
  return ScopeGuard<F>(std::move(f));
}

// Convenience macros that route through Arrow’s ARROW_LOG,
// which will use our installed logger.
#define GIZMOSQL_LOG(SEV)                                                       \
  (::arrow::util::LogMessage(::arrow::util::ArrowLogLevel::ARROW_##SEV,         \
                             ::arrow::util::LoggerRegistry::GetDefaultLogger(), \
                             ::arrow::util::SourceLocation{__FILE__, __LINE__}) \
       .Stream())

#define GIZMOSQL_LOGF(SEV) GIZMOSQL_LOG(SEV) << "[func=" << __func__ << "] "

#define GIZMOSQL_LOGKV(SEV, MSG, ...)                                            \
  ::gizmosql::LogWithFields(::arrow::util::ArrowLogLevel::ARROW_##SEV, __FILE__, \
                            __LINE__, MSG, ::gizmosql::FieldList{__VA_ARGS__})

#define GIZMOSQL_CONCAT_INNER(a, b) a##b
#define GIZMOSQL_CONCAT(a, b) GIZMOSQL_CONCAT_INNER(a, b)

// STATUS_VAR is a std::string local you define in the function.
#define GIZMOSQL_LOG_SCOPE_STATUS(SEV, OPERATION, STATUS_VAR, ...)                     \
  STATUS_VAR = "initial";                                                              \
  auto GIZMOSQL_CONCAT(_gizmosql_start_, __LINE__) = std::chrono::steady_clock::now(); \
  GIZMOSQL_LOGKV(SEV, OPERATION " - BEGIN", {"operation", std::string(OPERATION)},     \
                 {"kind", "function-scope-lifecycle"}, {"lifecycle", "begin"},         \
                 {"status", STATUS_VAR}, __VA_ARGS__);                                 \
  STATUS_VAR = "error";                                                                \
  auto GIZMOSQL_CONCAT(_gizmosql_guard_, __LINE__) = ::gizmosql::MakeScopeGuard(       \
      [&, _gizmosql_start_local = GIZMOSQL_CONCAT(_gizmosql_start_, __LINE__)] {       \
        auto _gizmosql_end = std::chrono::steady_clock::now();                         \
        auto _gizmosql_ms = std::chrono::duration_cast<std::chrono::milliseconds>(     \
                                _gizmosql_end - _gizmosql_start_local)                 \
                                .count();                                              \
        GIZMOSQL_LOGKV(SEV, OPERATION " - END", {"operation", std::string(OPERATION)}, \
                       {"kind", "function-scope-lifecycle"}, {"lifecycle", "end"},     \
                       {"status", STATUS_VAR},                                         \
                       {"duration_ms", std::to_string(_gizmosql_ms)}, __VA_ARGS__);    \
      })
#define GIZMOSQL_LOGKV_DYNAMIC(SEV, MSG, ...)             \
  ::gizmosql::LogWithFields(SEV, __FILE__, __LINE__, MSG, \
                            ::gizmosql::FieldList{__VA_ARGS__})
}  // namespace gizmosql