#pragma once

#include <iostream>
#include <arrow/util/logger.h>  // Arrow v21+
#include <optional>
#include <sstream>
#include <string>
#include <variant>
#include <vector>
#include <regex>
#include <string_view>
#include <chrono>

namespace gizmosql {

// -----------------------------------------------------------------------------
// String utilities
// -----------------------------------------------------------------------------
static inline std::string_view lstrip_ws(std::string_view s) {
  size_t i = 0;
  while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n'))
    ++i;
  return s.substr(i);
}

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
  static const std::regex secret_re(
      R"(^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:PERSISTENT\s+)?SECRET\b)",
      std::regex::icase | std::regex::optimize);
  return std::regex_search(sql.begin(), sql.end(), secret_re);
}

static inline std::string redact_sql_for_logs(std::string_view sql) {
  return should_redact_secret_sql(sql) ? "[REDACTED: secret DDL]" : std::string(sql);
}

// -----------------------------------------------------------------------------
// Log configuration
// -----------------------------------------------------------------------------
enum class LogFormat { kText, kJson };

struct LogConfig {
  LogFormat format = LogFormat::kText;
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_INFO;
  std::optional<std::string> file_path{};
  std::optional<std::string> component{};
  bool show_source = false;
  bool flush_each_line = true;
};

using FieldValue = std::variant<std::string, int64_t, double, bool>;

struct Field {
  std::string key;
  FieldValue value;
};

using FieldList = std::vector<Field>;

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------
void InitLogging(const LogConfig& cfg);
void SetLogLevel(arrow::util::ArrowLogLevel level);
void LogWithFields(arrow::util::ArrowLogLevel level, const char* file, int line,
                   std::string_view msg, const FieldList& fields = {});

/// Set the instance ID for log correlation. Once set, this ID will be included
/// in every log entry (both JSON and text formats). Call this as early as possible
/// after the server instance is created.
void SetInstanceId(const std::string& instance_id);

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------
static inline auto lower = [](std::string s) {
  for (auto& c : s) c = static_cast<char>(std::tolower((unsigned char)c));
  return s;
};

static inline arrow::util::ArrowLogLevel log_level_string_to_arrow_log_level(
    const std::string& lvl_s) {
  using L = arrow::util::ArrowLogLevel;
  auto v = lower(lvl_s);
  if (v == "debug") return L::ARROW_DEBUG;
  if (v == "info") return L::ARROW_INFO;
  if (v == "warn" || v == "warning") return L::ARROW_WARNING;
  if (v == "error") return L::ARROW_ERROR;
  if (v == "fatal") return L::ARROW_FATAL;
  std::cerr << "Unknown log-level '" << lvl_s << "', defaulting to INFO\n";
  return L::ARROW_INFO;
}

static inline std::string log_level_arrow_log_level_to_string(
    arrow::util::ArrowLogLevel lvl) {
  using L = arrow::util::ArrowLogLevel;
  switch (lvl) {
    case L::ARROW_DEBUG:
      return "DEBUG";
    case L::ARROW_INFO:
      return "INFO";
    case L::ARROW_WARNING:
      return "WARN";
    case L::ARROW_ERROR:
      return "ERROR";
    case L::ARROW_FATAL:
      return "FATAL";
    default:
      return "INFO";
  }
}

// -----------------------------------------------------------------------------
// Scope guard utility (RAII cleanup helper)
// -----------------------------------------------------------------------------
template <typename F>
class ScopeGuard {
 public:
  explicit ScopeGuard(F f) : f_(std::move(f)), active_(true) {}
  ~ScopeGuard() {
    if (active_) f_();
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

// -----------------------------------------------------------------------------
// Logging Macros
// -----------------------------------------------------------------------------

// ✅ Check if a given severity level is enabled (runtime, via severity_threshold)
#define GIZMOSQL_LOG_ENABLED(SEV)                                        \
  ([] {                                                                  \
    auto _logger_sp = ::arrow::util::LoggerRegistry::GetDefaultLogger(); \
    auto* _logger = _logger_sp.get();                                    \
    if (!_logger) return true; /* safe fallback */                       \
    using _Lvl = ::arrow::util::ArrowLogLevel;                           \
    return _Lvl::ARROW_##SEV >= _logger->severity_threshold();           \
  })()

// Basic log message (stream interface)
#define GIZMOSQL_LOG(SEV)                                                       \
  (::arrow::util::LogMessage(::arrow::util::ArrowLogLevel::ARROW_##SEV,         \
                             ::arrow::util::LoggerRegistry::GetDefaultLogger(), \
                             ::arrow::util::SourceLocation{__FILE__, __LINE__}) \
       .Stream())

#define GIZMOSQL_LOGF(SEV) GIZMOSQL_LOG(SEV) << "[func=" << __func__ << "] "

// Structured key/value logging — only runs when enabled
#define GIZMOSQL_LOGKV(SEV, MSG, ...)                                                \
  do {                                                                               \
    if (GIZMOSQL_LOG_ENABLED(SEV)) {                                                 \
      ::gizmosql::LogWithFields(::arrow::util::ArrowLogLevel::ARROW_##SEV, __FILE__, \
                                __LINE__, MSG, ::gizmosql::FieldList{__VA_ARGS__});  \
    }                                                                                \
  } while (0)

// String concatenation helpers
#define GIZMOSQL_CONCAT_INNER(a, b) a##b
#define GIZMOSQL_CONCAT(a, b) GIZMOSQL_CONCAT_INNER(a, b)

// Function-scope BEGIN/END logging with timing — conditional on IsEnabled()
#define GIZMOSQL_LOG_SCOPE_STATUS(SEV, OPERATION, STATUS_VAR, ...)                       \
  STATUS_VAR = "initial";                                                                \
  if (GIZMOSQL_LOG_ENABLED(SEV)) {                                                       \
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
        });                                                                              \
  }

// Dynamic-level logging variant (for ArrowLogLevel values)
#define GIZMOSQL_LOGKV_DYNAMIC(SEV, MSG, ...)                            \
  do {                                                                   \
    auto _logger_sp = ::arrow::util::LoggerRegistry::GetDefaultLogger(); \
    auto* _logger = _logger_sp.get();                                    \
    if (_logger && (SEV >= _logger->severity_threshold())) {             \
      ::gizmosql::LogWithFields(SEV, __FILE__, __LINE__, MSG,            \
                                ::gizmosql::FieldList{__VA_ARGS__});     \
    }                                                                    \
  } while (0)

// -----------------------------------------------------------------------------
// Session-aware logging macros (DRY helpers for ClientSession context)
// -----------------------------------------------------------------------------
// These macros automatically include session_id, user, role, and peer
// from a ClientSession pointer, reducing repetition in logging calls.
// Note: instance_id is logged once at server startup (process-level), not per-session.

// Common session KV fields - expands to Field initializers for use in FieldList
// Usage: SESSION_KV_FIELDS(client_session) expands to the 4 common session fields
#define SESSION_KV_FIELDS(s)         \
    {"session_id", (s)->session_id}, \
    {"user", (s)->username},         \
    {"role", (s)->role},             \
    {"peer", (s)->peer}

// Session-aware structured logging - includes all session fields automatically
// Usage: GIZMOSQL_LOGKV_SESSION(INFO, client_session, "message", {"extra", "field"})
#define GIZMOSQL_LOGKV_SESSION(SEV, SESSION, MSG, ...) \
    GIZMOSQL_LOGKV(SEV, MSG, SESSION_KV_FIELDS(SESSION), __VA_ARGS__)

// Session-aware dynamic-level logging variant
#define GIZMOSQL_LOGKV_SESSION_DYNAMIC(SEV, SESSION, MSG, ...)           \
  do {                                                                   \
    auto _logger_sp = ::arrow::util::LoggerRegistry::GetDefaultLogger(); \
    auto* _logger = _logger_sp.get();                                    \
    if (_logger && (SEV >= _logger->severity_threshold())) {             \
      ::gizmosql::LogWithFields(SEV, __FILE__, __LINE__, MSG,            \
          ::gizmosql::FieldList{SESSION_KV_FIELDS(SESSION), __VA_ARGS__}); \
    }                                                                    \
  } while (0)

}  // namespace gizmosql
