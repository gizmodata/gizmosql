#pragma once

#include <iostream>
#include <arrow/util/logger.h>  // Arrow v21+
#include <functional>
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

// ---- Redacting credentials from log text --------------------------------------
// Log lines and instrumentation rows carry SQL and error messages, and DuckDB's
// error messages repeat the SQL. Statements that carry credentials are found by
// their words, the way DuckDB reads them (comments and string literals are not
// words), so a comment between keywords cannot hide them.

static inline bool is_sql_word_char(char c) {
  const auto u = static_cast<unsigned char>(c);
  return (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') || (u >= '0' && u <= '9') ||
         u == '_' || u >= 0x80;
}

static inline char ascii_upper(char c) {
  return (c >= 'a' && c <= 'z') ? static_cast<char>(c - 32) : c;
}

// Upper-cased words of `s` in order: keywords, names and quoted identifiers,
// without comments (nested /* */ included) or string literals ('', E'',
// $tag$...$tag$). With `raw`, every run of word characters instead, comments and
// literals included, so a misread quote cannot hide a statement either.
static inline std::vector<std::string> sql_words(std::string_view s, bool raw = false) {
  std::vector<std::string> words;
  const size_t n = s.size();
  size_t i = 0;
  while (i < n) {
    const char c = s[i];
    if (!raw && c == '-' && i + 1 < n && s[i + 1] == '-') {
      const size_t nl = s.find('\n', i);
      i = nl == std::string_view::npos ? n : nl + 1;
    } else if (!raw && c == '/' && i + 1 < n && s[i + 1] == '*') {
      int depth = 1;
      i += 2;
      while (i < n && depth > 0) {
        if (s.compare(i, 2, "/*") == 0) {
          ++depth;
          i += 2;
        } else if (s.compare(i, 2, "*/") == 0) {
          --depth;
          i += 2;
        } else {
          ++i;
        }
      }
    } else if (!raw && c == '\'') {
      // E'...' takes backslash escapes; its E was read as a word just before.
      const bool escapes = !words.empty() && words.back() == "E" && i > 0 &&
                           (s[i - 1] == 'E' || s[i - 1] == 'e');
      if (escapes) words.pop_back();
      ++i;
      while (i < n) {
        if (escapes && s[i] == '\\') {
          i += 2;
        } else if (s[i] == '\'') {
          if (i + 1 < n && s[i + 1] == '\'') {
            i += 2;
          } else {
            ++i;
            break;
          }
        } else {
          ++i;
        }
      }
    } else if (!raw && c == '$' && i + 1 < n && !(s[i + 1] >= '0' && s[i + 1] <= '9')) {
      // $tag$ ... $tag$ (a $ followed by a digit is a parameter).
      size_t j = i + 1;
      while (j < n && is_sql_word_char(s[j]) && s[j] != '$') ++j;
      if (j < n && s[j] == '$') {
        const std::string_view tag = s.substr(i, j - i + 1);
        const size_t end = s.find(tag, j + 1);
        i = end == std::string_view::npos ? n : end + tag.size();
      } else {
        ++i;
      }
    } else if (!raw && c == '"') {
      std::string word;
      ++i;
      while (i < n) {
        if (s[i] == '"') {
          if (i + 1 < n && s[i + 1] == '"') {
            word += '"';
            i += 2;
          } else {
            ++i;
            break;
          }
        } else {
          word += ascii_upper(s[i++]);
        }
      }
      words.push_back(std::move(word));
    } else if (is_sql_word_char(c)) {
      std::string word;
      while (i < n && is_sql_word_char(s[i])) word += ascii_upper(s[i++]);
      words.push_back(std::move(word));
    } else {
      ++i;
    }
  }
  return words;
}

// A setting whose value is a credential (keep in step with the access-control
// check of current_setting()).
static inline bool is_credential_setting_name(const std::string& upper_name) {
  for (const char* marker :
       {"PASSWORD", "SECRET", "TOKEN", "CREDENTIAL", "CONNECTION_STRING", "ACCESS_KEY",
        "ACCOUNT_KEY", "API_KEY", "PRIVATE_KEY", "KEY_ID"}) {
    if (upper_name.find(marker) != std::string::npos) return true;
  }
  return false;
}

// Which kind of credential-carrying statement `words` contain, or nullptr.
static inline const char* credential_statement_kind(const std::vector<std::string>& w) {
  const size_t n = w.size();
  for (size_t i = 0; i < n; ++i) {
    if (w[i] == "CREATE") {
      size_t j = i + 1;
      if (j + 1 < n && w[j] == "OR" && w[j + 1] == "REPLACE") j += 2;
      if (j < n && (w[j] == "PERSISTENT" || w[j] == "TEMPORARY" || w[j] == "TEMP")) ++j;
      if (j < n && w[j] == "SECRET") return "secret DDL";
    }
    if ((w[i] == "CREATE" || w[i] == "ALTER") && i + 1 < n && w[i + 1] == "USER")
      return "user DDL";
    if (w[i] == "SET" || w[i] == "PRAGMA") {
      size_t j = i + 1;
      if (j < n && (w[j] == "GLOBAL" || w[j] == "SESSION" || w[j] == "LOCAL")) ++j;
      if (j < n && is_credential_setting_name(w[j])) return "credential setting";
    }
  }
  return nullptr;
}

static inline bool should_redact_secret_sql(std::string_view sql) {
  const char* kind = credential_statement_kind(sql_words(sql));
  if (!kind) kind = credential_statement_kind(sql_words(sql, /*raw=*/true));
  return kind && std::string_view(kind) == "secret DDL";
}

// Replace the value after a credential key (password=..., 'password': '...',
// sig=..., token=...) and the password of a user:password@ URI with ***.
static inline std::string mask_inline_credentials(std::string_view s) {
  static constexpr const char* kKeys[] = {"password",
                                          "passwd",
                                          "pwd",
                                          "accountkey",
                                          "sharedaccesssignature",
                                          "sig",
                                          "token",
                                          "access_token",
                                          "secret",
                                          "x-amz-signature",
                                          "x-amz-security-token",
                                          "x-amz-credential"};
  std::string out;
  out.reserve(s.size());
  const size_t n = s.size();
  auto lower = [](char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c + 32) : c;
  };
  auto key_at = [&](size_t i) -> size_t {  // length of the key starting at i, or 0
    if (i > 0 && (is_sql_word_char(s[i - 1]) || s[i - 1] == '-')) return 0;
    for (const char* key : kKeys) {
      const size_t len = std::char_traits<char>::length(key);
      if (i + len > n) continue;
      bool match = true;
      for (size_t k = 0; k < len && match; ++k) match = lower(s[i + k]) == key[k];
      if (match && (i + len == n || !(is_sql_word_char(s[i + len]) || s[i + len] == '-')))
        return len;
    }
    return 0;
  };
  size_t i = 0;
  while (i < n) {
    // user:password@host in a URI
    if (s.compare(i, 3, "://") == 0) {
      size_t j = i + 3, colon = std::string_view::npos;
      while (j < n &&
             std::string_view("/?#@ \t\r\n'\"").find(s[j]) == std::string_view::npos) {
        if (s[j] == ':' && colon == std::string_view::npos) colon = j;
        ++j;
      }
      if (j < n && s[j] == '@' && colon != std::string_view::npos && colon + 1 < j) {
        out.append(s.substr(i, colon + 1 - i));
        out += "***";
        i = j;
        continue;
      }
    }
    if (const size_t len = key_at(i)) {
      size_t j = i + len;
      // key=value anywhere; 'key': 'value' only with a quoted key (a map or
      // JSON), so prose such as "invalid token: expired" stays readable.
      const bool quoted_key = j < n && (s[j] == '\'' || s[j] == '"');
      if (quoted_key) ++j;
      while (j < n && (s[j] == ' ' || s[j] == '\t')) ++j;
      if (j < n && (s[j] == '=' || (quoted_key && s[j] == ':'))) {
        ++j;
        while (j < n && (s[j] == ' ' || s[j] == '\t')) ++j;
        size_t end = j;
        if (s.compare(j, 2, "''") == 0) {  // a quoted value inside a SQL literal
          const size_t close = s.find("''", j + 2);
          end = close == std::string_view::npos ? n : close + 2;
        } else if (j < n && (s[j] == '\'' || s[j] == '"')) {
          const size_t close = s.find(s[j], j + 1);
          end = close == std::string_view::npos ? n : close + 1;
        } else {
          while (end < n && std::string_view(" \t\r\n'\";&),}").find(s[end]) ==
                                std::string_view::npos)
            ++end;
        }
        if (end > j) {
          out.append(s.substr(i, j - i));
          out += "***";
          i = end;
          continue;
        }
      }
    }
    out += s[i++];
  }
  return out;
}

// Cheap test before the scans above: text without any of these cannot carry a
// credential they would find.
static inline bool might_hold_credentials(std::string_view s) {
  std::string lower(s.size(), '\0');
  for (size_t i = 0; i < s.size(); ++i)
    lower[i] = (s[i] >= 'A' && s[i] <= 'Z') ? static_cast<char>(s[i] + 32) : s[i];
  for (const char* needle :
       {"secret", "user", "set", "pragma", "password", "passwd", "pwd", "token",
        "credential", "connection_string", "accountkey", "sig", "://"}) {
    if (lower.find(needle) != std::string::npos) return true;
  }
  return false;
}

// SQL, an error message or any other text, as it may be logged or recorded: a
// statement that carries credentials (secret DDL, user DDL with a password, a
// credential setting) is replaced as a whole, wherever it appears in the text
// (DuckDB's errors quote the statement); other credentials (connection-string
// passwords, URI passwords, signed-URL tokens) are masked. Applying it twice
// gives the same text.
static inline std::string redact_sql_for_logs(std::string_view sql) {
  if (!might_hold_credentials(sql)) return std::string(sql);
  const char* kind = credential_statement_kind(sql_words(sql));
  if (!kind) kind = credential_statement_kind(sql_words(sql, /*raw=*/true));
  if (kind) return std::string("[REDACTED: ") + kind + "]";
  return mask_inline_credentials(sql);
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

struct TraceCorrelationIds {
  std::string trace_id;
  std::string span_id;
};

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------
void InitLogging(const LogConfig& cfg);
void SetLogLevel(arrow::util::ArrowLogLevel level);
void LogWithFields(arrow::util::ArrowLogLevel level, const char* file, int line,
                   std::string_view msg, const FieldList& fields = {});
// Variant that skips the global logger severity check — used by DYNAMIC_AT macros
// where a caller-supplied threshold (e.g., query_log_level) is the sole gate.
void LogWithFieldsUnchecked(arrow::util::Logger* logger,
                            arrow::util::ArrowLogLevel level, const char* file,
                            int line, std::string_view msg,
                            const FieldList& fields = {});
std::optional<TraceCorrelationIds> GetCurrentTraceCorrelationIds();

/// Set the instance ID for log correlation. Once set, this ID will be included
/// in every log entry (both JSON and text formats). Call this as early as possible
/// after the server instance is created.
void SetInstanceId(const std::string& instance_id);

/// Set the (optional) cluster ID for log correlation. When non-empty, this UUID
/// is included in every log entry (both formats), so logs and instrumentation
/// records from all instances in a cluster can be filtered together. Sourced
/// from the --cluster-id flag / GIZMOSQL_CLUSTER_ID env var.
void SetClusterId(const std::string& cluster_id);

// -----------------------------------------------------------------------------
// Secondary log sinks (e.g. forking logs to a catalog table)
// -----------------------------------------------------------------------------

/// A fully-assembled log record delivered to registered secondary sinks. The
/// common/queryable fields are promoted to members; any remaining structured
/// key/value fields are carried as a JSON object string in `fields_json`.
struct LogRecord {
  std::string timestamp;  // ISO8601 UTC; also the source for a TIMESTAMPTZ column
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_INFO;
  std::string instance_id;  // empty if unset
  std::string cluster_id;   // empty if unset
  std::string session_id;   // promoted from KV (empty if absent)
  std::string username;     // promoted from KV "user"
  std::string role;         // promoted from KV
  std::string peer;         // promoted from KV
  std::string trace_id;
  std::string span_id;
  std::string component;
  std::string func;
  std::string source_file;
  int source_line = 0;
  int pid = 0;
  std::string tid;
  std::string message;
  std::string fields_json;  // remaining KV fields as a JSON object (empty if none)
};

using LogSink = std::function<void(const LogRecord&)>;

/// Register a secondary log sink. Every emitted record (one that passes the
/// level gate) is delivered to each registered sink IN ADDITION to stdout/file
/// — this is the "fork". Sinks MUST be cheap and non-blocking (e.g. enqueue to
/// a writer thread); a thread-local recursion guard drops any log emitted from
/// within sink delivery so a sink can never recursively log into itself.
/// Thread-safe.
void RegisterLogSink(LogSink sink);

/// Remove all registered sinks. Call before a sink object is destroyed (e.g. at
/// shutdown) so no further records are delivered to it.
void ClearLogSinks();

/// RAII guard that marks the CURRENT thread as "executing inside a log sink" for
/// its lifetime. While active, any log emitted on this thread is delivered only
/// to the primary stdout/file sink and is NEVER re-dispatched to the secondary
/// sinks. Construct one at the top of a sink's own writer-thread loop so that a
/// log the sink emits (e.g. reporting a failed catalog write) cannot recursively
/// enqueue back into the sink. The dispatch path sets the same guard around
/// each sink callback, so synchronous re-entry from a sink callback is also
/// covered. Nesting-safe: restores the previous value on destruction.
class ScopedLogSinkGuard {
 public:
  ScopedLogSinkGuard();
  ~ScopedLogSinkGuard();

  ScopedLogSinkGuard(const ScopedLogSinkGuard&) = delete;
  ScopedLogSinkGuard& operator=(const ScopedLogSinkGuard&) = delete;

 private:
  bool previous_;
};

// Temporarily override trace correlation IDs for logs on the current thread.
// This is used by async statement execution paths when OTel runtime context
// may no longer carry the parent span.
class ScopedLogCorrelation {
 public:
  ScopedLogCorrelation(std::string trace_id, std::string span_id);
  ~ScopedLogCorrelation();

  ScopedLogCorrelation(const ScopedLogCorrelation&) = delete;
  ScopedLogCorrelation& operator=(const ScopedLogCorrelation&) = delete;
  ScopedLogCorrelation(ScopedLogCorrelation&&) = delete;
  ScopedLogCorrelation& operator=(ScopedLogCorrelation&&) = delete;

 private:
  bool active_ = false;
  std::optional<TraceCorrelationIds> previous_;
};

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

// Function-scope BEGIN/END logging with timing — conditional on IsEnabled().
//
// The scope guard that emits the END line MUST live at the enclosing function's
// scope. It used to be declared inside the `if (enabled)` block, so it was
// destroyed at that block's closing brace — the END line fired immediately after
// BEGIN, before the function body ran, always with duration_ms=0 and the default
// status "error" (callers set STATUS_VAR = "success" later, which nobody saw).
// Now the enabled check is captured once at BEGIN and re-used by the guard.
#define GIZMOSQL_LOG_SCOPE_STATUS(SEV, OPERATION, STATUS_VAR, ...)                       \
  STATUS_VAR = "initial";                                                                \
  const bool GIZMOSQL_CONCAT(_gizmosql_enabled_, __LINE__) = GIZMOSQL_LOG_ENABLED(SEV);  \
  const auto GIZMOSQL_CONCAT(_gizmosql_start_, __LINE__) = std::chrono::steady_clock::now(); \
  if (GIZMOSQL_CONCAT(_gizmosql_enabled_, __LINE__)) {                                   \
    GIZMOSQL_LOGKV(SEV, OPERATION " - BEGIN", {"operation", std::string(OPERATION)},     \
                   {"kind", "function-scope-lifecycle"}, {"lifecycle", "begin"},         \
                   {"status", STATUS_VAR}, __VA_ARGS__);                                 \
    STATUS_VAR = "error";                                                                \
  }                                                                                      \
  const auto GIZMOSQL_CONCAT(_gizmosql_guard_, __LINE__) = ::gizmosql::MakeScopeGuard(   \
      [&, _gizmosql_enabled_local = GIZMOSQL_CONCAT(_gizmosql_enabled_, __LINE__),       \
       _gizmosql_start_local = GIZMOSQL_CONCAT(_gizmosql_start_, __LINE__)] {            \
        if (!_gizmosql_enabled_local) return;                                            \
        auto _gizmosql_end = std::chrono::steady_clock::now();                           \
        auto _gizmosql_ms = std::chrono::duration_cast<std::chrono::milliseconds>(       \
                                _gizmosql_end - _gizmosql_start_local)                   \
                                .count();                                                \
        GIZMOSQL_LOGKV(SEV, OPERATION " - END", {"operation", std::string(OPERATION)},   \
                       {"kind", "function-scope-lifecycle"}, {"lifecycle", "end"},       \
                       {"status", STATUS_VAR},                                           \
                       {"duration_ms", std::to_string(_gizmosql_ms)}, __VA_ARGS__);      \
      })

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

// Dynamic-level logging with separate threshold and display severity.
// THRESHOLD is the component's minimum level (e.g., auth_log_level).
// DISPLAY_SEV is the message's natural severity (e.g., INFO for routine logs).
// The message is emitted only when DISPLAY_SEV >= both the component THRESHOLD
// and the overall logger severity, ensuring that setting a component threshold
// to ERROR suppresses INFO-level messages rather than promoting them to ERROR.
#define GIZMOSQL_LOGKV_DYNAMIC_AT(THRESHOLD, DISPLAY_SEV, MSG, ...)      \
  do {                                                                   \
    auto _logger_sp = ::arrow::util::LoggerRegistry::GetDefaultLogger(); \
    auto* _logger = _logger_sp.get();                                    \
    if (_logger && (DISPLAY_SEV >= THRESHOLD) &&                         \
        (DISPLAY_SEV >= _logger->severity_threshold())) {                \
      ::gizmosql::LogWithFields(DISPLAY_SEV, __FILE__, __LINE__, MSG,    \
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

// Session-aware dynamic-level logging with separate threshold and display severity.
// THRESHOLD is the component's minimum level (e.g., query_log_level).
// DISPLAY_SEV is the message's natural severity (e.g., INFO for routine logs).
// See GIZMOSQL_LOGKV_DYNAMIC_AT for full semantics.
#define GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(THRESHOLD, DISPLAY_SEV, SESSION, MSG, ...) \
  do {                                                                               \
    auto _logger_sp = ::arrow::util::LoggerRegistry::GetDefaultLogger();             \
    auto* _logger = _logger_sp.get();                                                \
    if (_logger && (DISPLAY_SEV >= THRESHOLD)) {                                     \
      ::gizmosql::LogWithFieldsUnchecked(_logger, DISPLAY_SEV,                       \
          __FILE__, __LINE__, MSG,                                                   \
          ::gizmosql::FieldList{SESSION_KV_FIELDS(SESSION), __VA_ARGS__});           \
    }                                                                                \
  } while (0)

// Convenience: session-aware dynamic-level logging where the threshold IS the display level
#define GIZMOSQL_LOGKV_SESSION_DYNAMIC(SEV, SESSION, MSG, ...) \
    GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(SEV, SEV, SESSION, MSG, __VA_ARGS__)

// Session-aware variant of GIZMOSQL_LOGKV_DYNAMIC_AT: gates on BOTH the
// component THRESHOLD and the global logger severity (mirrors auth-style
// behavior, unlike GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT which bypasses the
// global threshold). Use this for server-wide thresholds that should still
// defer to --log-level (e.g., session_log_level).
#define GIZMOSQL_LOGKV_SESSION_AT(THRESHOLD, DISPLAY_SEV, SESSION, MSG, ...) \
  do {                                                                       \
    auto _logger_sp = ::arrow::util::LoggerRegistry::GetDefaultLogger();     \
    auto* _logger = _logger_sp.get();                                        \
    if (_logger && (DISPLAY_SEV >= THRESHOLD) &&                             \
        (DISPLAY_SEV >= _logger->severity_threshold())) {                    \
      ::gizmosql::LogWithFields(DISPLAY_SEV, __FILE__, __LINE__, MSG,        \
          ::gizmosql::FieldList{SESSION_KV_FIELDS(SESSION), __VA_ARGS__});   \
    }                                                                        \
  } while (0)

}  // namespace gizmosql
