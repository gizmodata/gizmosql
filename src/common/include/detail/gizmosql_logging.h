#pragma once

#include <arrow/util/logger.h>  // Arrow v21+
#include <optional>
#include <sstream>
#include <string>
#include <variant>
#include <vector>
#include <string_view>

namespace gizmosql {
enum class LogFormat { kText, kJson };

struct LogConfig {
  LogFormat format = LogFormat::kText; // runtime: text or json
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_INFO;
  std::optional<std::string> file_path{}; // if set, write to file; else stderr
  std::optional<std::string> component{}; // optional tag (e.g., "flight_server")
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
void LogWithFields(arrow::util::ArrowLogLevel level,
                   const char* file,
                   int line,
                   std::string_view msg,
                   const FieldList& fields = {});

// Convenience macros that route through Arrow’s ARROW_LOG,
// which will use our installed logger.
#define GIZMOSQL_LOG(SEV)                                                \
(::arrow::util::LogMessage(                                              \
::arrow::util::ArrowLogLevel::ARROW_##SEV,                               \
::arrow::util::LoggerRegistry::GetDefaultLogger(),                       \
::arrow::util::SourceLocation{__FILE__, __LINE__})                       \
.Stream())

#define GIZMOSQL_LOGF(SEV)                                               \
GIZMOSQL_LOG(SEV) << "[func=" << __func__ << "] "

#define GIZMOSQL_LOGKV(SEV, MSG, ...) \
::gizmosql::LogWithFields(::arrow::util::ArrowLogLevel::ARROW_##SEV, \
__FILE__, __LINE__, MSG, \
::gizmosql::FieldList{ __VA_ARGS__ } )

#define GIZMOSQL_LOGKV_DYNAMIC(SEV, MSG, ...) \
::gizmosql::LogWithFields(SEV, __FILE__, __LINE__, MSG, \
::gizmosql::FieldList{ __VA_ARGS__ } )
} // namespace gizmosql