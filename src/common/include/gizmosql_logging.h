#pragma once

#include <arrow/util/logger.h>  // Arrow v21+
#include <optional>
#include <ostream>
#include <string>

namespace gizmosql {

enum class LogFormat { kText, kJson };

struct LogConfig {
  LogFormat format = LogFormat::kText;  // runtime: text or json
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_INFO;
  std::optional<std::string> file_path{};  // if set, write to file; else stderr
  std::optional<std::string> component{};  // optional tag (e.g., "flight_server")
  bool flush_each_line = true;
};

// Initialize the global logger (install into LoggerRegistry)
void InitLogging(const LogConfig& cfg);

// Adjust the severity threshold at runtime
void SetLogLevel(arrow::util::ArrowLogLevel level);

// Convenience macros that route through Arrow’s ARROW_LOG,
// which will use our installed logger.
#define GIZMOSQL_LOG(sev) ARROW_LOG(sev)
#define GIZMOSQL_LOGF(sev) ARROW_LOG(sev) << "[func=" << __func__ << "] "

}  // namespace gizmosql
