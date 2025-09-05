#include "include/detail/gizmosql_logging.h"

#include <arrow/util/logger.h>
#include <atomic>
#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <iostream>

#if defined(_WIN32)
#include <process.h>
static inline int gizmosql_getpid() { return _getpid(); }
#else
#include <unistd.h>
static inline int gizmosql_getpid() { return getpid(); }
#endif

#include <nlohmann/json.hpp>

namespace gizmosql {
namespace {

using arrow::util::ArrowLogLevel;
using arrow::util::LogDetails;
using arrow::util::Logger;
using arrow::util::LoggerRegistry;

struct GlobalState {
  std::mutex sink_mu;
  std::unique_ptr<std::ostream> owned_sink;  // if file-backed
  std::ostream* sink = &std::cerr;

  std::atomic<ArrowLogLevel> level{ArrowLogLevel::ARROW_INFO};
  LogConfig cfg;
};

GlobalState G;

// ---------- helpers

inline std::string NowIso8601Utc() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const auto secs = time_point_cast<std::chrono::seconds>(now);
  const auto ms = duration_cast<milliseconds>(now - secs).count();

  std::time_t t = system_clock::to_time_t(now);
  std::tm tm_utc{};
#if defined(_WIN32)
  gmtime_s(&tm_utc, &t);
#else
  gmtime_r(&t, &tm_utc);
#endif

  std::ostringstream oss;
  oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3)
      << std::setfill('0') << ms << "Z";
  return oss.str();
}

inline const char* LevelName(ArrowLogLevel lvl) {
  using L = ArrowLogLevel;
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

// ---------- custom logger (Arrow v21+)

class GizmoSQLLogger final : public Logger {
 public:
  explicit GizmoSQLLogger() = default;

  bool is_enabled() const override { return true; }

  ArrowLogLevel severity_threshold() const override { return G.level.load(); }

  void Log(const LogDetails& d) override {
    // Extract basics from LogDetails
    const auto lvl = d.severity;
    const std::string& file = d.source_location.file;
    const int line = static_cast<int>(d.source_location.line);
    const std::string_view& msg = d.message;

    if (G.cfg.format == LogFormat::kJson) {
      WriteJson(lvl, file, line, msg);
      return;
    }
    WriteText(lvl, file, line, msg);
  }

 private:
  static void WriteJson(ArrowLogLevel level, const std::string file, int line,
                        const std::string_view& message) {
    nlohmann::json j;
    j["ts"] = NowIso8601Utc();
    j["lvl"] = LevelName(level);
    j["pid"] = gizmosql_getpid();

    std::ostringstream tid;
    tid << std::this_thread::get_id();
    j["tid"] = tid.str();

    if (G.cfg.show_source) { j["file"] = file; j["line"] = line; }
    if (G.cfg.component) j["component"] = *G.cfg.component;

    // Pull out optional [func=...] tag (from GIZMOSQL_LOGF) into a proper field
    std::string func;
    std::string text = std::string(message);
    constexpr char kTag[] = "[func=";
    auto pos = text.find(kTag);
    if (pos != std::string::npos) {
      auto end = text.find(']', pos);
      if (end != std::string::npos) {
        func = text.substr(pos + sizeof(kTag) - 1, end - (pos + sizeof(kTag) - 1));
        text.erase(pos, (end - pos) + 1);
        if (!text.empty() && text.front() == ' ') text.erase(0, 1);
      }
    }
    if (!func.empty()) j["func"] = func;

    j["msg"] = text;

    std::lock_guard<std::mutex> lk(G.sink_mu);
    (*G.sink) << j.dump() << '\n';
    if (G.cfg.flush_each_line) G.sink->flush();
  }

  static void WriteText(ArrowLogLevel level, const std::string file, int line,
                        const std::string_view& message) {
    std::ostringstream tid;
    tid << std::this_thread::get_id();

    std::ostringstream oss;
    oss << NowIso8601Utc() << " " << LevelName(level) << " pid=" << gizmosql_getpid()
        << " tid=" << tid.str();

    if (G.cfg.component) oss << " component=" << *G.cfg.component;

    if (G.cfg.show_source) oss << " " << file << ":" << line;
    oss << " - " << message;

    std::lock_guard<std::mutex> lk(G.sink_mu);
    (*G.sink) << oss.str() << '\n';
    if (G.cfg.flush_each_line) G.sink->flush();
  }
};

std::shared_ptr<GizmoSQLLogger> g_logger;

}  // namespace

// ---------- public API

void InitLogging(const LogConfig& cfg) {
  G.cfg = cfg;
  G.level.store(cfg.level);

  // Configure sink
  if (cfg.file_path) {
    auto out = std::make_unique<std::ofstream>(*cfg.file_path, std::ios::app);
    if (!*out) {
      G.owned_sink.reset();
      G.sink = &std::cerr;
      // At this early stage we don't have a logger yet; write a plain warning.
      std::cerr << "WARN Failed to open log file '" << *cfg.file_path
                << "', logging to stderr\n";
    } else {
      G.sink = out.get();
      G.owned_sink = std::move(out);
    }
  } else {
    G.owned_sink.reset();
    G.sink = &std::cerr;
  }

  // Install our logger into Arrow's registry (v21+ API)
  g_logger = std::make_shared<GizmoSQLLogger>();
  LoggerRegistry::SetDefaultLogger(g_logger);
}

void SetLogLevel(ArrowLogLevel level) {
  G.level.store(level);
  // Arrow will consult severity_threshold() on our logger,
  // so just updating G.level is sufficient.
}

}  // namespace gizmosql
