#include "gizmosql_logging.h"

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

// Marker prefix to smuggle structured fields through Arrow's Logger message
static constexpr const char* kKV_PREFIX = "[[KV]]";

struct GlobalState {
  std::mutex sink_mu;
  std::unique_ptr<std::ostream> owned_sink; // if file-backed
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
  const auto us = duration_cast<microseconds>(now - secs).count();

  std::time_t t = system_clock::to_time_t(now);
  std::tm tm_utc{};
#if defined(_WIN32)
  gmtime_s(&tm_utc, &t);
#else
  gmtime_r(&t, &tm_utc);
#endif

  std::ostringstream oss;
  oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(6)
      << std::setfill('0') << us << "Z";
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

// Encode fields into a compact JSON string (only for transport inside message)
inline std::string EncodeFieldsForMessage(const FieldList& fields) {
  if (fields.empty()) return {};
  nlohmann::json j = nlohmann::json::object();
  for (const auto& f : fields) {
    std::visit([&](auto&& v) {
      j[f.key] = v;
    }, f.value);
  }
  return std::string(kKV_PREFIX) + j.dump() + " ";
}

// Try to peel off the encoded fields prefix from the message
inline std::optional<nlohmann::json> TryExtractFieldsFromMessage(std::string& text) {
  if (text.rfind(kKV_PREFIX, 0) != 0) return std::nullopt; // no prefix

  const size_t prefix_len = std::strlen(kKV_PREFIX);
  if (text.size() <= prefix_len || text[prefix_len] != '{') return std::nullopt;

  // Find the end of the JSON object by tracking depth, respecting quotes/escapes
  size_t i = prefix_len;
  int depth = 0;
  bool in_string = false;
  bool escaped = false;

  for (; i < text.size(); ++i) {
    char c = text[i];

    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '"') {
        in_string = false;
      }
      continue;
    }

    if (c == '"') {
      in_string = true;
      continue;
    }
    if (c == '{') {
      ++depth;
    } else if (c == '}') {
      --depth;
      if (depth == 0) {
        // i is the position of the matching closing brace
        size_t json_end = i + 1; // past '}'
        // Optional single space delimiter after the JSON
        size_t erase_upto = json_end;
        if (json_end < text.size() && text[json_end] == ' ') {
          erase_upto = json_end + 1;
        }

        std::string json_str = text.substr(prefix_len, json_end - prefix_len);
        text.erase(0, erase_upto);

        try {
          return nlohmann::json::parse(json_str);
        } catch (...) {
          return std::nullopt;
        }
      }
    }
  }

  // If we get here, we never closed the top-level object
  return std::nullopt;
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

    if (G.cfg.show_source) {
      j["file"] = file;
      j["line"] = line;
    }
    if (G.cfg.component) j["component"] = *G.cfg.component;

    // Pull func tag and the encoded KV prefix
    std::string text = std::string(message);

    // 1) Extract encoded fields if present
    if (auto kv = TryExtractFieldsFromMessage(text)) {
      // Merge into top-level (don’t overwrite required keys)
      for (auto it = kv->begin(); it != kv->end(); ++it) {
        if (!j.contains(it.key())) j[it.key()] = *it;
        else j["extra_" + it.key()] = *it; // avoid collisions
      }
    }

    // 2) Extract [func=...] tag like before
    std::string func;
    constexpr char kTag[] = "[func=";
    if (auto pos = text.find(kTag); pos != std::string::npos) {
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

  static inline std::string QuoteIfNeeded(const std::string& s) {
    // quote if whitespace present
    for (char c : s)
      if (std::isspace(static_cast<unsigned char>(c)))
        return
            "\"" + s + "\"";
    return s;
  }

  static void AppendFieldsText(std::ostringstream& oss, const nlohmann::json& kv) {
    for (auto it = kv.begin(); it != kv.end(); ++it) {
      oss << " " << it.key() << "=";
      if (it->is_string()) {
        oss << QuoteIfNeeded(it->get<std::string>());
      } else {
        oss << it->dump(); // numbers/bools
      }
    }
  }

  static void WriteText(ArrowLogLevel level, const std::string file, int line,
                        const std::string_view& message) {
    std::ostringstream tid;
    tid << std::this_thread::get_id();

    std::string text = std::string(message);
    nlohmann::json kv;
    if (auto extracted = TryExtractFieldsFromMessage(text)) {
      kv = std::move(*extracted);
    }

    std::ostringstream oss;
    oss << NowIso8601Utc() << " " << LevelName(level) << " pid=" << gizmosql_getpid()
        << " tid=" << tid.str();

    if (G.cfg.component) oss << " component=" << *G.cfg.component;
    if (G.cfg.show_source) oss << " " << file << ":" << line;

    if (!kv.is_null()) {
      AppendFieldsText(oss, kv);
    }

    // Optional func tag extraction (reuse the JSON logic if you like; omitted here)
    oss << " - " << text;

    std::lock_guard<std::mutex> lk(G.sink_mu);
    (*G.sink) << oss.str() << '\n';
    if (G.cfg.flush_each_line) G.sink->flush();
  }
};

std::shared_ptr<GizmoSQLLogger> g_logger;
} // namespace

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

// ---------- public API (cont.) ----------

void LogWithFields(arrow::util::ArrowLogLevel level,
                   const char* file,
                   int line,
                   std::string_view msg,
                   const FieldList& fields) {
  auto logger = arrow::util::LoggerRegistry::GetDefaultLogger();
  if (!logger || !logger->is_enabled() || level < logger->severity_threshold()) {
    return;
  }

  // Prepend encoded KV if present
  std::string full = EncodeFieldsForMessage(fields);

  full.append(msg.data(), msg.size());

  arrow::util::LogDetails d;
  d.severity = level;
  d.message = full;
  d.source_location.file = file;
  d.source_location.line = static_cast<uint32_t>(line);
  logger->Log(d);
}
} // namespace gizmosql