// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
#include "metrics_service.h"
#include "enterprise/enterprise_features.h"
#include "detail/cgroup_limits.h"
#include "detail/shutdown_state.h"
#include "version.h"
// Match the other cpp-httplib translation units: this macro changes class
// layouts even when this particular listener uses plaintext HTTP.
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <duckdb/main/config.hpp>
#include <openssl/pem.h>
#include <cmath>
#include <fstream>
#include <limits>
#include <sstream>
#ifndef _WIN32
#include <sys/resource.h>
#include <unistd.h>
#endif
#ifdef __APPLE__
#include <mach/mach.h>
#include <libproc.h>
#include <sys/sysctl.h>
#endif
#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#include <tlhelp32.h>
#endif
namespace gizmosql::enterprise {
namespace {
double Now() {
  return std::chrono::duration<double>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
double ProcessStartTime() {
#ifdef __linux__
  // /proc/self/stat field 2 is parenthesized and can contain spaces or ')'.
  // Field 22 is the process start time in clock ticks since system boot.
  std::ifstream process_stat("/proc/self/stat");
  std::string line;
  if (!std::getline(process_stat, line)) return NAN;
  const auto end_name = line.rfind(')');
  if (end_name == std::string::npos) return NAN;
  std::istringstream fields(line.substr(end_name + 1));
  std::string ignored;
  for (int field = 3; field < 22; ++field)
    if (!(fields >> ignored)) return NAN;
  double start_ticks = 0;
  const auto ticks_per_second = sysconf(_SC_CLK_TCK);
  if (!(fields >> start_ticks) || ticks_per_second <= 0) return NAN;
  std::ifstream system_stat("/proc/stat");
  while (std::getline(system_stat, line)) {
    if (line.rfind("btime ", 0) != 0) continue;
    std::istringstream boot_field(line.substr(6));
    double boot_time = 0;
    if (boot_field >> boot_time) return boot_time + start_ticks / ticks_per_second;
    break;
  }
#endif
  // Other platforms fill this from native process information during Collect().
  // Never substitute collector startup time for the actual process start time.
  return NAN;
}
double FileSize(const std::filesystem::path& path) {
  std::error_code ec;
  const auto n = std::filesystem::file_size(path, ec);
  return ec ? 0 : static_cast<double>(n);
}
}  // namespace
MetricsService::MetricsService(std::shared_ptr<duckdb::DuckDB> db,
                               std::filesystem::path database, std::string certificate,
                               int session_limit,
                               std::function<void(MetricsRegistry&)> sample_server)
    : registry_(std::make_shared<MetricsRegistry>()),
      db_(std::move(db)),
      connection_(std::make_unique<duckdb::Connection>(*db_)),
      database_(std::move(database)),
      sample_server_(std::move(sample_server)),
      http_(std::make_unique<httplib::Server>()) {
  registry_->Add("gizmosql_build_info", "gauge", "GizmoSQL build information.",
                 {{"version", PROJECT_VERSION},
                  {"duckdb_version", duckdb::DuckDB::LibraryVersion()},
                  {"edition", "Enterprise"}},
                 1);
  const std::pair<const char*, const char*> gauges[] = {
      {"gizmosql_duckdb_memory_used_bytes",
       "DuckDB buffer manager memory usage in bytes."},
      {"gizmosql_duckdb_memory_limit_bytes", "Effective DuckDB memory limit in bytes."},
      {"gizmosql_duckdb_threads", "Effective DuckDB worker thread setting."},
      {"gizmosql_duckdb_temp_storage_bytes",
       "DuckDB evicted temporary storage in bytes."},
      {"gizmosql_duckdb_spill_files", "Live DuckDB temporary spill files."},
      {"gizmosql_duckdb_spill_bytes", "Live DuckDB temporary spill bytes."},
      {"gizmosql_draining", "One while the server is draining for shutdown."},
      {"gizmosql_health_check_status",
       "Effective cached gRPC health status; one is serving."},
      {"gizmosql_health_check_duration_seconds",
       "Duration of the last completed internal health query."},
      {"gizmosql_database_file_bytes", "Main database file size in bytes."},
      {"gizmosql_wal_bytes", "Main database write ahead log size in bytes."},
      {"process_resident_memory_bytes", "Current process resident memory in bytes."},
      {"process_open_fds", "Current process open file descriptors."},
      {"process_threads", "Current process thread count."},
      {"gizmosql_host_memory_bytes", "Physical host memory in bytes."},
      {"gizmosql_host_cpus", "Host logical CPU count."},
      {"gizmosql_metrics_collection_success",
       "One if the last background metrics refresh succeeded."},
      {"gizmosql_metrics_last_collection_success_seconds",
       "Unix timestamp of the last successful metrics refresh."},
      {"gizmosql_metrics_collection_duration_seconds",
       "Duration of the last background metrics refresh."}};
  for (auto [name, help] : gauges)
    registry_->Add(name, "gauge", help, {}, std::numeric_limits<double>::quiet_NaN());
  registry_->Add("process_cpu_seconds_total", "counter",
                 "Process user plus system CPU time in seconds.");
  registry_->Add("process_start_time_seconds", "gauge",
                 "Process start time as a Unix timestamp.", {}, ProcessStartTime());
  registry_->Add(
      "gizmosql_last_exit_unclean", "gauge",
      "One if the preceding metrics-enabled run for this database did not stop cleanly.",
      {}, NAN);
  registry_->Add("gizmosql_unclean_exits_total", "counter",
                 "Detected unclean exits persisted beside this database.", {}, NAN);
#ifdef _WIN32
  registry_->Add("process_open_handles", "gauge", "Current Windows process handle count.",
                 {}, NAN);
#endif
  registry_->Add("gizmosql_disk_free_bytes", "gauge",
                 "Available filesystem space in bytes.", {{"path", "database"}}, NAN);
  registry_->Add("gizmosql_disk_free_bytes", "gauge",
                 "Available filesystem space in bytes.", {{"path", "temp"}}, NAN);
  registry_->At("gizmosql_session_limit").value.store(session_limit);
  const auto cgroup = gizmosql::ReadCGroupLimits();
  if (cgroup.has_memory_limit())
    registry_->Add("gizmosql_cgroup_memory_limit_bytes", "gauge",
                   "Container memory limit in bytes.", {}, cgroup.memory_limit_bytes);
  if (cgroup.has_cpu_quota())
    registry_->Add("gizmosql_cgroup_cpu_quota_cores", "gauge",
                   "Container fractional CPU quota in cores.", {}, cgroup.cpu_quota);
  if (auto license =
          EnterpriseFeatures::Instance().GetLicenseManager()->GetCurrentLicense())
    registry_->Add(
        "gizmosql_license_expiry_seconds", "gauge", "License expiry as a Unix timestamp.",
        {},
        std::chrono::duration<double>(license->expires_at.time_since_epoch()).count());
  if (!certificate.empty()) {
    auto* bio = BIO_new_file(certificate.c_str(), "r");
    auto* cert = bio ? PEM_read_bio_X509(bio, nullptr, nullptr, nullptr) : nullptr;
    struct tm expiry{};
    if (cert && ASN1_TIME_to_tm(X509_get0_notAfter(cert), &expiry) == 1) {
#ifdef _WIN32
      auto seconds = _mkgmtime(&expiry);
#else
      auto seconds = timegm(&expiry);
#endif
      registry_->Add("gizmosql_tls_certificate_expiry_seconds", "gauge",
                     "Serving TLS certificate expiry as a Unix timestamp.", {}, seconds);
    }
    X509_free(cert);
    BIO_free(bio);
  }
  if (!database_.empty() && database_ != ":memory:" &&
      database_.string().find("://") == std::string::npos) {
    exit_state_ = database_.string() + ".gizmosql-metrics-state";
  }
}
MetricsService::~MetricsService() { Stop(); }
arrow::Status MetricsService::Start(int port, const std::string& address) {
  if (!EnterpriseFeatures::Instance().IsMetricsAvailable())
    return arrow::Status::Invalid(
        "Metrics requires the 'metrics' Enterprise license feature");
  if (port < 0 || port > 65535)
    return arrow::Status::Invalid("metrics-port must be between 0 and 65535");
  if (port) {
    http_->Get("/metrics", [registry = registry_](const httplib::Request&,
                                                  httplib::Response& response) {
      if (!EnterpriseFeatures::Instance().IsMetricsAvailable()) {
        response.status = 403;
        response.set_content(
            "Metrics requires a valid GizmoSQL Enterprise license with the 'metrics' "
            "feature.\n",
            "text/plain");
        return;
      }
      response.set_content(MetricsRegistry::Serialize(registry->Snapshot()),
                           "text/plain; version=0.0.4; charset=utf-8");
    });
    http_->Get("/", [](const httplib::Request&, httplib::Response& response) {
      response.set_content("GizmoSQL metrics: /metrics\n", "text/plain");
    });
    http_->new_task_queue = [] { return new httplib::ThreadPool(2, 2, 32); };
    http_->set_read_timeout(5);
    http_->set_write_timeout(5);
    if (!http_->bind_to_port(address, port))
      return arrow::Status::IOError("Cannot bind metrics listener to ", address, ":",
                                    port);
  }
  if (!exit_state_.empty()) {
    bool running = false;
    std::ifstream previous(exit_state_);
    if (previous) previous >> unclean_exits_ >> running;
    if (running) ++unclean_exits_;
    WriteExitState(true);
    if (exit_state_started_) {
      registry_->At("gizmosql_last_exit_unclean").value.store(running ? 1 : 0);
      registry_->At("gizmosql_unclean_exits_total").value.store(unclean_exits_);
    }
  }
  MetricsRegistry::Publish(registry_);
  collector_ = std::thread([this] {
    std::unique_lock lock(mutex_);
    while (!stopped_) {
      lock.unlock();
      Collect();
      lock.lock();
      wake_.wait_for(lock, std::chrono::seconds(5), [this] { return stopped_.load(); });
    }
  });
  if (port) listener_ = std::thread([this] { http_->listen_after_bind(); });
  return arrow::Status::OK();
}
void MetricsService::Stop() {
  {
    std::lock_guard lock(mutex_);
    stopped_ = true;
  }
  wake_.notify_all();
  if (connection_) connection_->Interrupt();
  if (http_) http_->stop();
  if (listener_.joinable()) listener_.join();
  if (collector_.joinable()) collector_.join();
  if (MetricsRegistry::Current() == registry_) MetricsRegistry::Publish(nullptr);
  if (exit_state_started_) {
    WriteExitState(false);
    exit_state_started_ = false;
  }
}
void MetricsService::WriteExitState(bool running) {
  // Replace atomically, so a crash cannot leave a truncated marker interpreted
  // as a clean exit. No state file is created for an in-memory database.
  auto pending = exit_state_.string() + ".tmp";
  std::ofstream output(pending, std::ios::trunc);
  if (!output) return;
  output << unclean_exits_ << ' ' << running << '\n';
  output.close();
  if (!output) return;
  std::error_code ec;
#ifdef _WIN32
  if (!MoveFileExW(std::filesystem::path(pending).c_str(), exit_state_.c_str(),
                   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
    return;
#else
  std::filesystem::rename(pending, exit_state_, ec);
  if (ec) return;
#endif
  exit_state_started_ = running;
}
void MetricsService::Collect() {
  const auto begin = std::chrono::steady_clock::now();
  auto set = [&](const char* name, double value) {
    registry_->At(name).value.store(value, std::memory_order_relaxed);
  };
  bool ok = true;
  try {
    auto memory = connection_->Query(
        "SELECT coalesce(sum(memory_usage_bytes),0)::DOUBLE, "
        "coalesce(sum(temporary_storage_bytes),0)::DOUBLE FROM duckdb_memory()");
    if (memory->HasError())
      ok = false;
    else {
      set("gizmosql_duckdb_memory_used_bytes", memory->GetValue(0, 0).GetValue<double>());
      set("gizmosql_duckdb_temp_storage_bytes",
          memory->GetValue(1, 0).GetValue<double>());
    }
    auto settings = connection_->Query(
        "SELECT current_setting('memory_limit'), current_setting('threads')::DOUBLE, "
        "current_setting('temp_directory')");
    if (settings->HasError())
      ok = false;
    else {
      set("gizmosql_duckdb_memory_limit_bytes",
          duckdb::DBConfig::ParseMemoryLimit(settings->GetValue(0, 0).ToString()));
      set("gizmosql_duckdb_threads", settings->GetValue(1, 0).GetValue<double>());
      auto temp = std::filesystem::path(settings->GetValue(2, 0).ToString());
      if (!temp.empty()) {
        if (!std::filesystem::exists(temp))
          temp = std::filesystem::absolute(temp).parent_path();
        std::error_code ec;
        auto space = std::filesystem::space(temp, ec);
        if (!ec)
          registry_->At("gizmosql_disk_free_bytes", {{"path", "temp"}})
              .value.store(space.available);
      }
    }
    auto spill = connection_->Query(
        "SELECT count(*)::DOUBLE, coalesce(sum(size),0)::DOUBLE FROM "
        "duckdb_temporary_files()");
    if (spill->HasError())
      ok = false;
    else {
      set("gizmosql_duckdb_spill_files", spill->GetValue(0, 0).GetValue<double>());
      set("gizmosql_duckdb_spill_bytes", spill->GetValue(1, 0).GetValue<double>());
    }
    if (!database_.empty() && database_ != ":memory:") {
      set("gizmosql_database_file_bytes", FileSize(database_));
      set("gizmosql_wal_bytes", FileSize(database_.string() + ".wal"));
      std::error_code ec;
      auto space =
          std::filesystem::space(std::filesystem::absolute(database_).parent_path(), ec);
      if (!ec)
        registry_->At("gizmosql_disk_free_bytes", {{"path", "database"}})
            .value.store(space.available);
    }
#ifndef _WIN32
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) == 0)
      set("process_cpu_seconds_total",
          usage.ru_utime.tv_sec + usage.ru_stime.tv_sec +
              (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6);
    set("gizmosql_host_cpus", sysconf(_SC_NPROCESSORS_ONLN));
#endif
#ifdef __linux__
    set("gizmosql_host_memory_bytes",
        static_cast<double>(sysconf(_SC_PHYS_PAGES)) * sysconf(_SC_PAGESIZE));
    std::ifstream statm("/proc/self/statm");
    uint64_t total = 0, resident = 0;
    if (statm >> total >> resident)
      set("process_resident_memory_bytes",
          static_cast<double>(resident) * sysconf(_SC_PAGESIZE));
    set("process_open_fds",
        std::distance(std::filesystem::directory_iterator("/proc/self/fd"),
                      std::filesystem::directory_iterator{}));
    set("process_threads",
        std::distance(std::filesystem::directory_iterator("/proc/self/task"),
                      std::filesystem::directory_iterator{}));
#endif
#ifdef __APPLE__
    proc_bsdinfo bsd{};
    if (proc_pidinfo(getpid(), PROC_PIDTBSDINFO, 0, &bsd, sizeof(bsd)) == sizeof(bsd))
      set("process_start_time_seconds", bsd.pbi_start_tvsec + bsd.pbi_start_tvusec / 1e6);
    uint64_t physical = 0;
    size_t size = sizeof(physical);
    if (sysctlbyname("hw.memsize", &physical, &size, nullptr, 0) == 0)
      set("gizmosql_host_memory_bytes", physical);
    proc_taskinfo task{};
    if (proc_pidinfo(getpid(), PROC_PIDTASKINFO, 0, &task, sizeof(task)) ==
        sizeof(task)) {
      set("process_resident_memory_bytes", task.pti_resident_size);
      set("process_threads", task.pti_threadnum);
    }
    int bytes = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, nullptr, 0);
    if (bytes >= 0) {
      std::vector<proc_fdinfo> fds(bytes / sizeof(proc_fdinfo) + 32);
      bytes = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, fds.data(),
                           static_cast<int>(fds.size() * sizeof(proc_fdinfo)));
      if (bytes >= 0) set("process_open_fds", bytes / sizeof(proc_fdinfo));
    }
#endif
#ifdef _WIN32
    const HANDLE process = GetCurrentProcess();
    PROCESS_MEMORY_COUNTERS process_memory{};
    if (GetProcessMemoryInfo(process, &process_memory, sizeof(process_memory)))
      set("process_resident_memory_bytes", process_memory.WorkingSetSize);
    DWORD handles = 0;
    if (GetProcessHandleCount(process, &handles)) set("process_open_handles", handles);
    FILETIME created, exited, kernel, user;
    auto seconds = [](FILETIME value) {
      ULARGE_INTEGER ticks;
      ticks.LowPart = value.dwLowDateTime;
      ticks.HighPart = value.dwHighDateTime;
      return static_cast<double>(ticks.QuadPart) / 1e7;
    };
    if (GetProcessTimes(process, &created, &exited, &kernel, &user)) {
      set("process_cpu_seconds_total", seconds(kernel) + seconds(user));
      set("process_start_time_seconds", seconds(created) - 11644473600.0);
    }
    MEMORYSTATUSEX host{};
    host.dwLength = sizeof(host);
    if (GlobalMemoryStatusEx(&host)) set("gizmosql_host_memory_bytes", host.ullTotalPhys);
    set("gizmosql_host_cpus", GetActiveProcessorCount(ALL_PROCESSOR_GROUPS));
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
    if (snapshot != INVALID_HANDLE_VALUE) {
      THREADENTRY32 entry{};
      entry.dwSize = sizeof(entry);
      uint64_t count = 0;
      if (Thread32First(snapshot, &entry)) do {
          if (entry.th32OwnerProcessID == GetCurrentProcessId()) ++count;
        } while (Thread32Next(snapshot, &entry));
      CloseHandle(snapshot);
      set("process_threads", count);
    }
#endif
    set("gizmosql_draining", gizmosql::IsDraining() ? 1 : 0);
    sample_server_(*registry_);
  } catch (...) {
    ok = false;
  }
  set("gizmosql_metrics_collection_success", ok ? 1 : 0);
  if (ok) set("gizmosql_metrics_last_collection_success_seconds", Now());
  set("gizmosql_metrics_collection_duration_seconds",
      std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count());
}

}  // namespace gizmosql::enterprise
