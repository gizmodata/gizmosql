// Licensed under the Apache License, Version 2.0.
#pragma once

#ifndef _WIN32
#include <atomic>
#include <csignal>
#include <functional>
#include <system_error>
#include <thread>
#include <pthread.h>

namespace gizmosql::client {

// Construct before creating any gRPC threads, so they inherit the blocked mask.
// Stop and join before destroying anything referenced by the callback.
class SignalWatcher {
 public:
  explicit SignalWatcher(std::function<void()> on_interrupt) {
    sigemptyset(&mask_);
    sigaddset(&mask_, SIGINT);
    const int error = pthread_sigmask(SIG_BLOCK, &mask_, &previous_mask_);
    if (error) throw std::system_error(error, std::generic_category());
    try {
      thread_ = std::thread([this, callback = std::move(on_interrupt)] {
        while (!stopped_.load()) {
          int signal = 0;
          if (sigwait(&mask_, &signal) != 0) return;
          if (stopped_.load()) return;
          if (signal == SIGINT) callback();
        }
      });
    } catch (...) {
      pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr);
      throw;
    }
  }

  ~SignalWatcher() {
    stopped_.store(true);
    // A thread-directed signal wakes only our waiter, including when shutdown
    // races with an in-flight callback. No detached thread survives this scope.
    pthread_kill(thread_.native_handle(), SIGINT);
    thread_.join();
    pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr);
  }

  SignalWatcher(const SignalWatcher&) = delete;
  SignalWatcher& operator=(const SignalWatcher&) = delete;

 private:
  sigset_t mask_{};
  sigset_t previous_mask_{};
  std::atomic<bool> stopped_{false};
  std::thread thread_;
};

}  // namespace gizmosql::client
#endif
