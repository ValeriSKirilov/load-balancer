#include "lb/rate_limiter.hpp"

#include <iostream>
#include <mutex>
#include <unordered_map>

#include "lb/constants.hpp"
#include "lb/time_utils.hpp"

namespace {
struct RateLimitEntry {
  int request_count{0};
  long long window_start_ms{0};
};

std::unordered_map<std::string, RateLimitEntry> rate_limit_map;
std::mutex rate_limit_mutex;
}  // namespace

bool is_rate_limited(const std::string& client_ip) {
  long long now = current_time_ms();
  std::lock_guard<std::mutex> lock(rate_limit_mutex);

  auto& entry = rate_limit_map[client_ip];
  if (now - entry.window_start_ms >= 1000) {
    entry.window_start_ms = now;
    entry.request_count = 1;
    return false;
  }

  entry.request_count++;
  if (entry.request_count > MAX_REQUESTS_PER_SEC) {
    std::cerr << "[RATE LIMIT] IP " << client_ip << " exceeded limit ("
              << entry.request_count << " req/s)\n";
    return true;
  }

  return false;
}