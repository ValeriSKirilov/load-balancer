#include "lb/metrics.hpp"

#include <atomic>
#include <sstream>

#include "lb/algorithms.hpp"

namespace {
std::atomic<long long> blocked_attacks{0};
}

void record_blocked_attack() {
  blocked_attacks.fetch_add(1, std::memory_order_relaxed);
}

long long get_blocked_attacks() {
  return blocked_attacks.load(std::memory_order_relaxed);
}

std::string build_metrics_json(const std::vector<BackendServer>& servers) {
  std::ostringstream oss;
  oss << "{\n";
  oss << "  \"active_algorithm\": \"" << get_active_algorithm_name() << "\",\n";
  oss << "  \"blocked_attacks\": " << get_blocked_attacks() << ",\n";
  oss << "  \"backend_servers\": [\n";

  for (size_t i = 0; i < servers.size(); ++i) {
    oss << "    {" << "\"ip\": \"" << servers[i].ip << "\", "
        << "\"port\": " << servers[i].port << ", " << "\"healthy\": "
        << (servers[i].is_healthy.load(std::memory_order_relaxed) ? "true"
                                                                  : "false")
        << ", " << "\"active_connections\": "
        << servers[i].active_connections.load(std::memory_order_relaxed) << ", "
        << "\"avg_latency_ms\": "
        << servers[i].avg_latency_time.load(std::memory_order_relaxed) << "}";

    if (i + 1 < servers.size()) {
      oss << ",";
    }
    oss << '\n';
  }

  oss << "  ]\n";
  oss << "}\n";
  return oss.str();
}