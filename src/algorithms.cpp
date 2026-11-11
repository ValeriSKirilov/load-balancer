#include "lb/algorithms.hpp"

#include <atomic>
#include <climits>
#include <cstddef>
#include <map>
#include <mutex>

namespace {

std::atomic<unsigned int> rr_index{0};
std::atomic<unsigned int> wrr_index{0};
std::vector<int> wrr_schedule;

std::vector<int> generate_wrr_schedule(
    const std::vector<BackendServer>& servers) {
  size_t total_weight = 0;

  struct ActiveServer {
    size_t original_index;
    int weight;
  };
  std::vector<ActiveServer> active_servers;

  for (size_t i = 0; i < servers.size(); ++i) {
    if (servers[i].is_healthy.load(std::memory_order_relaxed)) {
      total_weight += servers[i].weight;
      active_servers.push_back({i, servers[i].weight});
    }
  }

  if (total_weight == 0) {
    return {};
  }

  std::vector<int> schedule;
  std::vector<int> current_weight(active_servers.size(), 0);

  for (size_t i = 0; i < total_weight; ++i) {
    int curr_max = INT_MIN;
    size_t max_weight_index = 0;
    for (size_t j = 0; j < active_servers.size(); ++j) {
      current_weight[j] += active_servers[j].weight;
      if (current_weight[j] > curr_max) {
        curr_max = current_weight[j];
        max_weight_index = j;
      }
    }
    schedule.push_back(
        static_cast<int>(active_servers[max_weight_index].original_index));
    current_weight[max_weight_index] -= total_weight;
  }

  return schedule;
}

int round_robin(const std::vector<BackendServer>& servers, const std::string&) {
  size_t num_servers = servers.size();
  if (num_servers == 0) return -1;

  for (size_t i = 0; i < num_servers; ++i) {
    int idx = static_cast<int>(
        rr_index.fetch_add(1, std::memory_order_relaxed) % num_servers);
    if (servers[idx].is_healthy.load(std::memory_order_relaxed)) {
      return idx;
    }
  }
  return -1;
}

int weighted_round_robin(const std::vector<BackendServer>& servers,
                         const std::string&) {
  if (wrr_schedule.empty()) return -1;

  for (size_t i = 0; i < wrr_schedule.size(); ++i) {
    size_t idx =
        wrr_index.fetch_add(1, std::memory_order_relaxed) % wrr_schedule.size();
    if (servers[wrr_schedule[idx]].is_healthy.load(std::memory_order_relaxed)) {
      return wrr_schedule[idx];
    }
  }

  return -1;
}

int least_connection(const std::vector<BackendServer>& servers,
                     const std::string&) {
  int min_connections = INT_MAX;
  int best_idx = -1;
  for (size_t i = 0; i < servers.size(); ++i) {
    if (!servers[i].is_healthy.load(std::memory_order_relaxed)) continue;

    int curr_conn =
        servers[i].active_connections.load(std::memory_order_relaxed);
    if (curr_conn < min_connections) {
      min_connections = curr_conn;
      best_idx = static_cast<int>(i);
    }
  }
  return best_idx;
}

int least_time(const std::vector<BackendServer>& servers, const std::string&) {
  int best_idx = -1;
  long long min_score = LLONG_MAX;

  for (size_t i = 0; i < servers.size(); ++i) {
    if (!servers[i].is_healthy.load(std::memory_order_relaxed)) continue;

    long long curr_latency =
        servers[i].avg_latency_time.load(std::memory_order_relaxed);
    int curr_conn =
        servers[i].active_connections.load(std::memory_order_relaxed);
    long long score = curr_latency * (curr_conn + 1);
    if (score < min_score) {
      min_score = score;
      best_idx = static_cast<int>(i);
    }
  }
  return best_idx;
}

int ip_hash(const std::vector<BackendServer>& servers,
            const std::string& client_ip) {
  size_t num_servers = servers.size();
  if (num_servers == 0) return -1;

  size_t hash_value = std::hash<std::string>{}(client_ip);

  for (size_t i = 0; i < num_servers; ++i) {
    int idx = static_cast<int>((hash_value + i) % num_servers);
    if (servers[idx].is_healthy.load(std::memory_order_relaxed)) {
      return idx;
    }
  }
  return -1;
}

}  // namespace

namespace {
std::mutex algo_mutex;
std::string active_algo_name = "ROUND_ROBIN";
RouterFunc active_algo_func = round_robin;
}  // namespace

RouterFunc get_algorithm(const std::string& name) {
  static const std::map<std::string, RouterFunc> algorithms = {
      {"ROUND_ROBIN", round_robin},
      {"WEIGHTED_RR", weighted_round_robin},
      {"LEAST_CONN", least_connection},
      {"LEAST_TIME", least_time},
      {"IP_HASH", ip_hash}};

  auto it = algorithms.find(name);
  if (it != algorithms.end()) {
    return it->second;
  }
  return nullptr;
}

bool set_active_algorithm(const std::string& name) {
  RouterFunc func = get_algorithm(name);
  if (!func) {
    return false;
  }
  std::lock_guard<std::mutex> lock(algo_mutex);
  active_algo_name = name;
  active_algo_func = func;
  return true;
}

std::string get_active_algorithm_name() {
  std::lock_guard<std::mutex> lock(algo_mutex);
  return active_algo_name;
}

int route_with_active_algorithm(const std::vector<BackendServer>& servers,
                                const std::string& client_ip) {
  RouterFunc func;
  {
    std::lock_guard<std::mutex> lock(algo_mutex);
    func = active_algo_func;
  }
  return func(servers, client_ip);
}

void init_wrr_schedule(const std::vector<BackendServer>& servers) {
  wrr_schedule = generate_wrr_schedule(servers);
}