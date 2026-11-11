#pragma once

#include <sys/socket.h>

#include <atomic>
#include <string>

struct alignas(64) BackendServer {
  std::string ip;
  int port;
  int weight;
  struct sockaddr_storage addr {};
  socklen_t addr_len{0};

  std::atomic<int> active_connections{0};
  std::atomic<bool> is_healthy{true};
  std::atomic<long long> avg_latency_time{1};
  std::atomic<long long> last_response_time{0};

  BackendServer(const std::string& server_ip, int server_port,
                int server_weight);
  BackendServer(BackendServer&& other) noexcept;
};
