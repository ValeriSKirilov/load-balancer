#include "lb/backend_server.hpp"

#include <netdb.h>

#include <cstring>
#include <iostream>
#include <utility>

BackendServer::BackendServer(const std::string& server_ip, int server_port,
                             int server_weight)
    : ip(server_ip), port(server_port), weight(server_weight) {
  struct addrinfo hints, *res = nullptr;

  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if (getaddrinfo(server_ip.c_str(), std::to_string(server_port).c_str(),
                  &hints, &res) != 0) {
    std::cerr << "[ERROR] Failed to get address information for server: "
              << server_ip << ":" << server_port << '\n';
    return;
  }

  std::memcpy(&addr, res->ai_addr, res->ai_addrlen);
  addr_len = res->ai_addrlen;
  freeaddrinfo(res);
}

BackendServer::BackendServer(BackendServer&& other) noexcept
    : ip(std::move(other.ip)),
      port(other.port),
      weight(other.weight),
      addr(other.addr),
      addr_len(other.addr_len),
      active_connections(
          other.active_connections.load(std::memory_order_relaxed)),
      is_healthy(other.is_healthy.load(std::memory_order_relaxed)),
      avg_latency_time(other.avg_latency_time.load(std::memory_order_relaxed)),
      last_response_time(
          other.last_response_time.load(std::memory_order_relaxed)) {}