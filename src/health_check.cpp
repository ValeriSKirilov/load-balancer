#include "lb/health_check.hpp"

#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iostream>

#include "lb/time_utils.hpp"

bool health_check(BackendServer& server) {
  int sockFd = socket(server.addr.ss_family, SOCK_STREAM, 0);
  if (sockFd < 0) {
    std::cerr << "[ERROR] Failed to create socket for health check.\n";
    return false;
  }

  timeval timeout{};
  timeout.tv_sec = 2;
  timeout.tv_usec = 0;
  setsockopt(sockFd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  setsockopt(sockFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  long long start_ms = current_time_ms();

  if (connect(sockFd, reinterpret_cast<struct sockaddr*>(&server.addr),
              server.addr_len) < 0) {
    std::cerr << "[ERROR] Health check failed for server: " << server.ip << ":"
              << server.port << '\n';
    close(sockFd);
    server.is_healthy = false;
    return false;
  }

  long long end_ms = current_time_ms();
  close(sockFd);

  server.is_healthy = true;
  server.last_response_time = end_ms;
  server.avg_latency_time = std::max(1LL, end_ms - start_ms);

  return true;
}

std::thread start_health_thread(std::vector<BackendServer>& servers,
                                const std::atomic<bool>& running) {
  return std::thread([&servers, &running] {
    while (running.load()) {
      for (auto& server : servers) {
        health_check(server);
      }
      for (int i = 0; i < 50 && running.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  });
}