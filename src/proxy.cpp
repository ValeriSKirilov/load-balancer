#include "lb/proxy.hpp"

#include <sys/time.h>
#include <unistd.h>

#include <iostream>

#include "sys/socket.h"

bool forward_to_backend(int client_fd, const std::string& request,
                        BackendServer& server) {
  int backend_fd = socket(server.addr.ss_family, SOCK_STREAM, 0);
  if (backend_fd < 0) {
    std::cerr << "[ERROR] Failed to create socket for backend server.\n";
    return false;
  }

  timeval timeout{};
  timeout.tv_sec = 2;
  timeout.tv_usec = 0;
  setsockopt(backend_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  setsockopt(backend_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  if (connect(backend_fd, reinterpret_cast<struct sockaddr*>(&server.addr),
              server.addr_len) < 0) {
    std::cerr << "[ERROR] Failed to connect to backend server: " << server.ip
              << ":" << server.port << '\n';
    server.is_healthy.store(false, std::memory_order_relaxed);
    close(backend_fd);
    return false;
  }

  if (send(backend_fd, request.c_str(), request.size(), 0) == -1) {
    std::cerr << "[ERROR] Failed to send request to backend server.\n";
    server.is_healthy.store(false, std::memory_order_relaxed);
    close(backend_fd);
    return false;
  }

  shutdown(backend_fd, SHUT_WR);

  while (true) {
    char buffer[4096];
    ssize_t bytes_read = recv(backend_fd, buffer, sizeof(buffer), 0);
    if (bytes_read == 0) {
      break;
    } else if (bytes_read < 0) {
      std::cerr << "[ERROR] Failed to receive response from backend server.\n";
      server.is_healthy.store(false, std::memory_order_relaxed);
      close(backend_fd);
      return false;
    }

    if (send(client_fd, buffer, bytes_read, 0) == -1) {
      std::cerr << "[ERROR] Failed to send response to client.\n";
      close(backend_fd);
      break;
    }
  }

  close(backend_fd);
  return true;
}