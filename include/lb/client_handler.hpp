#pragma once

#include <string>
#include <vector>

#include "lb/algorithms.hpp"
#include "lb/backend_server.hpp"

struct ClientContext {
  int client_fd;
  std::string request_buffer;
  std::string client_ip;
  long long connected_at_ms;
};

void handle_client(ClientContext* ctx, std::vector<BackendServer>& servers,
                   RouterFunc selected_router_func, int epoll_fd);