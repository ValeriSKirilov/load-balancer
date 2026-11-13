#include "lb/client_handler.hpp"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <fstream>
#include <iostream>
#include <sstream>

#include "lb/algorithms.hpp"
#include "lb/http_response.hpp"
#include "lb/metrics.hpp"
#include "lb/net_utils.hpp"
#include "lb/proxy.hpp"
#include "lb/rate_limiter.hpp"
#include "lb/time_utils.hpp"
#include "lb/waf.hpp"

void handle_client(ClientContext* ctx, std::vector<BackendServer>& servers,
                   RouterFunc selected_router_func, int epoll_fd) {
  char buffer[4096];
  ssize_t bytes_read;

  while (true) {
    bytes_read = recv(ctx->client_fd, buffer, sizeof(buffer), 0);
    if (bytes_read > 0) {
      ctx->request_buffer.append(buffer, bytes_read);
      continue;
    }

    if (bytes_read == 0) {
      close(ctx->client_fd);
      delete ctx;
      return;
    }

    if (bytes_read == -1) {
      if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break;
      } else {
        std::cerr << "[ERROR] Failed to receive data from client.\n";
        close(ctx->client_fd);
        delete ctx;
        return;
      }
    }
  }

  bool is_request_complete =
      ctx->request_buffer.find("\r\n\r\n") != std::string::npos;

  if (!is_request_complete) {
    if (current_time_ms() - ctx->connected_at_ms > 5000 ||
        ctx->request_buffer.size() > 8192) {
      std::cerr << "[SLOWLORIS BLOCKED] Client " << ctx->client_ip
                << " exceeded request time or size limit.\n";
      HttpResponse response_408 = {
          408,
          "Request Timeout",
          {},
          "408 Request Timeout - Slowloris Detected\n"};
      std::string response_str = response_408.to_raw_string();
      if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
          -1) {
        std::cerr << "[ERROR] Failed to send 408 response to client.\n";
      }
      record_blocked_attack();
      close(ctx->client_fd);
      delete ctx;
      return;
    }

    struct epoll_event ev {};
    ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
    ev.data.ptr = ctx;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ctx->client_fd, &ev) == -1) {
      std::cerr << "[ERROR] Failed to modify client socket in epoll.\n";
      close(ctx->client_fd);
      delete ctx;
    }
    return;
  }

  if (set_blocking(ctx->client_fd) == false) {
    std::cerr << "[ERROR] Failed to set client socket to blocking.\n";
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  if (ctx->request_buffer.rfind("GET /lb-config?algo=", 0) == 0) {
    size_t start = 20;
    size_t end = ctx->request_buffer.find_first_of(" &\r\n", start);
    std::string new_algo = ctx->request_buffer.substr(start, end - start);

    if (set_active_algorithm(new_algo)) {
      std::cerr << "[CONFIG] Switched active algorithm to: " << new_algo
                << "\n";
      HttpResponse response_200 = {
          200,
          "OK",
          {{"Content-Type", "application/json"},
           {"Access-Control-Allow-Origin", "*"}},
          "{\"status\":\"ok\",\"active_algorithm\":\"" +
              get_active_algorithm_name() + "\"}\n"};
      std::string response_str = response_200.to_raw_string();
      if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
          -1) {
        std::cerr
            << "[ERROR] Failed to send config change response to client.\n";
      }
    } else {
      HttpResponse response_400 = {400,
                                   "Bad Request",
                                   {{"Content-Type", "application/json"},
                                    {"Access-Control-Allow-Origin", "*"}},
                                   "{\"error\":\"Invalid algorithm\"}\n"};
      std::string response_str = response_400.to_raw_string();
      if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
          -1) {
        std::cerr
            << "[ERROR] Failed to send invalid algorithm response to client.\n";
      }
    }
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  if (ctx->request_buffer.rfind("GET /dashboard", 0) == 0) {
    std::ifstream file("dashboard/index.html");
    if (!file.is_open()) {
      std::cerr << "[ERROR] Failed to open dashboard/index.html\n";
      HttpResponse response_500 = {
          500, "Internal Server Error", {}, "Internal Server Error"};
      std::string response_str = response_500.to_raw_string();
      if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
          -1) {
        std::cerr << "[ERROR] Failed to send 500 response to client.\n";
      }
      close(ctx->client_fd);
      delete ctx;
      return;
    }

    std::ostringstream ss;
    ss << file.rdbuf();
    HttpResponse res = {
        200, "OK", {{"Content-Type", "text/html; charset=utf-8"}}, ss.str()};
    std::string s = res.to_raw_string();
    send(ctx->client_fd, s.c_str(), s.size(), 0);
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  if (ctx->request_buffer.rfind("GET /lb-metrics", 0) == 0) {
    std::string json_body = build_metrics_json(servers);

    HttpResponse response_200 = {200,
                                 "OK",
                                 {{"Content-Type", "application/json"},
                                  {"Access-Control-Allow-Origin", "*"}},
                                 json_body};
    std::string response_str = response_200.to_raw_string();

    if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
        -1) {
      std::cerr << "[ERROR] Failed to send metrics response to client.\n";
    }
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  if (is_rate_limited(ctx->client_ip)) {
    HttpResponse response_429 = {
        429,
        "Too Many Requests",
        {{"Access-Control-Allow-Origin", "*"}},
        "429 Too Many Requests - Rate Limit Exceeded\n"};
    std::string response_str = response_429.to_raw_string();
    if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
        -1) {
      std::cerr << "[ERROR] Failed to send 429 response to client.\n";
    }
    record_blocked_attack();
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  if (inspect_request(ctx->request_buffer)) {
    HttpResponse response_403 = {
        403, "Forbidden", {{"Access-Control-Allow-Origin", "*"}}, "Forbidden"};
    std::string response_str = response_403.to_raw_string();
    if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
        -1) {
      std::cerr << "[ERROR] Failed to send 403 response to client.\n";
    }
    record_blocked_attack();
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  bool forwarded_successfuly = false;
  size_t max_retries = servers.size();

  for (size_t attempts = 0; attempts < max_retries; ++attempts) {
    int backend_idx = route_with_active_algorithm(servers, ctx->client_ip);
    if (backend_idx == -1) {
      break;
    }

    servers[backend_idx].active_connections.fetch_add(
        1, std::memory_order_relaxed);

    long long start_time = current_time_ms();

    bool ok = forward_to_backend(ctx->client_fd, ctx->request_buffer,
                                 servers[backend_idx]);

    long long end_time = current_time_ms();
    servers[backend_idx].active_connections.fetch_sub(
        1, std::memory_order_relaxed);

    if (ok) {
      long long old_latency =
          servers[backend_idx].avg_latency_time.load(std::memory_order_relaxed);
      long long sample_latency = end_time - start_time;
      long long new_avg_latency =
          std::max(1LL, (7 * old_latency + 3 * sample_latency) / 10);
      servers[backend_idx].avg_latency_time.store(new_avg_latency,
                                                  std::memory_order_relaxed);
      forwarded_successfuly = true;
      break;
    }
  }

  if (!forwarded_successfuly) {
    std::cerr << "[ERROR] Failed to forward request to backend server after "
                 "multiple attempts.\n";
    HttpResponse response_503 = {
        503, "Service Unavailable", {}, "Service Unavailable"};
    std::string response_str = response_503.to_raw_string();
    if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
        -1) {
      std::cerr << "[ERROR] Failed to send 503 response to client.\n";
    }
  }

  close(ctx->client_fd);
  delete ctx;
}