#include "lb/server.hpp"

#include <netdb.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <iostream>
#include <memory>
#include <thread>

#include "lb/client_handler.hpp"
#include "lb/health_check.hpp"
#include "lb/net_utils.hpp"
#include "lb/thread_pool.hpp"
#include "lb/time_utils.hpp"

std::atomic<bool> server_running{true};

namespace {

void handle_signal(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cerr << "\n[INFO] Signal received. Shutting down gracefully...\n";
    server_running = false;
  }
}

int create_listen_socket(const char* port) {
  struct addrinfo hints {
  }, *res = nullptr;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  if (getaddrinfo(nullptr, port, &hints, &res) != 0) {
    std::cerr << "[ERROR] Failed to get address information.\n";
    return -1;
  }

  int listen_fd = -1;
  for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
    listen_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (listen_fd == -1) {
      continue;
    }

    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) ==
        -1) {
      std::cerr << "[ERROR] Failed to set socket option.\n";
      close(listen_fd);
      listen_fd = -1;
      continue;
    }

    if (bind(listen_fd, p->ai_addr, p->ai_addrlen) == 0) {
      break;
    }

    close(listen_fd);
    listen_fd = -1;
  }

  freeaddrinfo(res);
  if (listen_fd == -1) {
    std::cerr << "[ERROR] Failed to bind to any address.\n";
    return -1;
  }

  if (set_nonblocking(listen_fd) == false) {
    std::cerr << "[ERROR] Failed to set listening socket to non-blocking.\n";
    close(listen_fd);
    return -1;
  }

  if (listen(listen_fd, SOMAXCONN) == -1) {
    std::cerr << "[ERROR] Failed to listen on socket.\n";
    close(listen_fd);
    return -1;
  }

  return listen_fd;
}

void accept_client(int listen_fd, int epoll_fd) {
  while (true) {
    struct sockaddr_storage client_addr {};
    socklen_t client_len = sizeof(client_addr);

    int client_fd =
        accept(listen_fd, reinterpret_cast<struct sockaddr*>(&client_addr),
               &client_len);
    if (client_fd == -1) {
      if (errno == EINTR) {
        continue;
      }
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        std::cerr << "[ERROR] accept() failed\n";
      }
      return;
    }

    char client_ip[NI_MAXHOST];
    if (getnameinfo(reinterpret_cast<struct sockaddr*>(&client_addr),
                    client_len, client_ip, sizeof(client_ip), nullptr, 0,
                    NI_NUMERICHOST) != 0) {
      std::cerr << "[ERROR] Failed to get client IP.\n";
      close(client_fd);
      continue;
    }

    if (set_nonblocking(client_fd) == false) {
      std::cerr << "[ERROR] Failed to set client socket to non-blocking.\n";
      close(client_fd);
      continue;
    }

    ClientContext* client_ctx =
        new ClientContext{client_fd, "", client_ip, current_time_ms()};
    struct epoll_event client_ev {};
    client_ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
    client_ev.data.ptr = client_ctx;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_ev) == -1) {
      std::cerr << "[ERROR] Failed to add client socket to epoll.\n";
      close(client_fd);
      delete client_ctx;
    }
  }
}

}  // namespace

int run_server(std::vector<BackendServer>& servers, RouterFunc router,
               int num_threads, const char* port) {
  server_running = true;
  signal(SIGINT, handle_signal);
  signal(SIGTERM, handle_signal);
  signal(SIGPIPE, SIG_IGN);

  int listen_fd = create_listen_socket(port);
  if (listen_fd == -1) {
    return 1;
  }

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    std::cerr << "[ERROR] Failed to create epoll instance.\n";
    close(listen_fd);
    return 1;
  }

  ClientContext listen_ctx{listen_fd, "", "", current_time_ms()};
  struct epoll_event ev {};
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = &listen_ctx;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
    std::cerr << "[ERROR] Failed to add listening socket to epoll.\n";
    close(listen_fd);
    close(epoll_fd);
    return 1;
  }
  auto pool = std::make_unique<ThreadPool>(num_threads);
  std::thread health_thread = start_health_thread(servers, server_running);

  struct epoll_event events[MAX_EVENTS];

  while (server_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "[ERROR] Failed to wait on epoll.\n";
      server_running = false;
      break;
    }

    for (int i = 0; i < nfds; i++) {
      ClientContext* ctx = static_cast<ClientContext*>(events[i].data.ptr);

      if (ctx->client_fd == listen_fd) {
        accept_client(listen_fd, epoll_fd);
      } else if (events[i].events & EPOLLERR || events[i].events & EPOLLHUP) {
        std::cerr << "[ERROR] Epoll error on fd: " << ctx->client_fd << "\n";
        close(ctx->client_fd);
        delete ctx;
      } else if (events[i].events & EPOLLIN) {
        pool->enqueue([ctx, &servers, router, epoll_fd] {
          handle_client(ctx, servers, router, epoll_fd);
        });
      }
    }
  }

  if (health_thread.joinable()) {
    health_thread.join();
  }
  pool.reset();
  close(listen_fd);
  close(epoll_fd);

  return 0;
}