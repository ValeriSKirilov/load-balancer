#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#define MAX_EVENTS 128

const std::string usage =
    "Usage: ./load_balancer <algorithm> <num_threads>\n"
    "Available algorithms:\n"
    "  ROUND_ROBIN\n"
    "  WEIGHTED_RR\n"
    "  LEAST_CONN\n"
    "  LEAST_TIME\n";

const std::string INVALID_ALG_ERROR =
    "[ERROR] Invalid load balancing algorithm: \'%s\'.\n"
    "Allowed values : ROUND_ROBIN, WEIGHTED_RR, LEAST_CONN, LEAST_TIME\n"
    "Usage : ./load_balancer [ALGORITHM] [THREAD_COUNT] [CONFIG_FILE]\n";

const std::string INVALID_THREAD_COUNT_ERROR =
    "[ERROR] Invalid thread count: '%s'. Thread count must be an integer "
    "between 1 and 64.\n";

const std::string INVALID_CONFIG_FILE_ERROR =
    "[ERROR] Failed to open configuration file '%s' or no valid "
    "backend servers found.\n"
    "Expected format per line: <IP_ADDRESS> <PORT> <WEIGHT>\n";

static std::atomic<unsigned int> rr_index{0};
static std::atomic<unsigned int> wrr_index{0};

static std::vector<int> wrr_schedule;

static std::atomic<bool> server_running{true};

static std::vector<std::thread> worker_threads;

struct BackendServer {
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
                int server_weight)
      : ip(server_ip), port(server_port), weight(server_weight) {
    struct addrinfo hints, *res;

    memset(&hints, 0, sizeof(hints));
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

  BackendServer(BackendServer&& other) noexcept
      : ip(std::move(other.ip)),
        port(other.port),
        weight(other.weight),
        addr(other.addr),
        addr_len(other.addr_len),
        active_connections(
            other.active_connections.load(std::memory_order_relaxed)),
        is_healthy(other.is_healthy.load(std::memory_order_relaxed)),
        avg_latency_time(
            other.avg_latency_time.load(std::memory_order_relaxed)),
        last_response_time(
            other.last_response_time.load(std::memory_order_relaxed)) {}
};

class ThreadPool {
 private:
  std::vector<std::thread> threads;
  std::queue<std::function<void()>> tasks;
  std::mutex queue_mutex;
  std::condition_variable cv;

  bool stop{false};

 public:
  explicit ThreadPool(size_t num_threads) {
    for (size_t i = 0; i < num_threads; ++i) {
      threads.emplace_back([this] {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(queue_mutex);
            cv.wait(lock, [this] { return stop || !tasks.empty(); });

            if (stop && tasks.empty()) {
              return;
            }

            task = std::move(tasks.front());
            tasks.pop();
          }
          try {
            task();
          } catch (const std::exception& e) {
            std::cerr << "[ThreadPool] Task threw exception: " << e.what()
                      << "\n";
          } catch (...) {
            std::cerr << "[ThreadPool] Task threw an unknown exception.\n";
          }
        }
      });
    }
  }

  ~ThreadPool() {
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      stop = true;
    }
    cv.notify_all();
    for (auto& t : threads) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void enqueue(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      if (stop) {
        return;
      }
      tasks.push(std::move(task));
    }
    cv.notify_one();
  }
};

std::map<std::string, std::function<int(const std::vector<BackendServer>&)>>
    algorithm_map = {
        {"ROUND_ROBIN",
         [](const std::vector<BackendServer>& servers) {
           size_t num_servers = servers.size();
           for (size_t i = 0; i < num_servers; ++i) {
             int idx =
                 rr_index.fetch_add(1, std::memory_order_relaxed) % num_servers;
             if (servers[idx].is_healthy.load(std::memory_order_relaxed)) {
               return idx;
             }
           }
           return -1;
         }},
        {"WEIGHTED_RR",
         [](const std::vector<BackendServer>& servers) {
           if (wrr_schedule.empty()) return -1;

           for (size_t i = 0; i < wrr_schedule.size(); ++i) {
             int idx = wrr_index.fetch_add(1, std::memory_order_relaxed) %
                       wrr_schedule.size();
             if (servers[wrr_schedule[idx]].is_healthy.load(
                     std::memory_order_relaxed)) {
               return wrr_schedule[idx];
             }
           }

           return -1;
         }},
        {"LEAST_CONN",
         [](const std::vector<BackendServer>& servers) {
           int min_connections = INT_MAX;
           int best_idx = -1;
           for (size_t i = 0; i < servers.size(); ++i) {
             if (!servers[i].is_healthy.load(std::memory_order_relaxed))
               continue;

             int curr_conn =
                 servers[i].active_connections.load(std::memory_order_relaxed);
             if (curr_conn < min_connections) {
               min_connections = curr_conn;
               best_idx = static_cast<int>(i);
             }
           }
           return best_idx;
         }},
        {"LEAST_TIME", [](const std::vector<BackendServer>& servers) {
           int best_idx = -1;
           long long min_score = LLONG_MAX;

           for (size_t i = 0; i < servers.size(); ++i) {
             if (!servers[i].is_healthy.load(std::memory_order_relaxed))
               continue;

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
         }}};

std::vector<BackendServer> load_backend_servers(
    const std::string& config_path) {
  std::vector<BackendServer> servers;

  std::ifstream file(config_path);
  if (!file.is_open()) {
    std::cerr << "[ERROR] Failed to open configuration file: " << config_path
              << '\n';
    return servers;
  }

  std::string line;
  while (std::getline(file, line)) {
    if (line.empty() or line[0] == '#') {
      continue;
    }

    std::istringstream stream(line);
    std::string ip;
    int port, weight;

    if (!(stream >> ip >> port >> weight)) {
      std::cerr << "[WARNING] Skipping malformed line: " << line << '\n';
      continue;
    }

    if (port >= 1 && port <= 65535 && weight >= 1) {
      BackendServer server(ip, port, weight);
      if (server.addr_len == 0) {
        std::cerr << "[WARNING] Skipping server with invalid address: " << ip
                  << ":" << port << '\n';
        continue;
      }
      servers.emplace_back(std::move(server));
    }
  }

  file.close();

  if (servers.empty()) {
    fprintf(stderr, INVALID_CONFIG_FILE_ERROR.c_str(), config_path.c_str());
    return servers;
  }

  return servers;
}

long long current_time_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

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
    int curr_max = INT_MIN, max_weight_index = 0;
    for (size_t j = 0; j < active_servers.size(); ++j) {
      current_weight[j] += active_servers[j].weight;
      if (current_weight[j] > curr_max) {
        curr_max = current_weight[j];
        max_weight_index = j;
      }
    }
    schedule.push_back(active_servers[max_weight_index].original_index);
    current_weight[max_weight_index] -= total_weight;
  }

  return schedule;
}

void handle_signal(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cerr << "\n[INFO] Signal received. Shutting down gracefully...\n";
    server_running = false;
  }
}

bool set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    std::cerr << "[ERROR] Failed to get file descriptor flags.\n";
    return false;
  }

  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    std::cerr << "[ERROR] Failed to set file descriptor to non-blocking.\n";
    return false;
  }

  return true;
}

int main(const int argc, const char* argv[]) {
  if (argc > 1 &&
      (argv[1] == std::string("-help") || argv[1] == std::string("-h"))) {
    std::cerr << usage;
    return 1;
  }

  std::string algorithm_str = "ROUND_ROBIN";  // Default algorithm
  int num_threads = 4;                        // Default number of threads
  std::string configFile = "backend.conf";    // Default config file
  const std::string logFile = "load-balancer.log";

  auto selected_router_func =
      algorithm_map[algorithm_str];  // Default to ROUND_ROBIN

  if (argc >= 2) {
    algorithm_str = argv[1];

    auto find_algorithm = algorithm_map.find(algorithm_str);
    if (find_algorithm == algorithm_map.end()) {
      fprintf(stderr, INVALID_ALG_ERROR.c_str(), argv[1]);
      return 1;
    }

    selected_router_func = algorithm_map[algorithm_str];
  }

  if (argc >= 3) {
    try {
      int threads_count = std::stoi(argv[2]);
      if (threads_count < 1 || threads_count > 64) {
        fprintf(stderr, INVALID_THREAD_COUNT_ERROR.c_str(), argv[2]);
        return 1;
      }
      num_threads = threads_count;
    } catch (const std::invalid_argument& e) {
      fprintf(stderr, INVALID_THREAD_COUNT_ERROR.c_str(), argv[2]);
      return 1;
    } catch (const std::out_of_range& e) {
      fprintf(stderr, INVALID_THREAD_COUNT_ERROR.c_str(), argv[2]);
      return 1;
    }
  }

  if (argc >= 4) {
    configFile = argv[3];
  }

  std::vector<BackendServer> servers = load_backend_servers(configFile);
  if (servers.empty()) {
    return 1;
  }

  wrr_schedule = generate_wrr_schedule(servers);

  signal(SIGINT, handle_signal);
  signal(SIGTERM, handle_signal);

  ThreadPool pool(num_threads);

  struct addrinfo hints, *res;
  int listen_fd;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_flags = AI_PASSIVE;
  hints.ai_socktype = SOCK_STREAM;

  if (getaddrinfo(nullptr, "8080", &hints, &res) != 0) {
    std::cerr << "[ERROR] Failed to get address information.\n";
    return 1;
  }

  listen_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (listen_fd == -1) {
    std::cerr << "[ERROR] Failed to create listening socket.\n";
    freeaddrinfo(res);
    return 1;
  }

  int yes = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  if (bind(listen_fd, res->ai_addr, res->ai_addrlen) == -1) {
    std::cerr << "[ERROR] Failed to bind listening socket.\n";
    freeaddrinfo(res);
    return 1;
  }

  freeaddrinfo(res);

  set_nonblocking(listen_fd);
  listen(listen_fd, SOMAXCONN);

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    std::cerr << "[ERROR] Failed to create epoll instance.\n";
    return 1;
  }

  struct epoll_event ev {};
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = listen_fd;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
    std::cerr << "[ERROR] Failed to add listening socket to epoll.\n";
    return 1;
  }

  struct epoll_event events[MAX_EVENTS];

  while (server_running) {
  }

  return 0;
}