#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <cerrno>
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
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
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

static std::atomic<long long> blocked_attacks{0};

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

struct ClientContext {
  int client_fd;
  std::string request_buffer;
  std::string client_ip;
  long long connected_at_ms;
};

struct HttpResponse {
  int status_code;
  std::string status_text;
  std::map<std::string, std::string> headers;
  std::string body;

  std::string to_raw_string() const {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status_code << " " << status_text << "\r\n";
    for (const auto& [key, value] : headers) {
      oss << key << ": " << value << "\r\n";
    }

    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "\r\n";

    oss << body;
    return oss.str();
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

bool set_blocking(int fd) {
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    std::cerr << "[ERROR] Failed to get file descriptor flags.\n";
    return false;
  }

  if (fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) == -1) {
    std::cerr << "[ERROR] Failed to set file descriptor to blocking.\n";
    return false;
  }

  return true;
}

int hex_char_to_int(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return 0;
}

std::string url_decode_once(std::string_view input) {
  std::string result;
  for (size_t i = 0; i < input.size(); ++i) {
    if (input[i] == '+') {
      result.push_back(' ');
    } else if (input[i] == '%' && i + 2 < input.size() &&
               std::isxdigit(static_cast<unsigned char>(input[i + 1])) &&
               std::isxdigit(static_cast<unsigned char>(input[i + 2]))) {
      int high, low;

      high = hex_char_to_int(input[i + 1]);
      low = hex_char_to_int(input[i + 2]);

      result.push_back(static_cast<char>((high << 4) | low));
      i += 2;
    } else {
      result.push_back(input[i]);
    }
  }

  return result;
}

std::string normalize_payload(std::string_view raw_request) {
  std::string current(raw_request);

  for (int i = 0; i < 3; ++i) {
    std::string next = url_decode_once(current);
    if (next == current) {
      break;
    }
    current = std::move(next);
  }
  return current;
}

bool inspect_request(const std::string& request) {
  std::string normalized_request = normalize_payload(request);

  static const std::regex master_sqli_rule(
      R"((\b(?:OR|AND)\b\s+(['"]\w+['"]|\d+)\s*=\s*\2)|)"
      R"((\bUNION\b\s+(?:\bALL\b\s+)?\bSELECT\b)|)"
      R"((;\s*(?:DROP|EXEC|ALTER|TRUNCATE)\b)|)"
      R"((\b(?:SLEEP|pg_sleep|WAITFOR\s+DELAY)\b)|)"
      R"((['";]\s*(?:--|\#))|(\/\*[\s\S]*?\*\/)|)"
      R"((<\s*script\b|javascript\s*:)|)"
      R"((\.\.\/|\.\.\\|\/etc\/passwd))",
      std::regex::icase | std::regex::optimize);

  std::smatch match;

  if (std::regex_search(normalized_request, match, master_sqli_rule)) {
    std::cerr << "[BLOCKED] Full HTTP string: " << request << "\n";
    std::cerr << "  -> Exact offending trigger (match[0]): " << match.str(0)
              << "\n";
    std::cerr << "  -> Trigger started at byte offset: " << match.position(0)
              << "\n";

    if (match[1].matched) {
      std::cerr << "  -> Tautology value repeated (match[2]): " << match.str(2)
                << "\n";
    }
    return true;
  }

  return false;
}

std::string build_metrics_json(const std::vector<BackendServer>& servers) {
  std::ostringstream oss;
  oss << "{\n";
  oss << "  \"blocked_attacks\": "
      << blocked_attacks.load(std::memory_order_relaxed) << ",\n";
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

void handle_client(
    ClientContext* ctx, std::vector<BackendServer>& servers,
    std::function<int(const std::vector<BackendServer>&)> selected_router_func,
    int epoll_fd) {
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

  if (inspect_request(ctx->request_buffer)) {
    HttpResponse response_403 = {403, "Forbidden", {}, "Forbidden"};
    std::string response_str = response_403.to_raw_string();
    if (send(ctx->client_fd, response_str.c_str(), response_str.size(), 0) ==
        -1) {
      std::cerr << "[ERROR] Failed to send 403 response to client.\n";
    }
    blocked_attacks.fetch_add(1, std::memory_order_relaxed);
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

  if (set_blocking(ctx->client_fd) == false) {
    std::cerr << "[ERROR] Failed to set client socket to blocking.\n";
    close(ctx->client_fd);
    delete ctx;
    return;
  }

  bool forwarded_successfuly = false;
  size_t max_retries = servers.size();

  for (size_t attempts = 0; attempts < max_retries; ++attempts) {
    int backend_idx = selected_router_func(servers);
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
      long long new_avg_latency =
          std::max(1LL, (servers[backend_idx].avg_latency_time.load(
                             std::memory_order_relaxed) +
                         (end_time - start_time)) /
                            2);
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
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  if (getaddrinfo(nullptr, "8080", &hints, &res) != 0) {
    std::cerr << "[ERROR] Failed to get address information.\n";
    return 1;
  }

  struct addrinfo* p;
  for (p = res; p != nullptr; p = p->ai_next) {
    listen_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (listen_fd == -1) {
      continue;
    }

    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) ==
        -1) {
      std::cerr << "[ERROR] Failed to set socket option.\n";
      close(listen_fd);
      continue;
    }

    if (bind(listen_fd, p->ai_addr, p->ai_addrlen) == 0) {
      break;  // Successfully bound
    }

    close(listen_fd);
  }

  freeaddrinfo(res);
  if (p == nullptr) {
    std::cerr << "[ERROR] Failed to bind to any address.\n";
    return 1;
  }

  if (set_nonblocking(listen_fd) == false) {
    std::cerr << "[ERROR] Failed to set listening socket to non-blocking.\n";
    close(listen_fd);
    return 1;
  }

  if (listen(listen_fd, SOMAXCONN) == -1) {
    std::cerr << "[ERROR] Failed to listen on socket.\n";
    close(listen_fd);
    return 1;
  }

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    std::cerr << "[ERROR] Failed to create epoll instance.\n";
    close(listen_fd);
    return 1;
  }

  ClientContext listen_ctx{listen_fd, ""};
  struct epoll_event ev {};
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = &listen_ctx;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
    std::cerr << "[ERROR] Failed to add listening socket to epoll.\n";
    close(listen_fd);
    close(epoll_fd);
    return 1;
  }

  struct epoll_event events[MAX_EVENTS];

  std::thread health_thread([&servers] {
    while (server_running) {
      for (auto& server : servers) {
        health_check(server);
      }
      std::this_thread::sleep_for(std::chrono::seconds(5));
    }
  });

  while (server_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "[ERROR] Failed to wait on epoll.\n";
      server_running = false;
    }

    int client_fd;
    ClientContext* ctx;

    for (int i = 0; i < nfds; i++) {
      ctx = static_cast<ClientContext*>(events[i].data.ptr);
      if (ctx->client_fd == listen_fd) {
        struct sockaddr_storage client_addr {};
        socklen_t client_len = sizeof(client_addr);

        while ((client_fd = accept(
                    listen_fd, reinterpret_cast<struct sockaddr*>(&client_addr),
                    &client_len)) != -1) {
          char client_ip[NI_MAXHOST];
          if (getnameinfo(reinterpret_cast<struct sockaddr*>(&client_addr),
                          client_len, client_ip, sizeof(client_ip), nullptr, 0,
                          NI_NUMERICHOST) != 0) {
            std::cerr << "[ERROR] Failed to get client IP.\n";
            close(client_fd);
            client_addr = {};
            client_len = sizeof(client_addr);
            continue;
          }

          client_addr = {};
          client_len = sizeof(client_addr);

          if (set_nonblocking(client_fd) == false) {
            std::cerr
                << "[ERROR] Failed to set client socket to non-blocking.\n";
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

        if (client_fd == -1) {
          if (errno != EAGAIN && errno != EWOULDBLOCK) {
            std::cerr << "[ERROR] accept() failed\n";
          }
        }
      } else if (events[i].events & EPOLLERR || events[i].events & EPOLLHUP) {
        std::cerr << "[ERROR] Epoll error on fd: " << ctx->client_fd << "\n";
        close(ctx->client_fd);
        delete ctx;
      } else if (events[i].events & EPOLLIN) {
        pool.enqueue([ctx, &servers, selected_router_func, epoll_fd] {
          handle_client(ctx, servers, selected_router_func, epoll_fd);
        });
      }
    }
  }

  if (health_thread.joinable()) {
    health_thread.join();
  }
  close(listen_fd);
  close(epoll_fd);

  return 0;
}