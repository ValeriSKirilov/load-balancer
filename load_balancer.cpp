#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

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

static std::atomic<int> rr_index{0};

struct BackendServer {
  std::string ip;
  int port;
  int weight;
  sockaddr_in addr{};

  std::atomic<int> active_connections{0};
  std::atomic<bool> is_healthy{true};
  std::atomic<long long> avg_latency_time{1};
  std::atomic<long long> last_response_time{0};

  BackendServer(const std::string& server_ip, int server_port,
                int server_weight)
      : ip(server_ip), port(server_port), weight(server_weight) {
    addr.sin_family = AF_INET;
    addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip.c_str(), &addr.sin_addr);
  }

  BackendServer(BackendServer&& other) noexcept
      : ip(std::move(other.ip)),
        port(other.port),
        weight(other.weight),
        addr(other.addr),
        active_connections(other.active_connections.load()),
        is_healthy(other.is_healthy.load()),
        avg_latency_time(other.avg_latency_time.load()),
        last_response_time(other.last_response_time.load()) {}
};

std::map<std::string, std::function<int(const std::vector<BackendServer>&)>>
    algorithm_map = {{"ROUND_ROBIN",
                      [](const std::vector<BackendServer>&) {
                        return 0; /* Implement ROUND_ROBIN logic */
                      }},
                     {"WEIGHTED_RR",
                      [](const std::vector<BackendServer>&) {
                        return 0; /* Implement WEIGHTED_RR logic */
                      }},
                     {"LEAST_CONN",
                      [](const std::vector<BackendServer>&) {
                        return 0; /* Implement LEAST_CONN logic */
                      }},
                     {"LEAST_TIME", [](const std::vector<BackendServer>&) {
                        return 0; /* Implement LEAST_TIME logic */
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
      in_addr dummy_addr{};
      if (inet_pton(AF_INET, ip.c_str(), &dummy_addr) == 1) {
        servers.emplace_back(ip, port, weight);
      }
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
  int sockFd = socket(AF_INET, SOCK_STREAM, 0);
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

  if (connect(sockFd, reinterpret_cast<sockaddr*>(&server.addr),
              sizeof(server.addr)) < 0) {
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

int main(const int argc, const char* argv[]) {
  if (argc > 1 &&
      (reinterpret_cast<const char*>(argv[1]) == std::string("-help") ||
       reinterpret_cast<const char*>(argv[1]) == std::string("-h"))) {
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

  return 0;
}