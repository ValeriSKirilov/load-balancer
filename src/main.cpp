#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "lb/algorithms.hpp"
#include "lb/backend_server.hpp"
#include "lb/config_loader.hpp"
#include "lb/constants.hpp"
#include "lb/server.hpp"

int main(int argc, char* argv[]) {
  if (argc > 1 &&
      (argv[1] == std::string("-help") || argv[1] == std::string("-h"))) {
    std::cerr << USAGE;
    return 1;
  }

  std::string algorithm_str = "ROUND_ROBIN";
  int num_threads = 4;
  std::string configFile = "backend.conf";

  if (argc >= 2) {
    algorithm_str = argv[1];
  }

  RouterFunc router = get_algorithm(algorithm_str);
  if (!router) {
    std::fprintf(stderr, INVALID_ALG_ERROR, algorithm_str.c_str());
    return 1;
  }
  set_active_algorithm(algorithm_str);

  if (argc >= 3) {
    try {
      int threads_count = std::stoi(argv[2]);
      if (threads_count < 1 || threads_count > 64) {
        std::fprintf(stderr, INVALID_THREAD_COUNT_ERROR, argv[2]);
        return 1;
      }
      num_threads = threads_count;
    } catch (const std::invalid_argument&) {
      std::fprintf(stderr, INVALID_THREAD_COUNT_ERROR, argv[2]);
      return 1;
    } catch (const std::out_of_range&) {
      std::fprintf(stderr, INVALID_THREAD_COUNT_ERROR, argv[2]);
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

  init_wrr_schedule(servers);

  return run_server(servers, router, num_threads);
}