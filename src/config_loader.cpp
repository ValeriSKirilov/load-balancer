#include "lb/config_loader.hpp"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "lb/constants.hpp"

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
    if (line.empty() || line[0] == '#') {
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

  if (servers.empty()) {
    std::fprintf(stderr, INVALID_CONFIG_FILE_ERROR, config_path.c_str());
  }

  return servers;
}