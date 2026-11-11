#pragma once

#include <string>
#include <vector>

#include "lb/backend_server.hpp"

std::vector<BackendServer> load_backend_servers(const std::string& config_path);