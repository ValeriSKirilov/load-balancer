#pragma once

#include <functional>
#include <string>
#include <vector>

#include "lb/backend_server.hpp"

using RouterFunc =
    std::function<int(const std::vector<BackendServer>&, const std::string&)>;

RouterFunc get_algorithm(const std::string& name);

bool set_active_algorithm(const std::string& name);
std::string get_active_algorithm_name();
int route_with_active_algorithm(const std::vector<BackendServer>& servers,
                                const std::string& request);

void init_wrr_schedule(const std::vector<BackendServer>& servers);