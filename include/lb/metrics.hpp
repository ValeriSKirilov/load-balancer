#pragma once

#include <string>
#include <vector>

#include "lb/backend_server.hpp"

void record_blocked_attack();
long long get_blocked_attacks();

std::string build_metrics_json(const std::vector<BackendServer>& servers);