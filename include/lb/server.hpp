#pragma once

#include <atomic>
#include <vector>

#include "lb/algorithms.hpp"
#include "lb/backend_server.hpp"
#include "lb/constants.hpp"

extern std::atomic<bool> server_running;

int run_server(std::vector<BackendServer>& servers, RouterFunc router,
               int num_threads, const char* port = LISTEN_PORT);