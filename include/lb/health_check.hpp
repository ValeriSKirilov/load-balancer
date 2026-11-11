#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "lb/backend_server.hpp"

bool health_check(BackendServer& server);

std::thread start_health_thread(std::vector<BackendServer>& servers,
                                const std::atomic<bool>& running);