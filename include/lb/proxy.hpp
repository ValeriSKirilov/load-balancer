#pragma once

#include <string>

#include "lb/backend_server.hpp"

bool forward_to_backend(int client_fd, const std::string& request,
                        BackendServer& server);