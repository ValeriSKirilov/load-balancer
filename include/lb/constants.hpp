#pragma once

inline constexpr int MAX_EVENTS = 128;
inline constexpr int MAX_REQUESTS_PER_SEC = 50;
inline constexpr const char* LISTEN_PORT = "8080";

inline constexpr const char* USAGE =
    "Usage: ./load_balancer <algorithm> <num_threads> <config_file>\n"
    "Available algorithms:\n"
    "  ROUND_ROBIN\n"
    "  WEIGHTED_RR\n"
    "  LEAST_CONN\n"
    "  LEAST_TIME\n"
    "  IP_HASH\n";

inline constexpr const char* INVALID_ALG_ERROR =
    "[ERROR] Invalid load balancing algorithm: \'%s\'.\n"
    "Allowed values : ROUND_ROBIN, WEIGHTED_RR, LEAST_CONN, LEAST_TIME, "
    "IP_HASH\n"
    "Usage : ./load_balancer [ALGORITHM] [THREAD_COUNT] [CONFIG_FILE]\n";

inline constexpr const char* INVALID_THREAD_COUNT_ERROR =
    "[ERROR] Invalid thread count: '%s'. Thread count must be an integer "
    "between 1 and 64.\n";

inline constexpr const char* INVALID_CONFIG_FILE_ERROR =
    "[ERROR] Failed to open configuration file '%s' or no valid "
    "backend servers found.\n"
    "Expected format per line: <IP_ADDRESS> <PORT> <WEIGHT>\n";