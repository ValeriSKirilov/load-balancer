#include "lb/net_utils.hpp"

#include <fcntl.h>

#include <iostream>

bool set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    std::cerr << "[ERROR] Failed to get file descriptor flags.\n";
    return false;
  }

  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    std::cerr << "[ERROR] Failed to set file descriptor to non-blocking.\n";
    return false;
  }

  return true;
}

bool set_blocking(int fd) {
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    std::cerr << "[ERROR] Failed to get file descriptor flags.\n";
    return false;
  }

  if (fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) == -1) {
    std::cerr << "[ERROR] Failed to set file descriptor to blocking.\n";
    return false;
  }

  return true;
}