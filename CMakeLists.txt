cmake_minimum_required(VERSION 3.16)
project(load_balancer VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "This project uses epoll and only builds on Linux.")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

# ---- Core library (everything except main) ----
add_library(lb_core STATIC
  src/backend_server.cpp
  src/config_loader.cpp
  src/time_utils.cpp
  src/net_utils.cpp
  src/algorithms.cpp
  src/health_check.cpp
  src/http_response.cpp
  src/waf.cpp
  src/rate_limiter.cpp
  src/metrics.cpp
  src/proxy.cpp
  src/client_handler.cpp
  src/server.cpp
)

target_include_directories(lb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(lb_core PUBLIC Threads::Threads)
target_compile_options(lb_core PRIVATE -Wall -Wextra -Wpedantic)

# ---- Executable ----
add_executable(load_balancer src/main.cpp)
target_link_libraries(load_balancer PRIVATE lb_core)
target_compile_options(load_balancer PRIVATE -Wall -Wextra -Wpedantic)

# Copy the sample config next to the binary (only if it exists)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/backend.conf)
  configure_file(backend.conf ${CMAKE_CURRENT_BINARY_DIR}/backend.conf COPYONLY)
endif()

# Optional sanitizer build: cmake -B build -DLB_SANITIZE=ON
option(LB_SANITIZE "Enable ASan/UBSan" OFF)
if(LB_SANITIZE)
  target_compile_options(lb_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(lb_core PUBLIC -fsanitize=address,undefined)
endif()

option(LB_TSAN "Enable ThreadSanitizer" OFF)
if(LB_TSAN)
  target_compile_options(lb_core PUBLIC -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(lb_core PUBLIC -fsanitize=thread)
endif()