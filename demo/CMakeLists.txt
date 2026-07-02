cmake_minimum_required(VERSION 3.14)

project(caf-flow-demo VERSION 1.0.0 LANGUAGES CXX)

# ---- C++ standard -----------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---- Fetch CAF from GitHub --------------------------------------------------
include(FetchContent)

FetchContent_Declare(
  CAF
  GIT_REPOSITORY https://github.com/actor-framework/actor-framework.git
  GIT_SHALLOW    ON
  GIT_PROGRESS   ON
)

# Suppress CAF's own test/example build so we only get the libraries.
set(CAF_ENABLE_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(CAF)

# ---- Demo executable --------------------------------------------------------
add_executable(caf-flow-demo main.cpp)
target_link_libraries(caf-flow-demo PRIVATE CAF::core)

# Copy the built demo to the binary dir next to the source.
set_target_properties(caf-flow-demo PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)
