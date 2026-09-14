cmake_minimum_required(VERSION 3.20)
project(NetProject LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(app main.cpp)

add_executable(test_queue test_queue.cpp)
target_link_libraries(test_queue PRIVATE Threads::Threads)