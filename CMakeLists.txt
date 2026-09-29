cmake_minimum_required(VERSION 3.20)
project(NetProject LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(TcpSocket)
add_subdirectory(TerminalUI)

add_executable(app main.cpp)

add_executable(test_queue test_queue.cpp)
target_link_libraries(test_queue PRIVATE Threads::Threads)

add_executable(test_socket test_socket.cpp)
target_link_libraries(test_socket PRIVATE Threads::Threads TcpSocket)

add_executable(test_terminal test_terminal.cpp)
target_link_libraries(test_terminal PRIVATE TerminalUI)

add_executable(test_chat test_chat.cpp)
target_link_libraries(test_chat PRIVATE TerminalUI Threads::Threads)