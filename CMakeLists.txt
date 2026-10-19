cmake_minimum_required(VERSION 3.20)
project(NetProject LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

add_subdirectory(TcpSocket)
add_subdirectory(TerminalUI)

add_executable(app main.cpp ThreadWorkers.cpp)
target_link_libraries(app PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_queue test_queue.cpp)
target_link_libraries(test_queue PRIVATE Threads::Threads)

add_executable(test_wire test_wire.cpp)

add_executable(test_socket test_socket.cpp)
target_link_libraries(test_socket PRIVATE Threads::Threads TcpSocket)

add_executable(test_terminal test_terminal.cpp)
target_link_libraries(test_terminal PRIVATE TerminalUI)

add_executable(test_chat test_chat.cpp)
target_link_libraries(test_chat PRIVATE TerminalUI Threads::Threads)

add_executable(test_p2p test_p2p.cpp ThreadWorkers.cpp)
target_link_libraries(test_p2p PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_workers test_workers.cpp ThreadWorkers.cpp)
target_link_libraries(test_workers PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_sender test_sender.cpp ThreadWorkers.cpp)
target_link_libraries(test_sender PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_store test_store.cpp ThreadWorkers.cpp)
target_link_libraries(test_store PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_render test_render.cpp ThreadWorkers.cpp)
target_link_libraries(test_render PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_main test_main.cpp ThreadWorkers.cpp)
target_link_libraries(test_main PRIVATE TcpSocket TerminalUI Threads::Threads)

add_executable(test_logger test_logger.cpp ThreadWorkers.cpp)
target_link_libraries(test_logger PRIVATE TcpSocket TerminalUI Threads::Threads)

add_test(NAME test_queue COMMAND test_queue)
add_test(NAME test_wire COMMAND test_wire)
add_test(NAME test_p2p COMMAND test_p2p)
add_test(NAME test_sender COMMAND test_sender)
add_test(NAME test_workers COMMAND test_workers)
add_test(NAME test_store COMMAND test_store)
add_test(NAME test_render COMMAND test_render)
add_test(NAME test_main COMMAND test_main)
add_test(NAME test_logger COMMAND test_logger)
# --- Render (T3): ChatController / TerminalRender / HeadlessRender ------------
add_library(Render STATIC Render.cpp)
target_include_directories(Render PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Render PUBLIC TcpSocket TerminalUI Threads::Threads)
foreach(render_user app test_p2p test_workers test_sender test_store test_render test_main test_logger)
    target_link_libraries(${render_user} PRIVATE Render)
endforeach()

add_executable(test_headless test_headless.cpp)
target_link_libraries(test_headless PRIVATE Render TcpSocket TerminalUI Threads::Threads)
add_test(NAME test_headless COMMAND test_headless)
