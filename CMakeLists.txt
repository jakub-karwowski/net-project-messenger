cmake_minimum_required(VERSION 3.20)
project(NetProject LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

add_subdirectory(TcpSocket)
add_subdirectory(TerminalUI)

# Everything the app and the worker tests share (workers, sessions, transports).
add_library(app_core STATIC
  ThreadWorkers.cpp
  PeerSession.cpp
  BlockingOutbound.cpp
  Transport.cpp
  Metrics.cpp
  Render.cpp)
target_include_directories(app_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(app_core PUBLIC TcpSocket TerminalUI Threads::Threads)

add_executable(app main.cpp)
target_link_libraries(app PRIVATE app_core)

add_executable(test_queue test_queue.cpp)
target_link_libraries(test_queue PRIVATE Threads::Threads)

add_executable(test_wire test_wire.cpp)

add_executable(test_socket test_socket.cpp)
target_link_libraries(test_socket PRIVATE Threads::Threads TcpSocket)

add_executable(test_pool test_pool.cpp)
target_link_libraries(test_pool PRIVATE Threads::Threads TcpSocket)

add_executable(test_terminal test_terminal.cpp)
target_link_libraries(test_terminal PRIVATE TerminalUI)

add_executable(test_chat test_chat.cpp)
target_link_libraries(test_chat PRIVATE TerminalUI Threads::Threads)

add_executable(test_p2p test_p2p.cpp)
target_link_libraries(test_p2p PRIVATE app_core)

add_executable(test_workers test_workers.cpp)
target_link_libraries(test_workers PRIVATE app_core)

add_executable(test_sender test_sender.cpp)
target_link_libraries(test_sender PRIVATE app_core)

add_executable(test_store test_store.cpp)
target_link_libraries(test_store PRIVATE app_core)

add_executable(test_render test_render.cpp)
target_link_libraries(test_render PRIVATE app_core)

add_executable(test_main test_main.cpp)
target_link_libraries(test_main PRIVATE app_core)

add_executable(test_logger test_logger.cpp)
target_link_libraries(test_logger PRIVATE app_core)

add_test(NAME test_pool COMMAND test_pool)
add_test(NAME test_queue COMMAND test_queue)
add_test(NAME test_wire COMMAND test_wire)
add_test(NAME test_p2p COMMAND test_p2p)
add_test(NAME test_sender COMMAND test_sender)
add_test(NAME test_workers COMMAND test_workers)
add_test(NAME test_store COMMAND test_store)
add_test(NAME test_render COMMAND test_render)
add_test(NAME test_main COMMAND test_main)
add_test(NAME test_logger COMMAND test_logger)
add_executable(test_headless test_headless.cpp)
target_link_libraries(test_headless PRIVATE app_core)
add_test(NAME test_headless COMMAND test_headless)

# --- stressgen (load generator) ---
add_executable(stressgen stressgen.cpp)
target_link_libraries(stressgen PRIVATE TcpSocket Threads::Threads)

add_executable(test_stressgen test_stressgen.cpp)
target_link_libraries(test_stressgen PRIVATE TcpSocket Threads::Threads)
add_test(NAME test_stressgen COMMAND test_stressgen)

# --- PoolOutbound (T6): --io=pool-sender ---
add_executable(test_pool_outbound test_pool_outbound.cpp)
target_link_libraries(test_pool_outbound PRIVATE app_core)
add_test(NAME test_pool_outbound COMMAND test_pool_outbound)

add_executable(test_metrics test_metrics.cpp)
target_link_libraries(test_metrics PRIVATE app_core)
add_test(NAME test_metrics COMMAND test_metrics)

# End-to-end tests: real headless processes (python3 stdlib only).
find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
  foreach(io threads pool-sender)
    add_test(NAME e2e_${io}
      COMMAND ${Python3_EXECUTABLE} -m unittest discover -s ${CMAKE_CURRENT_SOURCE_DIR}/tests/e2e -p "test_e2e.py" -v)
    set_tests_properties(e2e_${io} PROPERTIES
      ENVIRONMENT "E2E_IO=${io};APP_BIN=$<TARGET_FILE:app>"
      TIMEOUT 120)
  endforeach()
endif()
