add_executable(test_pool test_pool.cpp)
target_link_libraries(test_pool PRIVATE TcpSocket Threads::Threads)
add_test(NAME test_pool COMMAND test_pool)

add_executable(test_listener test_listener.cpp)
target_link_libraries(test_listener PRIVATE TcpSocket Threads::Threads)
add_test(NAME test_listener COMMAND test_listener)

add_executable(test_queue test_queue.cpp)
target_link_libraries(test_queue PRIVATE Common)
add_test(NAME test_queue COMMAND test_queue)

add_executable(test_terminal_ui test_terminal_ui.cpp)
target_link_libraries(test_terminal_ui PRIVATE TerminalUI)
add_test(NAME test_terminal_ui COMMAND test_terminal_ui)

add_executable(test_cells test_cells.cpp)
target_link_libraries(test_cells PRIVATE TerminalUI)
add_test(NAME test_cells COMMAND test_cells)

add_executable(test_layout test_layout.cpp)
target_link_libraries(test_layout PRIVATE TerminalUI)
add_test(NAME test_layout COMMAND test_layout)

foreach(t test_logger test_supervisor)
  add_executable(${t} ${t}.cpp)
  target_link_libraries(${t} PRIVATE Common)
  add_test(NAME ${t} COMMAND ${t})
endforeach()

add_executable(test_ids test_ids.cpp)
target_link_libraries(test_ids PRIVATE Messages)
add_test(NAME test_ids COMMAND test_ids)

# Stops workers blocked in the socket library's calls too.
add_executable(test_worker test_worker.cpp)
target_link_libraries(test_worker PRIVATE Common TcpSocket)
add_test(NAME test_worker COMMAND test_worker)

add_executable(test_store test_store.cpp)
target_link_libraries(test_store PRIVATE StoreWorker)
add_test(NAME test_store COMMAND test_store)

add_executable(test_io_worker test_io_worker.cpp)
target_link_libraries(test_io_worker PRIVATE IoWorker)
add_test(NAME test_io_worker COMMAND test_io_worker)

add_executable(test_router test_router.cpp)
target_link_libraries(test_router PRIVATE RouterWorker StoreWorker ListenerWorker BalancerWorker)
add_test(NAME test_router COMMAND test_router)

foreach(t test_commands test_jsonl)
  add_executable(${t} ${t}.cpp)
  target_link_libraries(${t} PRIVATE Frontend)
  add_test(NAME ${t} COMMAND ${t})
endforeach()

# Runs real node processes.
add_executable(test_cli_e2e test_cli_e2e.cpp)
target_compile_definitions(test_cli_e2e PRIVATE NODE_BINARY="$<TARGET_FILE:node>")
add_dependencies(test_cli_e2e node)
add_test(NAME test_cli_e2e COMMAND test_cli_e2e)

add_executable(test_balancer test_balancer.cpp)
target_link_libraries(test_balancer PRIVATE BalancerWorker)
add_test(NAME test_balancer COMMAND test_balancer)

add_executable(test_balancer_e2e test_balancer_e2e.cpp)
target_link_libraries(test_balancer_e2e PRIVATE RouterWorker StoreWorker ListenerWorker BalancerWorker)
add_test(NAME test_balancer_e2e COMMAND test_balancer_e2e)

add_executable(test_tui test_tui.cpp)
target_link_libraries(test_tui PRIVATE Frontend)
add_test(NAME test_tui COMMAND test_tui)

# Runs the node on a pseudo-terminal.
add_executable(test_tui_e2e test_tui_e2e.cpp)
target_compile_definitions(test_tui_e2e PRIVATE NODE_BINARY="$<TARGET_FILE:node>")
target_link_libraries(test_tui_e2e PRIVATE util)
add_dependencies(test_tui_e2e node)
add_test(NAME test_tui_e2e COMMAND test_tui_e2e)
