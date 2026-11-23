add_executable(test_pool test_pool.cpp)
target_link_libraries(test_pool PRIVATE TcpSocket Threads::Threads)
add_test(NAME test_pool COMMAND test_pool)

add_executable(test_listener test_listener.cpp)
target_link_libraries(test_listener PRIVATE TcpSocket Threads::Threads)
add_test(NAME test_listener COMMAND test_listener)

add_executable(test_queue test_queue.cpp)
target_include_directories(test_queue PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(test_queue PRIVATE Threads::Threads)
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
