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
