add_executable(test_pool test_pool.cpp)
target_link_libraries(test_pool PRIVATE TcpSocket Threads::Threads)
add_test(NAME test_pool COMMAND test_pool)
