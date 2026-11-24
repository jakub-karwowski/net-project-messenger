add_library(TcpSocket TcpSocket.cpp)

target_include_directories(TcpSocket PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(TcpSocket PUBLIC WakeupEvent)
target_compile_features(TcpSocket PUBLIC cxx_std_23)
