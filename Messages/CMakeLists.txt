# The messages workers exchange. They depend on the socket and wire types
# because connections and decoded frames travel inside messages. The one source
# file only compiles the header on its own.
add_library(Messages STATIC Messages.cpp)
# Wire.hpp lives at the project root.
target_include_directories(Messages PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR})
target_link_libraries(Messages PUBLIC Common TcpSocket)
target_compile_features(Messages PUBLIC cxx_std_23)
