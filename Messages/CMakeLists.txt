# The messages workers exchange. Header-only; it depends on the socket and wire
# types because connections and decoded frames travel inside messages.
add_library(Messages INTERFACE)
# Wire.hpp lives at the project root.
target_include_directories(Messages INTERFACE ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR})
target_link_libraries(Messages INTERFACE Common TcpSocket)
