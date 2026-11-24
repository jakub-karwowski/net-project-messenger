add_library(LoadGen LoadGen.cpp)
# Json.hpp (header-only) comes from the frontend; Wire.hpp from the project root.
target_include_directories(LoadGen PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/Frontend)
target_link_libraries(LoadGen PUBLIC TcpSocket)
target_compile_features(LoadGen PUBLIC cxx_std_23)

add_executable(loadgen main.cpp)
target_link_libraries(loadgen PRIVATE LoadGen)
