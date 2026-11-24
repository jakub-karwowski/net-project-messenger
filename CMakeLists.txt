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

add_subdirectory(Common)
add_subdirectory(TcpSocket)
add_subdirectory(Messages)
add_subdirectory(StoreWorker)
add_subdirectory(IoWorker)
add_subdirectory(RouterWorker)
add_subdirectory(ListenerWorker)
add_subdirectory(Frontend)
add_subdirectory(App)
add_subdirectory(TerminalUI)

add_subdirectory(tests)
