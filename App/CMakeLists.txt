add_executable(node main.cpp AppConfig.cpp)
target_include_directories(node PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(node PRIVATE StoreWorker RouterWorker ListenerWorker Frontend)
