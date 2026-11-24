add_library(RouterWorker RouterWorker.cpp)
target_include_directories(RouterWorker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(RouterWorker PUBLIC IoWorker Messages Common)
target_compile_features(RouterWorker PUBLIC cxx_std_23)
