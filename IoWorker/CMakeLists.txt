add_library(IoWorker IoWorker.cpp)
target_include_directories(IoWorker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(IoWorker PUBLIC Messages Common)
target_compile_features(IoWorker PUBLIC cxx_std_23)
