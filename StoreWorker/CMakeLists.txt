add_library(StoreWorker StoreWorker.cpp)
target_include_directories(StoreWorker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(StoreWorker PUBLIC Messages Common)
target_compile_features(StoreWorker PUBLIC cxx_std_23)
