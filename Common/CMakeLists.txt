# The wake-up event alone, so the socket library can use it without depending
# on the rest of Common.
add_library(WakeupEvent WakeupEvent.cpp)
target_include_directories(WakeupEvent PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(WakeupEvent PUBLIC cxx_std_23)

# Concurrency building blocks shared by every worker: queues, ids, logging,
# worker threads and the shutdown supervisor. No networking.
add_library(Common Logger.cpp Worker.cpp Supervisor.cpp)
target_include_directories(Common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Common PUBLIC WakeupEvent Threads::Threads)
target_compile_features(Common PUBLIC cxx_std_23)
