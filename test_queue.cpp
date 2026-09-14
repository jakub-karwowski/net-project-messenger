#include <chrono>
#include <iostream>
#include <thread>
#include "ThreadQueue.hpp"

void produce(ThreadQueue<int>& queue) {
    for (int i = 0; i < 10; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if ( !queue.push(i)) { return; }
    }
    queue.graceful_shutdown();
}

void consume(ThreadQueue<int>& queue) {
    while (auto value = queue.pop()) {
        std::cout << *value << '\n';
    }
}


int main() {
    ThreadQueue<int> q;
    std::thread t1([&q] {produce(q); });
    std::thread t2([&q] {consume(q); });

    t1.join();
    t2.join();
}