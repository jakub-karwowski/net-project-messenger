#include <chrono>
#include <cstdlib>
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

void test_try_pop() {
    ThreadQueue<int> q;

    // Empty queue: must return immediately instead of blocking.
    if (q.try_pop().has_value()) { std::cerr << "try_pop on empty returned a value\n"; std::exit(1); }

    for (int i = 0; i < 5; ++i) {
        if (!q.push(i)) { std::cerr << "push failed\n"; std::exit(1); }
    }

    // Drain everything currently queued.
    int drained = 0;
    while (q.try_pop()) { ++drained; }
    if (drained != 5) { std::cerr << "drained " << drained << " expected 5\n"; std::exit(1); }

    // Queue is momentarily empty but still open: pop() must keep waiting for new work.
    std::thread producer([&q] {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (!q.push(42)) { std::cerr << "push failed\n"; std::exit(1); }
        q.graceful_shutdown();
    });
    auto value = q.pop();
    if (!value || *value != 42) { std::cerr << "pop() did not resume waiting after try_pop\n"; std::exit(1); }
    producer.join();

    std::cout << "test_try_pop passed\n";
}


int main() {
    ThreadQueue<int> q;
    std::thread t1([&q] {produce(q); });
    std::thread t2([&q] {consume(q); });

    t1.join();
    t2.join();

    test_try_pop();
}