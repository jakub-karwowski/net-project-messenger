#include <chrono>
#include <iostream>
#include <span>
#include <thread>
#include "TcpSocket.hpp"

void server() {
    TcpListener listener("127.0.0.1", 9991, 1);
    auto conn = listener.accept_client();
    if (!conn) { 
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::cout << "[server] connected\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::string msg(1024, '\0');
    auto result = conn->receive(std::as_writable_bytes(std::span{msg}));
    if (!result) {
        std::cout << "[server] recieve error\n";
        return;
    }
    std::cout << msg << "[end]\n";
}


void client() {
    auto conn = TcpConnection::connect("127.0.0.1", 9991);
    if (!conn) {
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    std::cout << "[client] connected\n";
    const std::string msg{"Hello there!\n"};
    auto result = conn->send(std::as_bytes(std::span{msg}));
    if (!result) {
        std::cout << "[client] send error\n";
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    std::cout << *result << '\n';
}

int main() {
    std::thread t1(server);
    std::thread t2(client);

    t1.join();
    t2.join();
}