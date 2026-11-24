#include "TerminalState.hpp"

#include <atomic>
#include <termios.h>
#include <tuple>
#include <unistd.h>

namespace terminal_state {

namespace {

termios g_saved{};
std::atomic<bool> g_have{false};

}  // namespace

bool save() noexcept {
    if (::tcgetattr(STDIN_FILENO, &g_saved) != 0) { return false; }
    g_have.store(true, std::memory_order_release);
    return true;
}

void restore() noexcept {
    if (!g_have.load(std::memory_order_acquire)) { return; }
    std::ignore = ::tcsetattr(STDIN_FILENO, TCSANOW, &g_saved);
    static constexpr char kReset[] = "\033[0m\033[?25h\033[2J\033[1;1H";
    std::ignore = ::write(STDOUT_FILENO, kReset, sizeof kReset - 1);
}

}  // namespace terminal_state
