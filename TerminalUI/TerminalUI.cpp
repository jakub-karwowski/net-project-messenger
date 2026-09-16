#include "TerminalUI.hpp"
#include <iostream>
#include <stdexcept>
#include <memory>
#include <optional>
#include <termios.h>
#include <unistd.h>
#include <sys/ioctl.h>

namespace ansi {
    inline std::string move_to(int row, int col) {
        return "\033[" + std::to_string(row) + ";" + std::to_string(col) + "H";
    }
    inline constexpr std::string_view clear_line = "\033[2K";
    inline constexpr std::string_view clear_screen = "\033[2J";
    inline constexpr std::string_view hide_cursor = "\033[?25l";
    inline constexpr std::string_view show_cursor = "\033[?25h";
}

struct TerminalUI::TermImpl {
    termios obj; 
};

TerminalUI::TerminalUI() : originalTermState(std::make_unique<TerminalUI::TermImpl>()) {
    if (tcgetattr(STDIN_FILENO, &originalTermState->obj) < 0) {
        throw std::runtime_error("Failed to get Termios");
    }

    termios raw_termios{originalTermState->obj};
    raw_termios.c_lflag &= ~(ICANON | ECHO);
    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw_termios) < 0) {
        throw std::runtime_error("Failed to configure Termios");
    }
}

TerminalUI::~TerminalUI() {
    if (originalTermState) {
        tcsetattr(STDIN_FILENO, TCSANOW, &originalTermState->obj);
    }
    std::cout << ansi::show_cursor << std::flush;
}

std::optional<Renderable::TerminalSize> TerminalUI::get_terminal_size() const noexcept {
    winsize win;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &win) < 0) {
        return std::nullopt;
    }
    return Renderable::TerminalSize{win.ws_row, win.ws_col};
}

void TerminalUI::clear() const noexcept {
    std::cout 
        << ansi::clear_screen 
        << ansi::move_to(1, 1) 
        << std::flush;
}

bool TerminalUI::render(Renderable& renderable) {
    auto win = get_terminal_size();
    if (!win) { return false; }
    while (!renderable.empty()) {
        auto comm = renderable.get_command(*win);
        switch (comm.action) {
            case Renderable::DrawCommand::Action::ClearScreen:
                std::cout 
                    << ansi::clear_screen 
                    << ansi::move_to(1, 1);
                break;
        
            case Renderable::DrawCommand::Action::ClearLineAndDraw:
                std::cout 
                    << ansi::move_to(comm.start_pos.row, 1)
                    << ansi::clear_line
                    << ansi::move_to(comm.start_pos.row, comm.start_pos.col)
                    << comm.content;
                break;
        
            case Renderable::DrawCommand::Action::DrawText:
                std::cout 
                    << ansi::move_to(comm.start_pos.row, comm.start_pos.col)
                    << comm.content;
                break;
        }
        
        if (comm.end_pos) {
            std::cout 
                << ansi::move_to(comm.end_pos->row, comm.end_pos->col);
        }
    }
    std::cout << std::flush;
    return true;
}