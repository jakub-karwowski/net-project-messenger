#include "TerminalUI.hpp"
#include "Utils.hpp"
#include <cerrno>
#include <iostream>
#include <stdexcept>
#include <memory>
#include <optional>
#include <utility>
#include <poll.h>
#include <termios.h>
#include <unistd.h>
#include <sys/ioctl.h>

namespace ansi {
    inline std::string move_to(int row, int col) {
        return "\033[" + std::to_string(row) + ";" + std::to_string(col) + "H";
    }
    inline constexpr std::string_view reset = "\033[0m";
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
    // ISIG is cleared as well, so Ctrl+C arrives as the byte 0x03 instead of
    // raising SIGINT. A SIGINT would terminate the process before ~TerminalUI()
    // could restore the terminal (leaving it in raw mode); as a byte it flows
    // through the normal input path and shutdown stays orderly.
    raw_termios.c_lflag &= ~(ICANON | ECHO | ISIG);
    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw_termios) < 0) {
        throw std::runtime_error("Failed to configure Termios");
    }
}

TerminalUI::~TerminalUI() {
    if (originalTermState) {
        tcsetattr(STDIN_FILENO, TCSANOW, &originalTermState->obj);
    }
    std::cout << ansi::clear_screen << ansi::show_cursor << ansi::move_to(1, 1) << std::flush;
}

std::optional<TerminalUI::TerminalSize> TerminalUI::get_terminal_size() const noexcept {
    winsize win;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &win) < 0) {
        return std::nullopt;
    }
    return TerminalUI::TerminalSize{win.ws_col, win.ws_row};
}

void TerminalUI::clear() const noexcept {
    std::cout 
        << ansi::clear_screen 
        << ansi::move_to(1, 1) 
        << std::flush;
}

namespace {
    // poll(2) that retries on EINTR so a signal does not masquerade as a timeout.
    int poll_input(int timeout_ms) noexcept {
        pollfd descriptor{STDIN_FILENO, POLLIN, 0};
        int result = 0;
        do {
            result = ::poll(&descriptor, 1, timeout_ms);
        } while (result < 0 && errno == EINTR);
        return result;
    }
}

TerminalUI::ReadResult TerminalUI::read_byte(unsigned char& byte, int timeout_ms) const noexcept {
    const int ready = poll_input(timeout_ms);
    if (ready == 0) { return ReadResult::Timeout; }
    if (ready < 0) { return ReadResult::Eof; }

    ssize_t count = 0;
    do {
        count = ::read(STDIN_FILENO, &byte, 1);
    } while (count < 0 && errno == EINTR);
    return (count == 1) ? ReadResult::Byte : ReadResult::Eof;
}

std::string TerminalUI::read_utf8_char(unsigned char lead, int timeout_ms) const noexcept {
    std::string text(1, static_cast<char>(lead));
    const size_t length = utils::utf8::sequence_length(lead);
    for (size_t i = 1; i < length; ++i) {
        unsigned char continuation = 0;
        if (read_byte(continuation, timeout_ms) != ReadResult::Byte) { break; }
        text.push_back(static_cast<char>(continuation));
    }
    return text;
}

bool TerminalUI::render(Renderable& renderable) {
    auto win = get_terminal_size();
    if (!win) { return false; }
    renderable.resize(win->width, win->height);
    const std::optional<Renderable::Cursor> cursor = renderable.write_frame(frame_buffer);

    // Repaint only the rows whose cells differ from the frame currently on
    // screen. row_equal() also reports a mismatch when the size changed, so a
    // resize repaints everything. Starting each row with a reset keeps a row
    // self-contained: its cells carry the SGR prefixes they were rendered with,
    // but the colour state left by the previous row is not reused.
    std::string sequence;
    bool redrawn = false;
    for (int row = 0; row < frame_buffer.height; ++row) {
        if (previous_frame.row_equal(frame_buffer, row)) { continue; }
        redrawn = true;
        sequence += ansi::move_to(row + 1, 1);
        sequence += ansi::reset;
        sequence += ansi::clear_line;
        sequence += frame_buffer.row_text(row);
    }

    // Nothing to paint and the cursor is where it already was: stay silent.
    if (!redrawn && cursor == previous_cursor) { return true; }

    sequence += ansi::reset;
    if (cursor && cursor->state == Renderable::Cursor::State::VISIBLE) {
        sequence += ansi::show_cursor;
        sequence += ansi::move_to(cursor->y, cursor->x);
    } else {
        sequence += ansi::hide_cursor;
    }

    std::cout << sequence << std::flush;

    // write_frame() resizes (and therefore blanks) `frame_buffer` before filling
    // it, so swapping is safe and avoids copying the whole grid every frame.
    std::swap(previous_frame, frame_buffer);
    previous_cursor = cursor;
    return true;
}