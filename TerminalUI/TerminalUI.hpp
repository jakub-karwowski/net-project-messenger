#pragma once

#include <memory>
#include <optional>
#include <string>

struct Renderable {
    struct Position {
        int row{1};
        int col{1};
    };
    struct DrawCommand {
        enum struct Action {
            DrawText,          
            ClearLineAndDraw,  
            ClearScreen       
        };
        Action action{Action::DrawText};
        std::string content;
        Position start_pos;
        std::optional<Position> end_pos;
    };
    virtual ~Renderable() = default;
    virtual DrawCommand get_command() = 0;
    virtual void resize(int width, int height) = 0;
    virtual bool empty() const noexcept = 0;
};

class TerminalUI {
public:
    // Outcome of a timed read of a single input byte.
    enum class ReadResult { Byte, Timeout, Eof };

    TerminalUI();
    TerminalUI(const TerminalUI&) = delete;
    TerminalUI& operator=(const TerminalUI&) = delete;
    TerminalUI(TerminalUI&& other) = delete;
    TerminalUI& operator=(TerminalUI&& other) = delete;
    ~TerminalUI();

    bool render(Renderable& renderable);
    void clear() const noexcept;

    // Waits up to `timeout_ms` for one byte on stdin (negative waits forever).
    // The constructor disables ISIG, so Ctrl+C is delivered here as the byte
    // 0x03 rather than as SIGINT.
    ReadResult read_byte(unsigned char& byte, int timeout_ms = -1) const noexcept;

    // Completes the UTF-8 sequence introduced by `lead`, reading its continuation
    // bytes with `timeout_ms`. Returns `lead` alone when it is a single-byte or
    // invalid lead, or when the rest of the sequence does not arrive in time.
    std::string read_utf8_char(unsigned char lead, int timeout_ms = 50) const noexcept;

private:
    struct TerminalSize {
        int width;
        int height;
    };
    struct TermImpl;
    std::optional<TerminalSize> get_terminal_size() const noexcept;
    std::unique_ptr<TermImpl> originalTermState;
};