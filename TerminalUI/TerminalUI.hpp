#pragma once

#include <memory>
#include <optional>
#include <string>

struct Renderable {
    struct TerminalSize {
        int row;
        int col;
    };
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
    virtual DrawCommand get_command(TerminalSize win) = 0;
    virtual bool empty() const noexcept = 0;
};

class TerminalUI {
public:
    TerminalUI();
    TerminalUI(const TerminalUI&) = delete;
    TerminalUI& operator=(const TerminalUI&) = delete;
    TerminalUI(TerminalUI&& other) = delete;
    TerminalUI& operator=(TerminalUI&& other) = delete;
    ~TerminalUI();

    bool render(Renderable& renderable);
    void clear() const noexcept;
private:
    struct TermImpl;
    std::optional<Renderable::TerminalSize> get_terminal_size() const noexcept;
    std::unique_ptr<TermImpl> originalTermState;
};