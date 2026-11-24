#pragma once

// The TUI's screen, as a Renderable:
//
//   row 0           status bar   active conversation, its state, the others' unread counts
//   rows 1..h-5     chat         the active conversation's messages
//   row h-4         notice       the last command result or event
//   rows h-3..h-1   input        a top border and two lines of input
//
// Rows are fixed, so after every resize the children are placed exactly
// rather than scaled.

#include "Chat.hpp"
#include "Element.hpp"
#include "StatusBar.hpp"
#include "TerminalUI.hpp"
#include "TextLayout.hpp"

#include <memory>
#include <optional>

class TuiScreen : public Renderable {
public:
    TuiScreen(std::shared_ptr<ChatModel> chat, std::shared_ptr<InputModel> input);

    void set_status(StyledText text) noexcept { status_->set_text(std::move(text)); }
    void set_notice(StyledText text) noexcept { notice_->set_text(std::move(text)); }

    std::optional<Cursor> write_frame(FrameBuffer& frame) override;
    void resize(int width, int height) override;

    static constexpr Style kBarStyle{Color{}, Color{}, Style::Reverse};
    static constexpr Style kNoticeStyle{Color::ansi(3), Color{}, 0};          // yellow
    static constexpr Style kSeparatorStyle{Color::ansi(6), Color{}, Style::Bold};   // cyan

private:
    void place(int width, int height);

    AbsoluteLayout root_;
    std::shared_ptr<StatusBar> status_;
    std::shared_ptr<ChatWindow> chat_;
    std::shared_ptr<StatusBar> notice_;
    std::shared_ptr<InputBox> input_;
};
