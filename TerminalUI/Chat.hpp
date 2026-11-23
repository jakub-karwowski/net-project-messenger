# pragma once

#include "Element.hpp"
#include "Style.hpp"
#include "TextLayout.hpp"

#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>
#include <vector>

struct ChatModel {
    struct ChatMsg {
        std::chrono::time_point<std::chrono::system_clock> recived_time;
        std::string sender;
        std::string message;
    };
    ~ChatModel() = default;
    void add_message(std::chrono::time_point<std::chrono::system_clock> recived_time, std::string sender, std::string message);
    // Discards every message (a fresh snapshot replaces the whole history).
    void clear() noexcept;
    void expose(size_t pos_s, size_t n_msg) noexcept;
    std::span<const ChatMsg> get_messages() const noexcept;
    size_t get_state() const noexcept;
private:
    std::vector<ChatMsg> msg_;
    size_t model_state_{1};
    size_t exposed_pos_s{0};
    size_t exposed_n_msg{std::numeric_limits<size_t>::max()};
    void mutate_model() noexcept;
};

// The newest messages of a ChatModel, bottom-aligned: the latest message ends on
// the last row. Each message is "sender@YYYY-MM-DD[HH:MM]::> " in the separator
// style followed by its text in the message style, wrapped to the inner width.
// When anything is hidden above the top row, the top row starts with "...".
// Message text is shown as text: control characters appear as U+FFFD.
struct ChatWindow : public Element {
    ChatWindow(Position relative_position, int width, int height);
    void set_model(std::weak_ptr<ChatModel> model) noexcept;
    void set_separator_style(Style style) noexcept;
    void set_message_style(Style style) noexcept;

protected:
    void draw_content(Canvas& inner) override;

private:
    struct Row {
        size_t layout;   // index into layouts_
        size_t line;     // line within it
    };

    // Lays out just the messages that reach the window, newest first, until the
    // window is full; rows_ are the last `height` of their lines.
    void rebuild(const ChatModel& model, int width, int height);
    StyledText styled(const ChatModel::ChatMsg& message) const;

    std::weak_ptr<ChatModel> model_;
    Style separator_style_{};
    Style message_style_{};
    // What rows_ was built for; a style change resets last_model_state_.
    size_t last_model_state_{0};
    int last_width_{-1};
    int last_height_{-1};
    std::vector<TextLayout> layouts_;   // oldest first
    std::vector<Row> rows_;             // top to bottom, at most `height`
    bool truncated_{false};             // something is hidden above rows_
    std::string_view sep_{"::> "};
    std::string_view trunc_symbol_{"..."};
};

struct InputModel {
    struct EditRemove {
        size_t position;
        size_t n;
    };
    struct EditPut {
        size_t position;
        std::string_view content;
    };
    struct SetCursor {
        size_t position;
    };
    // A single ordered batch of edits, applied atomically: positions refer to
    // the content as already mutated by the preceding edits in the same batch.
    // `position`/`n` are byte offsets; they are snapped to codepoint boundaries.
    using Edit = std::variant<EditRemove, EditPut, SetCursor>;

    void apply(std::span<const Edit> edits);
    std::tuple<std::string_view, size_t> get_content() const noexcept;
    size_t get_cursor() const noexcept;
    size_t get_state() const noexcept;
private:
    std::string content;
    size_t content_state_{1};
    size_t cursor{0};
    void mutate_content() noexcept;
};

namespace Input {
    inline InputModel::Edit remove(size_t position, size_t n) {
        return InputModel::EditRemove{position, n};
    }
    inline InputModel::Edit put(size_t position, std::string_view content) {
        return InputModel::EditPut{position, content};
    }
    inline InputModel::Edit set_cursor(size_t position) {
        return InputModel::SetCursor{position};
    }
}

// An editable text area for an InputModel: the content wrapped to the inner
// width, in the element's style, with the terminal cursor at the model's
// cursor. The view scrolls minimally: it moves only when the cursor leaves it,
// and never past the last line.
struct InputBox : public Element {
    InputBox(Position relative_position, int width, int height);
    void set_model(std::weak_ptr<InputModel> model) noexcept;

protected:
    void draw_content(Canvas& inner) override;

private:
    std::weak_ptr<InputModel> model_;
    size_t last_model_state_{0};
    int last_width_{-1};
    Style last_style_{};
    TextLayout layout_;
    // First line shown; the cursor's line always lies in
    // [first_visible_line_, first_visible_line_ + inner height).
    size_t first_visible_line_{0};
};
