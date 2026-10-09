# pragma once

#include "Element.hpp"
#include <chrono>
#include <cstddef>
#include <limits>
#include <span>
#include <string_view>
#include <variant>

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

struct ChatWindow : public Element {
    ChatWindow(Position relative_position, int width, int height);
    virtual void inner_content(int local_row, std::string& buffer) override;
    virtual void begin_render() override;
    virtual void end_render(std::vector<RenderCommand>& buffer) override;
    void set_model(std::weak_ptr<ChatModel> model) noexcept;
    void set_separator_color(std::string_view color) noexcept;
private:
    size_t last_model_state_{0};
    int last_width_{0};
    int last_height_{0};
    std::string frame_buffer_;
    std::vector<std::string_view> line_buffer_;
    std::weak_ptr<ChatModel> model_;
    std::string_view sep_{"::> "};
    std::string_view sep_color_{};
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

struct InputBox : public Element {
    InputBox(Position relative_position, int width, int height);
    virtual void inner_content(int row, std::string& buffer) override;
    virtual void begin_render() override;
    virtual void end_render(std::vector<RenderCommand>& buffer) override;
    void set_model(std::weak_ptr<InputModel> model) noexcept;
private:
    int last_width_{0};
    int last_height_{0};
    std::string frame_buffer_;
    std::vector<std::string_view> line_buffer_;
    std::weak_ptr<InputModel> model_;
    size_t last_model_state_{0};
    size_t last_model_cursor_{0};
    // First wrapped line shown by the viewport; the cursor's line always lies in
    // [first_visible_line_, first_visible_line_ + get_inner_height()).
    size_t first_visible_line_{0};
    int cursor_draw_x_{0};
    // Row of the cursor inside the box (0-based), not an index into line_buffer_.
    int cursor_draw_y_{0};
    bool cursor_visible_{false};

    void move_cursor(size_t cursor_pos);
};