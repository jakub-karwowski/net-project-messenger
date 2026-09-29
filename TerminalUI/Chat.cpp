#include "Chat.hpp"

#include "Element.hpp"
#include "Utils.hpp"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <type_traits>
#include <variant>

namespace {
    // Keeps the static_assert in the discarded branch below well-formed until it
    // is instantiated (a bare static_assert(false) is rejected at definition
    // time inside a template).
    template<class>
    inline constexpr bool always_false_v = false;
}

void ChatModel::add_message(std::chrono::time_point<std::chrono::system_clock> recived_time, std::string sender, std::string message) {
    msg_.emplace_back(recived_time, std::move(sender), std::move(message));
    mutate_model();
}

void ChatModel::expose(size_t pos_s, size_t n_msg) noexcept {
    exposed_pos_s = pos_s;
    exposed_n_msg = n_msg;
    mutate_model();
}

std::span<const ChatModel::ChatMsg> ChatModel::get_messages() const noexcept {
    const size_t offset = std::min(exposed_pos_s, msg_.size());
    return std::span{msg_}.subspan(offset, std::min(msg_.size() - offset, exposed_n_msg));
}

size_t ChatModel::get_state() const noexcept {
    return model_state_;
}

void ChatModel::mutate_model() noexcept {
    if (++model_state_ == 0) { ++model_state_; }
}

ChatWindow::ChatWindow(Element::Position relative_position, int width, int height) 
    : Element(relative_position, width, height) {}


void ChatWindow::set_model(std::weak_ptr<ChatModel> model) noexcept {
    model_ = std::move(model);
    last_model_state_ = 0;
}

void ChatWindow::set_separator_color(std::string_view color) noexcept {
    sep_color_ = color;
    last_model_state_ = 0;
}

void ChatWindow::begin_render() {
    auto m_ptr = model_.lock();
    if (!m_ptr) { return; }

    const size_t current_state = m_ptr->get_state();
    const int current_width = get_inner_width();
    const int current_height = get_inner_height();

    if (current_state == last_model_state_ &&
        current_width == last_width_ &&
        current_height == last_height_) 
    { 
        return; 
    }

    last_model_state_ = current_state;
    last_width_ = current_width;
    last_height_ = current_height;

    line_buffer_.clear();
    frame_buffer_.clear(); 

    auto messages = m_ptr->get_messages();
    if (messages.empty()) { return; }

    const size_t max_n_char_needed = current_height * current_width;
    size_t n_char_to_buffer = 0;
    auto itr = messages.end();
    do {
        --itr;
        n_char_to_buffer += itr->message.size();
    } while (
        itr != messages.begin() 
        && std::distance(itr, messages.end()) <= current_height
        && n_char_to_buffer < max_n_char_needed
    );
    

    for (const auto& chat_msg : std::span{itr, messages.end()}) {
        std::format_to(
            std::back_inserter(frame_buffer_), 
            "{}{}{:%Y%m%d%H%M}{}{}{}\n", 
            sep_color_, chat_msg.sender, chat_msg.recived_time, sep_, get_main_color(), chat_msg.message
        );
    }

    const auto [n_lines, is_consumed] = utils::line_split_tail(
        line_buffer_, frame_buffer_, current_width, current_height
    );

    if (!is_consumed && !line_buffer_.empty()) {
        auto first_line = line_buffer_.front();
        if (first_line.size() > trunc_symbol_.size()) {
            size_t offset = first_line.data() - frame_buffer_.data();
            frame_buffer_.replace(offset, trunc_symbol_.size(), trunc_symbol_);
        }
    }
}

void ChatWindow::inner_content(const int row, std::string& buffer) {
    if (row >= get_inner_height()) { return; }
    int empty_rows = get_inner_height() - line_buffer_.size();
    if (row >= empty_rows) {
        int line_idx = row - empty_rows;
        buffer.append(line_buffer_[line_idx]);
    }
}

void ChatWindow::end_render(std::vector<RenderCommand>& buffer) {}

InputBox::InputBox(Position relative_position, int width, int height)
    : Element(relative_position, width, height) {}

void InputBox::begin_render() {
    auto m_ptr = model_.lock();
    if (!m_ptr) { return; }

    const size_t current_state = m_ptr->get_state();
    const int current_width = get_inner_width();
    const int current_height = get_inner_height();

    if (current_state == last_model_state_ &&
        current_width == last_width_ &&
        current_height == last_height_) {
        if (const auto current_cursor = m_ptr->get_cursor(); current_cursor != last_model_cursor_) {
            move_cursor(current_cursor);
            last_model_cursor_ = current_cursor;
        }
        return;
    }

    last_model_state_ = current_state;
    last_width_ = current_width;
    last_height_ = current_height;

    line_buffer_.clear();
    frame_buffer_.clear();

    const auto [content_view, cursor_pos] = m_ptr->get_content();
    
    frame_buffer_ = content_view;

    // Unbounded: the viewport window picks which lines are displayed, so the
    // wrap must not stop at the visible height.
    utils::line_split(line_buffer_, frame_buffer_, current_width);

    cursor_visible_ = false;
    cursor_draw_x_ = 0;
    cursor_draw_y_ = 0;

    if (frame_buffer_.empty()) {
        cursor_visible_ = true;
        first_visible_line_ = 0;
        last_model_cursor_ = cursor_pos;
        return;
    }

    move_cursor(cursor_pos);
    // Keep the cache in sync with what was just drawn: a later cursor move back
    // to this exact offset would otherwise be skipped as a no-op and leave the
    // cursor (and the window it drives) showing the previous position.
    last_model_cursor_ = cursor_pos;
}

void InputBox::move_cursor(size_t cursor_pos) {
    const size_t cursor_target = utils::utf8::align(frame_buffer_, std::min(cursor_pos, frame_buffer_.size()));
    for (int row = 0; row < static_cast<int>(line_buffer_.size()); ++row) {
        const auto& line = line_buffer_[row];
        size_t line_start_idx = std::distance<const char*>(frame_buffer_.data(), line.data());
        size_t next_line_start = (row + 1 < static_cast<int>(line_buffer_.size())) 
            ? std::distance<const char*>(frame_buffer_.data(), line_buffer_[row + 1].data()) 
            : std::string::npos; 
        if (cursor_target < next_line_start) {
            // Minimal scrolling: the window keeps its place while the cursor
            // stays inside it and follows only when the cursor leaves it. It
            // never scrolls past the final line, so the last page is shown in
            // full instead of leaving blank rows below it.
            const size_t height = static_cast<size_t>(std::max(0, get_inner_height()));
            const size_t cursor_row = static_cast<size_t>(row);
            if (cursor_row < first_visible_line_) {
                first_visible_line_ = cursor_row;
            } else if (cursor_row >= first_visible_line_ + height) {
                first_visible_line_ = cursor_row + 1 - height;
            }
            const size_t last_start =
                line_buffer_.size() > height ? line_buffer_.size() - height : 0;
            first_visible_line_ = std::min(first_visible_line_, last_start);

            cursor_draw_y_ = row - static_cast<int>(std::min(first_visible_line_, cursor_row));
            const size_t logical_x = std::min(cursor_target - line_start_idx, line.size());
            cursor_draw_x_ = utils::utf8::width(line.substr(0, logical_x));
            cursor_visible_ = true;
            break;
        }
    }
}

void InputBox::inner_content(int row, std::string& buffer) {
    // Element::render only asks for rows inside the box, so the viewport is a
    // plain offset into the wrapped lines; the bounds check also covers a stale
    // first_visible_line_ (no row matched in move_cursor).
    const size_t line = first_visible_line_ + static_cast<size_t>(row);
    if (line < line_buffer_.size()) {
        buffer.append(line_buffer_[line]);
    }
}

void InputBox::end_render(std::vector<RenderCommand> &buffer) {
    // The row is box-local, so a cursor outside the rendered rows (possible when
    // the element is no taller than its borders) must not be anchored.
    if (!cursor_visible_ || cursor_draw_y_ < 0 || cursor_draw_y_ >= get_inner_height()) {
        return;
    }
    buffer.push_back(
        RenderCommand{
            cursor_draw_x_, 
            cursor_draw_y_, 
            {}, 
            RenderCommand::Type::CURSOR
        }
    );
}

void InputModel::apply(std::span<const Edit> edits) {
    bool content_changed = false;
    for (const auto& edit : edits) {
        std::visit([&](const auto& operation) {
            using Operation = std::decay_t<decltype(operation)>;
            if constexpr (std::is_same_v<Operation, EditPut>) {
                const size_t position =
                    utils::utf8::align(content, std::min(operation.position, content.size()));
                if (!operation.content.empty()) {
                    content.insert(position, operation.content);
                    content_changed = true;
                }
            } else if constexpr (std::is_same_v<Operation, EditRemove>) {
                const size_t begin =
                    utils::utf8::align(content, std::min(operation.position, content.size()));
                const size_t limit = begin + std::min(operation.n, content.size() - begin);
                const size_t end = utils::utf8::align(content, limit);
                if (end > begin) {
                    content.erase(begin, end - begin);
                    content_changed = true;
                }
            } else if constexpr (std::is_same_v<Operation, SetCursor>) {
                cursor = utils::utf8::align(content, std::min(operation.position, content.size()));
            } else {
                static_assert(always_false_v<Operation>, "unhandled InputModel::Edit alternative");
            }
        }, edit);
    }
    if (content_changed) { mutate_content(); }
}

std::tuple<std::string_view, size_t> InputModel::get_content() const noexcept {
    return {content, cursor};
}

size_t InputModel::get_cursor() const noexcept {
    return cursor;
}

size_t InputModel::get_state() const noexcept {
    return content_state_;
}

void InputModel::mutate_content() noexcept {
    if (++content_state_ == 0) { ++content_state_; }
}

void InputBox::set_model(std::weak_ptr<InputModel> model) noexcept {
    model_ = std::move(model);
    last_model_state_ = 0;
}