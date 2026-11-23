#include "Chat.hpp"

#include "Element.hpp"
#include "Utils.hpp"
#include <algorithm>
#include <cstddef>
#include <format>
#include <type_traits>
#include <variant>

namespace {
    // Keeps the static_assert in the discarded branch below well-formed until it
    // is instantiated (a bare static_assert(false) is rejected at definition
    // time inside a template).
    template<class>
    inline constexpr bool always_false_v = false;
}

// --- ChatModel --------------------------------------------------------------------

void ChatModel::add_message(std::chrono::time_point<std::chrono::system_clock> recived_time, std::string sender, std::string message) {
    msg_.emplace_back(recived_time, std::move(sender), std::move(message));
    mutate_model();
}

void ChatModel::clear() noexcept {
    msg_.clear();
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

// --- ChatWindow -------------------------------------------------------------------

ChatWindow::ChatWindow(Element::Position relative_position, int width, int height)
    : Element(relative_position, width, height) {}

void ChatWindow::set_model(std::weak_ptr<ChatModel> model) noexcept {
    model_ = std::move(model);
    last_model_state_ = 0;
}

void ChatWindow::set_separator_style(Style style) noexcept {
    separator_style_ = style;
    last_model_state_ = 0;
}

void ChatWindow::set_message_style(Style style) noexcept {
    message_style_ = style;
    last_model_state_ = 0;
}

StyledText ChatWindow::styled(const ChatModel::ChatMsg& message) const {
    StyledText text;
    text.add(std::format("{}@{:%Y-%m-%d[%H:%M]}{}", message.sender, message.recived_time, sep_), separator_style_);
    text.add(message.message, message_style_);
    return text;
}

void ChatWindow::rebuild(const ChatModel& model, int width, int height) {
    layouts_.clear();
    rows_.clear();
    truncated_ = false;
    const auto messages = model.get_messages();
    if (height <= 0 || messages.empty()) { return; }

    // Newest first, only as many messages as it takes to fill the window.
    const size_t wanted = static_cast<size_t>(height);
    size_t lines = 0;
    size_t first = messages.size();
    while (first > 0 && lines < wanted) {
        --first;
        layouts_.push_back(TextLayout::wrap(styled(messages[first]), width));
        lines += layouts_.back().lines().size();
    }
    std::reverse(layouts_.begin(), layouts_.end());

    // The last `height` lines of those messages, in order.
    size_t skip = lines > wanted ? lines - wanted : 0;
    truncated_ = skip > 0 || first > 0;
    for (size_t i = 0; i < layouts_.size(); ++i) {
        for (size_t line = 0; line < layouts_[i].lines().size(); ++line) {
            if (skip > 0) { --skip; continue; }
            rows_.push_back(Row{i, line});
        }
    }
}

void ChatWindow::draw_content(Canvas& inner) {
    const auto model = model_.lock();
    if (!model) { return; }

    const int width = get_inner_width();
    const int height = get_inner_height();
    if (model->get_state() != last_model_state_ || width != last_width_ || height != last_height_) {
        rebuild(*model, width, height);
        last_model_state_ = model->get_state();
        last_width_ = width;
        last_height_ = height;
    }

    const int top = height - static_cast<int>(rows_.size());   // bottom-aligned
    for (size_t i = 0; i < rows_.size(); ++i) {
        inner.text(0, top + static_cast<int>(i), layouts_[rows_[i].layout].line_glyphs(rows_[i].line));
    }

    // Something is hidden above: the top row starts with the marker, drawn in the
    // style of what it covers. Drawn as glyphs, so it can only ever replace
    // whole glyphs (a wide glyph it half covers is blanked by Canvas::put).
    if (truncated_ && !rows_.empty()) {
        const auto first_line = layouts_[rows_.front().layout].line_glyphs(rows_.front().line);
        const int columns = layouts_[rows_.front().layout].lines()[rows_.front().line].columns;
        if (columns > static_cast<int>(trunc_symbol_.size())) {
            inner.text(0, top, trunc_symbol_, first_line.front().style);
        }
    }
}

// --- InputBox ---------------------------------------------------------------------

InputBox::InputBox(Position relative_position, int width, int height)
    : Element(relative_position, width, height) {}

void InputBox::set_model(std::weak_ptr<InputModel> model) noexcept {
    model_ = std::move(model);
    last_model_state_ = 0;
}

void InputBox::draw_content(Canvas& inner) {
    const auto model = model_.lock();
    if (!model) { return; }

    const auto [content, cursor] = model->get_content();
    const int width = get_inner_width();
    const int height = get_inner_height();
    if (model->get_state() != last_model_state_ || width != last_width_ || get_style() != last_style_) {
        // Unbounded: the view picks which lines are shown, so the wrap must not
        // stop at the visible height.
        layout_ = TextLayout::wrap(StyledText(content, get_style()), width);
        last_model_state_ = model->get_state();
        last_width_ = width;
        last_style_ = get_style();
    }
    if (height <= 0) { return; }

    // Minimal scrolling: follow the cursor only when it leaves the view, and
    // never scroll past the last line, so the last page is shown in full.
    const TextLayout::Position at = layout_.locate(cursor);
    const size_t visible = static_cast<size_t>(height);
    if (at.line < first_visible_line_) {
        first_visible_line_ = at.line;
    } else if (at.line >= first_visible_line_ + visible) {
        first_visible_line_ = at.line + 1 - visible;
    }
    const size_t line_count = layout_.lines().size();
    first_visible_line_ = std::min(first_visible_line_, line_count > visible ? line_count - visible : 0);

    for (size_t row = 0; row < visible && first_visible_line_ + row < line_count; ++row) {
        inner.text(0, static_cast<int>(row), layout_.line_glyphs(first_visible_line_ + row));
    }
    inner.set_cursor(at.column, static_cast<int>(at.line - first_visible_line_));
}

// --- InputModel -------------------------------------------------------------------

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
