#include "Element.hpp"
#include "TerminalUI.hpp"
#include "Utils.hpp"
#include <algorithm>
#include <cmath>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace {

// TRANSITIONAL (design.md §13.3, removed in step 3): elements still hand over
// rows as strings that may carry colour codes. This turns such a string into
// cells: SGR sequences update a running Style (it lasts until the end of the
// string), every other escape sequence is dropped, and glyphs come from
// unicode::next_glyph(), which turns control characters into U+FFFD. So no byte
// of the string reaches the terminal as a control sequence.
//
// Writes `text` starting at the 0-based cell (row, column). '\n' moves to the
// next row and back to column 0; anything outside the frame is dropped.
void write_text(FrameBuffer& frame, int row, int column, std::string_view text) {
    Style style{};
    for (size_t i = 0; i < text.size();) {
        const char byte = text[i];
        if (byte == '\n') {
            ++row;
            column = 0;
            ++i;
            continue;
        }
        if (byte == '\r') {
            column = 0;
            ++i;
            continue;
        }
        if (byte == '\033' && i + 1 < text.size() && text[i + 1] == '[') {
            // CSI: parameters, then a final byte in 0x40..0x7E.
            const size_t parameters = i + 2;
            size_t end = parameters;
            while (end < text.size()
                   && !(static_cast<unsigned char>(text[end]) >= 0x40
                        && static_cast<unsigned char>(text[end]) <= 0x7E)) { ++end; }
            if (end < text.size() && text[end] == 'm') {
                apply_sgr(style, text.substr(parameters, end - parameters));
            }
            i = std::min(end + 1, text.size());
            continue;
        }

        const unicode::GlyphRead read = unicode::next_glyph(text, i);
        i += read.length;
        frame.put(row, column, read.glyph, style);
        column += read.glyph.width();
    }
}

} // namespace

Element::Element(Element::Position relative_position, int width, int height) 
    : relative_position_(relative_position)
    , width_(width)
    , height_(height)
    , border_({})
    , visible_(true) {}

Element::Position Element::get_position() const noexcept {
    return relative_position_;
}

int Element::get_height() const noexcept {
    return height_;
}

int Element::get_width() const noexcept {
    return width_;
}

void Element::set_position(Position relative_position) noexcept {
    relative_position_ = relative_position;
}

void Element::set_size(int width, int height) noexcept {
    width_ = width;
    height_ = height;
}

bool Element::visible() const noexcept {
    return visible_;
}

void Element::set_border(Border border) noexcept {
    // Sanitised on the way in so render() can use the sides directly: a negative
    // thickness would become a huge size_t in string::append/reserve.
    border_ = Border{border.type,
                     std::max(0, border.top),
                     std::max(0, border.bottom),
                     std::max(0, border.left),
                     std::max(0, border.right)};
}

void Element::set_main_color(std::string_view color) noexcept {
    main_color_ = color;
}

void Element::set_margin(Margin margin) noexcept {
    margin_ = Margin{
        std::max(0, margin.top),
        std::max(0, margin.bottom),
        std::max(0, margin.left),
        std::max(0, margin.right)
    };
}

void Element::set_padding(Padding padding) noexcept {
    padding_ = Padding{
        std::max(0, padding.top),
        std::max(0, padding.bottom),
        std::max(0, padding.left),
        std::max(0, padding.right)
    };
}

int Element::get_inner_height() const noexcept {
    return height_ - (margin_.top + margin_.bottom)
                   - (padding_.top + padding_.bottom)
                   - (border_.top + border_.bottom);
}

int Element::get_inner_width() const noexcept {
    return width_ - (margin_.left + margin_.right)
                   - (padding_.left + padding_.right)
                   - (border_.left + border_.right);
}

std::string_view Element::get_main_color() const noexcept {
    return main_color_;
}

Element::Border::Border()
    : Border(BorderType::None, 0, 0, 0, 0) {}

Element::Border::Border(BorderType type)
    : Border(type, thickness(type), thickness(type), thickness(type), thickness(type)) {}

Element::Border::Border(BorderType type, int top, int bottom, int left, int right)
    : type(type)
    , top(top)
    , bottom(bottom)
    , left(left)
    , right(right) {}

void Element::render(std::vector<RenderCommand>& buffer) noexcept {
    if (!visible_) { return; }
    const size_t initial_buffer_size = buffer.size();
    // Sides are already non-negative (set_border sanitises), so no clamping here.
    const int border_left_width = border_.left;
    const int border_right_width = border_.right;
    const int border_top_width = border_.top;
    const int border_bottom_width = border_.bottom;

    const int inner_offset_x = margin_.left + padding_.left + border_left_width;
    const int inner_offset_y = margin_.top + padding_.top + border_top_width;

    // The box is the element minus its margins; every border row spans the full
    // box width (all sides share one character, so the corners come out right).
    const int box_left = margin_.left;
    const int box_width = std::max(0, width_ - margin_.left - margin_.right);
    const int box_top = margin_.top;
    const int box_bottom = height_ - margin_.bottom;   // exclusive

    for (int i = 0; i < border_top_width; ++i) {
        buffer.emplace_back(
            Position{box_left, box_top + i},
            std::string(static_cast<size_t>(box_width), Border::border_top(border_.type))
        );
    }
    for (int i = 0; i < border_bottom_width; ++i) {
        buffer.emplace_back(
            Position{box_left, box_bottom - 1 - i},
            std::string(static_cast<size_t>(box_width), Border::border_bottom(border_.type))
        );
    }

    std::string inner_row_buffer;
    inner_row_buffer.reserve(static_cast<size_t>(std::max(0, get_inner_width())));
    this->begin_render();
    // Iterate every row between the top and bottom border, not just the content
    // rows: the vertical padding rows carry no content but still need their
    // left/right border characters drawn.
    const int content_top = inner_offset_y;
    const int content_bottom = height_ - (margin_.bottom + padding_.bottom + border_bottom_width);
    for (int row = box_top + border_top_width; row < box_bottom - border_bottom_width; ++row) {
        inner_row_buffer.clear();
        if (row >= content_top && row < content_bottom) {
            this->inner_content(row - content_top, inner_row_buffer);
        }
        const int content_columns = utils::utf8::width(inner_row_buffer);
        const int padding_columns = std::max(0, get_inner_width() - content_columns);

        std::string row_text;
        row_text.reserve(static_cast<size_t>(std::max(0, width_)));
        row_text.append(static_cast<size_t>(border_left_width), Border::border_left(border_.type));
        row_text.append(static_cast<size_t>(padding_.left), ' ');
        row_text.append(inner_row_buffer);
        row_text.append(static_cast<size_t>(padding_columns), ' ');
        row_text.append(static_cast<size_t>(padding_.right), ' ');
        row_text.append(static_cast<size_t>(border_right_width), Border::border_right(border_.type));

        buffer.emplace_back(Position{box_left, row}, std::move(row_text));
        inner_row_buffer.clear();
    }
    const size_t end_render_command_buffer_start = buffer.size();
    this->end_render(buffer);
    for (size_t i = end_render_command_buffer_start; i < buffer.size(); ++i) {
        auto& [x, y, z]  = buffer[i].anchor;
        (void)z;
        x += inner_offset_x;
        y += inner_offset_y;
    }

    for (size_t i = initial_buffer_size; i < buffer.size(); ++i) {
        auto& [x, y, z]  = buffer[i].anchor;
        (void)z;
        x += relative_position_.x;
        y += relative_position_.y;
    }
}

AbsoluteLayout::AbsoluteLayout(Element::Position relative_position, int width, int height)
    : Element(relative_position, width, height) {}

void AbsoluteLayout::add_element(std::shared_ptr<Element> element) {
    elements_.push_back(std::move(element));
}

void AbsoluteLayout::clear_elements() noexcept {
    elements_.clear();
}

// The layout has no content of its own; children are drawn in end_render. The
// base Element::render still emits a blank (space padded) row per inner row,
// which conveniently clears this layout's area before the children are drawn.
void AbsoluteLayout::begin_render() {}

void AbsoluteLayout::inner_content(int /*local_row*/, std::string& /*buffer*/) {}

// Children render in this layout's content plane coordinates; Element::render
// later shifts those commands by (relative_position + inner offset) so that the
// layout works both as a nested element and as the tree's root.
void AbsoluteLayout::end_render(std::vector<RenderCommand>& buffer) {
    for (const auto& element : elements_) {
        element->render(buffer);
    }
}

// Flattens the whole element tree into the frame grid. render() emits anchors
// relative to the screen's content plane (0-based) and already folds the
// element's margin/padding/border plus its relative position in, so the only
// conversion left is the +1 to terminal (ANSI) coordinates for the cursor.
std::optional<Renderable::Cursor> AbsoluteLayout::write_frame(FrameBuffer& frame) {
    std::vector<RenderCommand> commands;
    render(commands);

    // Element::render translates x/y but not z, and the tree is emitted depth
    // first. A stable sort gives the requested paint order while ties (the
    // layout's own blank rows before its children) keep their emission order.
    std::stable_sort(
        commands.begin(), commands.end(),
        [](const RenderCommand& lhs, const RenderCommand& rhs) {
            return lhs.anchor.z < rhs.anchor.z;
        }
    );

    frame.resize(get_width(), get_height());
    std::optional<Renderable::Cursor> cursor;
    for (const auto& command : commands) {
        if (command.type == RenderCommand::Type::CURSOR) {
            // A cursor is not drawn on its own; remember the (last) requested
            // cell so the caller can park the terminal cursor there once the
            // frame has been drawn. Clamped, since a stale cursor may sit
            // outside the layout after a shrink.
            cursor = Renderable::Cursor{
                std::clamp(command.anchor.x + 1, 1, std::max(1, frame.width)),
                std::clamp(command.anchor.y + 1, 1, std::max(1, frame.height)),
                Renderable::Cursor::State::VISIBLE
            };
            continue;
        }
        write_text(frame, command.anchor.y, command.anchor.x, command.data);
    }

    return cursor;
}


// Renderable entry point: forwards the new terminal size into the (virtual)
// Element::set_size override below.
void AbsoluteLayout::resize(int width, int height) {
    set_size(width, height);
}

// Overrides Element::set_size so resizing scales the whole subtree: children
// are scaled by the ratio between the requested size and the previous one.
// Nested AbsoluteLayouts recurse through this same virtual override, since the
// parent invokes set_size() on each child below.
void AbsoluteLayout::set_size(int width, int height) noexcept {
    const int previous_width = get_width();
    const int previous_height = get_height();
    if (previous_width == width && previous_height == height) { return; }

    Element::set_size(width, height);

    if (previous_width <= 0 || previous_height <= 0) { return; }
    const double col_ratio = static_cast<double>(width) / previous_width;
    const double row_ratio = static_cast<double>(height) / previous_height;

    const auto scale_scalar = [](int value, double ratio) {
        return static_cast<int>(std::lround(value * ratio));
    };
    const auto scale_size = [&](int value, double ratio) {
        return std::max(1, scale_scalar(value, ratio));
    };
    const auto scale_position = [&](Element::Position position, double x_ratio, double y_ratio) {
        return Element::Position{
            std::max(0, scale_scalar(position.x, x_ratio)),
            std::max(0, scale_scalar(position.y, y_ratio))
        };
    };

    for (const auto& element : elements_) {
        element->set_position(scale_position(element->get_position(), col_ratio, row_ratio));
        element->set_size(
            scale_size(element->get_width(), col_ratio),
            scale_size(element->get_height(), row_ratio)
        );
    }
}