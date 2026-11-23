#include "Element.hpp"
#include "TerminalUI.hpp"
#include <algorithm>
#include <cmath>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

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
    // Sanitised on the way in so draw() can use the sides directly.
    border_ = Border{border.type,
                     std::max(0, border.top),
                     std::max(0, border.bottom),
                     std::max(0, border.left),
                     std::max(0, border.right)};
}

void Element::set_main_color(std::string_view color) noexcept {
    main_color_ = color;
}

void Element::set_style(Style style) noexcept {
    style_ = style;
}

const Style& Element::get_style() const noexcept {
    return style_;
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

void Element::draw(Canvas& area) {
    if (!visible_) { return; }

    // The box is the element minus its margins. The margin is not drawn: what
    // lies under it stays visible.
    const int box_width = std::max(0, width_ - margin_.left - margin_.right);
    const int box_height = std::max(0, height_ - margin_.top - margin_.bottom);
    Canvas box = area.sub(margin_.left, margin_.top, box_width, box_height);

    box.fill(0, 0, box_width, box_height, unicode::Glyph{}, style_);

    // Every border row spans the full box width (all sides share one character,
    // so the corners come out right); the side columns fill the rows between.
    const auto edge = [](char c) { return unicode::Glyph::of(static_cast<unsigned char>(c)); };
    for (int i = 0; i < border_.top; ++i) {
        box.fill(0, i, box_width, 1, edge(Border::border_top(border_.type)), style_);
    }
    for (int i = 0; i < border_.bottom; ++i) {
        box.fill(0, box_height - 1 - i, box_width, 1, edge(Border::border_bottom(border_.type)), style_);
    }
    const int side_top = border_.top;
    const int side_height = box_height - border_.top - border_.bottom;
    box.fill(0, side_top, border_.left, side_height, edge(Border::border_left(border_.type)), style_);
    box.fill(box_width - border_.right, side_top, border_.right, side_height,
             edge(Border::border_right(border_.type)), style_);

    Canvas inner = box.sub(border_.left + padding_.left, border_.top + padding_.top,
                           get_inner_width(), get_inner_height());
    draw_content(inner);
}

void LegacyElement::draw_content(Canvas& inner) {
    begin_render();
    std::string row_text;
    for (int row = 0; row < get_inner_height(); ++row) {
        row_text.clear();
        inner_content(row, row_text);
        inner.write_legacy(0, row, row_text);
    }
    std::vector<RenderCommand> commands;
    end_render(commands);
    for (const RenderCommand& command : commands) {
        if (command.type == RenderCommand::Type::CURSOR) {
            inner.set_cursor(command.anchor.x, command.anchor.y);
        } else {
            inner.write_legacy(command.anchor.x, command.anchor.y, command.data);
        }
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

void AbsoluteLayout::draw_content(Canvas& inner) {
    // Painter's order: lower z first. stable_sort keeps the insertion order for
    // equal z, so later siblings still cover earlier ones.
    std::vector<Element*> order;
    order.reserve(elements_.size());
    for (const auto& element : elements_) { order.push_back(element.get()); }
    std::stable_sort(order.begin(), order.end(), [](const Element* a, const Element* b) {
        return a->get_position().z < b->get_position().z;
    });

    for (Element* element : order) {
        const Position p = element->get_position();
        Canvas area = inner.sub(p.x, p.y, element->get_width(), element->get_height());
        element->draw(area);
    }
}

std::optional<Renderable::Cursor> AbsoluteLayout::write_frame(FrameBuffer& frame) {
    frame.resize(get_width(), get_height());
    std::optional<Canvas::Point> cursor;
    Canvas screen(frame, cursor);
    const Position p = get_position();
    Canvas area = screen.sub(p.x, p.y, get_width(), get_height());
    draw(area);

    if (!cursor) { return std::nullopt; }
    // Canvas coordinates are 0-based; the terminal's are 1-based.
    return Renderable::Cursor{cursor->x + 1, cursor->y + 1, Renderable::Cursor::State::VISIBLE};
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
            std::max(0, scale_scalar(position.y, y_ratio)),
            position.z
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
