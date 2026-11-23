#pragma once

#include "Canvas.hpp"
#include "Style.hpp"
#include "TerminalUI.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

// A rectangle of the screen with an optional margin, border and padding around
// its content. Elements draw themselves into a Canvas (design.md §13.2): the
// parent hands each child a canvas of the child's size at the child's position,
// clipped to the parent, so no element can draw outside its own box.
struct Element {
    struct Position {
        int x{0};
        int y{0};
        int z{0};   // paint order among siblings: higher is drawn later, on top
    };
    struct Border {
        enum struct BorderType : uint32_t {
            None = 0,
            Hash = 1,
            Asterisk = 2,
        };
        BorderType type;
        int top;
        int bottom;
        int left;
        int right;

        // Not constexpr, so the definitions can live in Element.cpp. constexpr
        // would make them implicitly inline, forcing a header definition.
        Border();
        Border(BorderType type);
        // Per-side thickness; a side may be 0 to leave that side open.
        Border(BorderType type, int top, int bottom, int left, int right);

        // Thickness implied by the type: None leaves every side open.
        static constexpr int thickness(BorderType type) {
            return type == BorderType::None ? 0 : 1;
        }

        // constexpr => implicitly inline, so these stay defined in the header.
        static constexpr char border_top(BorderType type) {
            static constexpr char character[] = {' ', '#', '*'};
            return character[static_cast<uint16_t>(type)];
        }
        static constexpr char border_bottom(BorderType type) {
            static constexpr char character[] = {' ', '#', '*'};
            return character[static_cast<uint16_t>(type)];
        }
        static constexpr char border_left(BorderType type) {
            static constexpr char character[] = {' ', '#', '*'};
            return character[static_cast<uint16_t>(type)];
        }
        static constexpr char border_right(BorderType type) {
            static constexpr char character[] = {' ', '#', '*'};
            return character[static_cast<uint16_t>(type)];
        }
    };

    struct Margin {
        int top{0};
        int bottom{0};
        int left{0};
        int right{0};
    };
    struct Padding {
        int top{0};
        int bottom{0};
        int left{0};
        int right{0};
    };
    Element(Position relative_position, int width, int height);
    virtual ~Element() = default;
    int get_height() const noexcept;
    int get_inner_height() const noexcept;
    int get_inner_width() const noexcept;
    Position get_position() const noexcept;
    int get_width() const noexcept;
    const Style& get_style() const noexcept;
    void set_border(Border border) noexcept;
    void set_margin(Margin margin) noexcept;
    void set_padding(Padding padding) noexcept;
    // The style of the element's own cells: its background, border and padding.
    void set_style(Style style) noexcept;
    virtual void set_position(Position relative_position) noexcept;
    virtual void set_size(int width, int height) noexcept;
    bool visible() const noexcept;

    // TRANSITIONAL (until step 4): a colour as an SGR string, read by widgets
    // that still build their text with colour codes. Must outlive the element.
    std::string_view get_main_color() const noexcept;
    void set_main_color(std::string_view color) noexcept;

    // Draws the element into `area`, a canvas of the element's size at its
    // position. The margin is left untouched; the rest of the box is cleared in
    // the element's style, then the border is drawn, then draw_content() fills
    // the inner area (inside border and padding).
    void draw(Canvas& area);

protected:
    // The element's content. `inner` is the area inside border and padding,
    // get_inner_width() x get_inner_height(), already cleared.
    virtual void draw_content(Canvas& inner) = 0;

private:
    Position relative_position_;
    int width_;
    int height_;
    Border border_;
    Margin margin_;
    Padding padding_;
    Style style_{};
    std::string_view main_color_{};
    bool visible_;
};

// TRANSITIONAL (removed in step 4, design.md §13.3): the old interface for
// widgets that still produce each inner row as a string with colour codes.
// draw_content() calls begin_render(), then inner_content() once per inner row,
// then end_render(), and writes what they produced into the canvas.
struct LegacyElement : Element {
    struct RenderCommand {
        enum struct Type { TEXT, CURSOR };
        Position anchor;   // relative to the inner area
        std::string data;
        Type type{Type::TEXT};
    };

    using Element::Element;

    virtual void begin_render() = 0;
    virtual void inner_content(int local_row, std::string& buffer) = 0;
    virtual void end_render(std::vector<RenderCommand>& buffer) = 0;

protected:
    void draw_content(Canvas& inner) override;
};

// Places children at their positions inside its inner area and draws them in
// z order (equal z: in the order they were added). As the root it is also the
// Renderable that TerminalUI draws.
struct AbsoluteLayout : public Element, Renderable {
    AbsoluteLayout(Element::Position relative_position, int width, int height);

    void add_element(std::shared_ptr<Element> element);
    void clear_elements() noexcept;

    // Renderable interface: sizes `frame` to get_width() x get_height() (which
    // also blanks it) and draws the whole tree into it, offset by this layout's
    // position. Returns the 1-based cell of the cursor if an element asked for one.
    std::optional<Renderable::Cursor> write_frame(FrameBuffer& frame) override;

    void resize(int width, int height) override;

    // Element interface: resizes this layout and rescales its children.
    void set_size(int width, int height) noexcept override;

protected:
    void draw_content(Canvas& inner) override;

private:
    std::vector<std::shared_ptr<Element>> elements_;
};
