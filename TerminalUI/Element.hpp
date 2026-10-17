#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <vector>

#include "TerminalUI.hpp"

struct Element {
    struct Position {
        int x{0};
        int y{0};
        int z{0};
    };
    struct RenderCommand {
        enum struct Type { TEXT, CURSOR };
        Position anchor;
        std::string data;
        Type type{Type::TEXT};
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
    std::string_view get_main_color() const noexcept;
    void render(std::vector<RenderCommand>& buffer) noexcept;;
    void set_border(Border border) noexcept;
    void set_main_color(std::string_view color) noexcept;
    void set_margin(Margin margin) noexcept;
    void set_padding(Padding padding) noexcept;
    virtual void set_position(Position relative_position) noexcept;
    virtual void set_size(int width, int height) noexcept;
    bool visible() const noexcept;
    virtual void begin_render() = 0;
    virtual void inner_content(int local_row, std::string& buffer) = 0;
    virtual void end_render(std::vector<RenderCommand>& buffer) = 0;
private:
    Position relative_position_;
    int width_;
    int height_;
    Border border_;
    Margin margin_;
    Padding padding_;
    std::string_view main_color_{};
    bool visible_;
};

struct AbsoluteLayout : public Element, Renderable {
    AbsoluteLayout(Element::Position relative_position, int width, int height);

    void add_element(std::shared_ptr<Element> element);
    void clear_elements() noexcept;

    // Element interface. Both inner_content and end_render operate in this
    // layout's content plane coordinates; Element::render translates the
    // result into the parent element's coordinate system.
    void begin_render() override;
    void inner_content(int local_row, std::string& buffer) override;
    void end_render(std::vector<RenderCommand>& buffer) override;

    // Renderable interface: flattens the whole tree into `frame`'s cell grid,
    // resizing it to get_width() x get_height() (which also blanks it, so the
    // frame is complete rather than a patch of the previous one). Overlaps are
    // resolved here by z order, then emission order; a cell holds its own glyph
    // (plus the SGR sequences that precede it) so the caller can diff two frames
    // cell by cell. Returns the cursor's 1-based cell when the tree asked for one.
    std::optional<Renderable::Cursor> write_frame(FrameBuffer& frame) override;

    void resize(int width, int height) override;

    // Element interface: resizes this layout and rescales its children.
    void set_size(int width, int height) noexcept override;
private:
    std::vector<std::shared_ptr<Element>> elements_;
};



