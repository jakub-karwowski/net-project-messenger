#pragma once

// One row of styled text, cut at the element's width (never wrapped). The
// element's own style fills the rest of the row, so a bar can have a
// background of its own.

#include "Element.hpp"
#include "TextLayout.hpp"

struct StatusBar : public Element {
    StatusBar(Position relative_position, int width);
    void set_text(StyledText text) noexcept;

protected:
    void draw_content(Canvas& inner) override;

private:
    StyledText text_;
};
