#include "StatusBar.hpp"

#include <tuple>
#include <utility>

StatusBar::StatusBar(Position relative_position, int width) : Element(relative_position, width, 1) {}

void StatusBar::set_text(StyledText text) noexcept {
    text_ = std::move(text);
}

void StatusBar::draw_content(Canvas& inner) {
    if (inner.width() <= 0 || inner.height() <= 0) { return; }
    // Laid out as one long line, then cut: the start of the text matters most.
    const TextLayout layout = TextLayout::wrap(text_, 1 << 20);
    std::ignore = inner.text(0, 0, layout.line_glyphs(0));
}
