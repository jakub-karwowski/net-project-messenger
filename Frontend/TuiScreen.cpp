#include "TuiScreen.hpp"

#include <algorithm>

namespace {

constexpr int kInputRows = 3;   // a border row and two lines of text

}  // namespace

TuiScreen::TuiScreen(std::shared_ptr<ChatModel> chat, std::shared_ptr<InputModel> input)
    : root_(Element::Position{0, 0}, 80, 24),
      status_(std::make_shared<StatusBar>(Element::Position{0, 0}, 80)),
      chat_(std::make_shared<ChatWindow>(Element::Position{0, 1}, 80, 19)),
      notice_(std::make_shared<StatusBar>(Element::Position{0, 20}, 80)),
      input_(std::make_shared<InputBox>(Element::Position{0, 21}, 80, kInputRows)) {
    status_->set_style(kBarStyle);
    chat_->set_model(chat);
    chat_->set_separator_style(kSeparatorStyle);
    input_->set_model(input);
    input_->set_border(Element::Border(Element::Border::BorderType::Hash, 1, 0, 0, 0));
    root_.add_element(status_);
    root_.add_element(chat_);
    root_.add_element(notice_);
    root_.add_element(input_);
    place(80, 24);
}

void TuiScreen::resize(int width, int height) {
    root_.resize(width, height);   // scales the children; place() then puts them exactly
    place(width, height);
}

void TuiScreen::place(int width, int height) {
    width = std::max(width, 1);
    const int input_top = std::max(height - kInputRows, 2);
    const int notice_row = std::max(input_top - 1, 1);
    status_->set_position({0, 0});
    status_->set_size(width, 1);
    chat_->set_position({0, 1});
    chat_->set_size(width, std::max(notice_row - 1, 0));
    notice_->set_position({0, notice_row});
    notice_->set_size(width, 1);
    input_->set_position({0, input_top});
    input_->set_size(width, kInputRows);
}

std::optional<Renderable::Cursor> TuiScreen::write_frame(FrameBuffer& frame) {
    return root_.write_frame(frame);
}
