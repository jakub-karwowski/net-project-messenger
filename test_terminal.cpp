#include <thread>
#include <vector>

#include "TerminalUI.hpp"

struct Dummy : public Renderable {
    std::vector<Renderable::DrawCommand> commands;
    DrawCommand get_command() override {
        auto obj = commands.back();
        commands.pop_back();
        return obj;
    }
    void resize(int, int) override {}
    bool empty() const noexcept override {
        return commands.empty();
    }
};

int main() {
    TerminalUI ui;
    Dummy dummy;
    dummy.commands.emplace_back(Renderable::DrawCommand::Action::DrawText, "Hello there!\nCollin\nJane");
    dummy.commands.emplace_back(Renderable::DrawCommand::Action::ClearScreen);
    ui.render(dummy);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    dummy.commands.emplace_back(
        Renderable::DrawCommand::Action::ClearLineAndDraw, 
        "John", 
        Renderable::Position{2, 1}, 
        Renderable::Position{}
    );
    ui.render(dummy);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    ui.clear();
}