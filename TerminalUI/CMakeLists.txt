add_library(TerminalUI TerminalUI.cpp Element.cpp Chat.cpp Style.cpp)

target_include_directories(TerminalUI PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(TerminalUI PUBLIC cxx_std_23)