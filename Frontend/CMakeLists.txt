add_library(Frontend
  Commands.cpp CommandProcessor.cpp
  CliFormat.cpp JsonlWriter.cpp LineReader.cpp CliFrontend.cpp
  Keys.cpp KeyboardWorker.cpp TerminalState.cpp TuiScreen.cpp TuiFrontend.cpp)
target_include_directories(Frontend PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Frontend PUBLIC Messages Common TerminalUI)
target_compile_features(Frontend PUBLIC cxx_std_23)
