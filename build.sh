#!/usr/bin/env bash
set -e

BUILD_DIR="${BUILD_DIR:-build}"
TARGET="$1"

if [ -z "$TARGET" ]; then
    echo "Usage: $0 <target_name>"
    echo ""
    if [ -f "$BUILD_DIR/Makefile" ]; then
        echo "Available CMake targets:"
        make -C "$BUILD_DIR" help | grep "^\.\.\. " | sed 's/^\.\.\. //' | grep -v "\.o$" | head -n 20
    fi
    exit 1
fi

# Ensure build directory exists and is configured
if [ ! -d "$BUILD_DIR" ]; then
    echo "==> Configuring CMake in $BUILD_DIR..."
    cmake -B "$BUILD_DIR" -S .
fi

# Automatically keep clangd's compilation database in sync at project root
if [ -f "$BUILD_DIR/compile_commands.json" ]; then
    ln -sf "$BUILD_DIR/compile_commands.json" compile_commands.json
fi

echo "==> Building target: $TARGET"
cmake --build "$BUILD_DIR" --target "$TARGET" -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)"

echo "==> Build complete: $BUILD_DIR/$TARGET"