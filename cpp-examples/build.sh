#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FFI_LIB="$PROJECT_ROOT/target/release/liblog_map_ffi.a"

echo "Building C++ example..."
g++ -std=c++17 -O2 -o example \
    example.cpp \
    -I"$PROJECT_ROOT/include" \
    "$FFI_LIB" \
    -stdc++ \
    -lpthread \
    -ldl

echo "Build complete: ./example"
