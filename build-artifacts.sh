#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
ARTIFACTS_DIR="$PROJECT_ROOT/artifacts"

echo "Building Rust workspace..."
cargo build --release

echo "Building C++ sample client..."
cd "$PROJECT_ROOT/sync"
g++ -std=c++17 -I. -I.. -L../target/release tasks.cpp -o tasks -pthread -llog_map_ffi -ldl

echo "Collecting artifacts..."
rm -rf "$ARTIFACTS_DIR"
mkdir -p "$ARTIFACTS_DIR"

cp "$PROJECT_ROOT/target/release/log-server" "$ARTIFACTS_DIR/server"
cp "$PROJECT_ROOT/target/release/matrix-mul" "$ARTIFACTS_DIR/"
cp "$PROJECT_ROOT/target/release/liblog_map_ffi.so" "$ARTIFACTS_DIR/liblogmap.so"
cp "$PROJECT_ROOT/target/release/liblog_map_ffi.a" "$ARTIFACTS_DIR/liblogmap.a"
cp "$PROJECT_ROOT/include/log_map.hpp" "$ARTIFACTS_DIR/"
cp "$PROJECT_ROOT/sync/tasks" "$ARTIFACTS_DIR/"

echo "Artifacts collected in $ARTIFACTS_DIR/:"
ls -la "$ARTIFACTS_DIR"
