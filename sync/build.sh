#!/bin/bash
  g++ -std=c++17 -I. -I.. -L../target/release tasks.cpp -o tasks -pthread -llog_map_ffi -ldl
