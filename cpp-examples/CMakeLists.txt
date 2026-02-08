cmake_minimum_required(VERSION 3.15)
project(log_map_cpp_example)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(stdthreads REQUIRED)

add_executable(example example.cpp)

target_include_directories(example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(example
    ${CMAKE_CURRENT_SOURCE_DIR}/../target/debug/liblog_map_ffi.a
    stdc++
    dl
    pthread
    ${CMAKE_DL_LIBS}
)
