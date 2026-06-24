cmake_minimum_required(VERSION 3.16)
project(test_package CXX)

find_package(zerobus REQUIRED)

add_executable(example src/example.cpp)
target_link_libraries(example PRIVATE zerobus::zerobus)
