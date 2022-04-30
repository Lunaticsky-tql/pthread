cmake_minimum_required(VERSION 3.21)
project(pthread)

set(CMAKE_CXX_STANDARD 14)


add_executable(parallel_finding_key_elements parallel_all_queries/parallel_finding_key_elements.cpp)
add_executable(Max_successor serial/max_successor.cpp parallel_readdata.h)
add_executable(sequential serial/sequential.cpp)
add_executable(parallel_all_queries parallel_all_queries/parallel_all_queries.cpp)
add_executable(simplified_Adp serial/simplified_Adp.cpp)