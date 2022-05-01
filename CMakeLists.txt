cmake_minimum_required(VERSION 3.21)
project(pthread)

set(CMAKE_CXX_STANDARD 14)



add_executable(simplified_Adp serial/simplified_Adp.cpp)
add_executable(sequential serial/sequential.cpp)
add_executable(max_successor serial/max_successor.cpp)
add_executable(group_parallel parallel_all_queries/group_parallel.cpp)
add_executable(block_task parallel_all_queries/block_task.cpp)
add_executable(parallel_finding_key_elements parallel_each_query/parallel_intersection.cpp parallel_each_query/early_stop.cpp)
add_executable(early_stop parallel_each_query/early_stop.cpp)