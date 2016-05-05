cmake_minimum_required(VERSION 2.8)
project(ut)

set(SOURCES source/framework.cpp
            source/database_test.cpp)

set(COMMON_LIBS cppkit tables pthread)

include(common.cmake NO_POLICY_SCOPE)
include(../../devel_artifacts/build/base_ut.cmake)
