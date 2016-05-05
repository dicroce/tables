cmake_minimum_required(VERSION 2.8)
project(tables)
include(ExternalProject)

set(SOURCES include/tables/database.h
            source/database.cpp)

set(COMMON_LIBS cppkit lmdb)

include(common.cmake NO_POLICY_SCOPE)
include("${devel_artifacts_path}/build/base_lib.cmake" NO_POLICY_SCOPE)

ExternalProject_Add(
  lmdb
  URL ${CMAKE_CURRENT_SOURCE_DIR}/lmdb.tar.bz2
  SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/lmdb_sources
  CONFIGURE_COMMAND ""
  BUILD_COMMAND cd ${CMAKE_CURRENT_BINARY_DIR}/lmdb_sources && make
  INSTALL_COMMAND mkdir -p ${devel_artifacts_path}/include/lmdb && cd ${CMAKE_CURRENT_BINARY_DIR}/lmdb_sources && cp lmdb.h ${devel_artifacts_path}/include/lmdb && cp midl.h ${devel_artifacts_path}/include/lmdb && cp liblmdb.a ${devel_artifacts_path}/lib
)
