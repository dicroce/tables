cmake_minimum_required(VERSION 2.8)
project(tables)
include(ExternalProject)

ExternalProject_add(
    lmdb_library
    URL file://$(CMAKE_CURRENT_SOURCE_DIR)/deps/lmdb
    SOURCE_DIR $(CMAKE_CURRENT_SOURCE_DIR)/deps/lmdb
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND cd libraries/liblmdb && make
    INSTALL_COMMAND ""
)

add_library(lmdb_static STATIC IMPORTED)
set_target_properties(lmdb_static PROPERTIES IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/deps/lmdb/libraries/liblmdb/liblmdb.a)

add_library(lmdb SHARED IMPORTED)
set_target_properties(lmdb PROPERTIES IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/deps/lmdb/libraries/liblmdb/liblmdb.so)

# global build stuff...

add_definitions(-std=c++11 -O0 -g)

include_directories(
    include
    deps/lmdb/libraries
    ut/include
)

# static library...

add_library(
    tables_static STATIC
    include/tables/database.h
    source/database.cpp
)

target_link_libraries(tables_static lmdb_static)

add_dependencies(tables_static lmdb_library)

# dynamic library...

add_library(
    tables SHARED
    include/tables/database.h
    source/database.cpp
)

target_link_libraries(tables lmdb)

add_dependencies(tables lmdb_library)

# unit test...

add_executable(ut ut/include/framework.h
                  ut/include/database_test.h
                  ut/source/framework.cpp
                  ut/source/database_test.cpp)

target_link_libraries(ut tables_static uuid pthread)

# installation (first lmdb)...

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/deps/lmdb/libraries/liblmdb/liblmdb.a
        DESTINATION "lib")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/deps/lmdb/libraries/liblmdb/liblmdb.so
        DESTINATION "lib")
install(FILES deps/lmdb/libraries/liblmdb/lmdb.h
              deps/lmdb/libraries/liblmdb/midl.h
        DESTINATION include/lmdb)

#              (now tables)...

install(TARGETS tables tables_static
        LIBRARY DESTINATION "lib"
        ARCHIVE DESTINATION "lib"
        RUNTIME DESTINATION "lib"
        COMPONENT library)

install(DIRECTORY include/${PROJECT_NAME} DESTINATION include USE_SOURCE_PERMISSIONS)
