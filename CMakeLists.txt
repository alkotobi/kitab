cmake_minimum_required(VERSION 3.16)

project(jamharah C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_library(jamharah STATIC
    src/index_format.c
    src/normalize_arabic.c
    src/tokenize_arabic.c
)

target_include_directories(jamharah
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

find_package(SQLite3 REQUIRED)

add_executable(build_from_sqlite
    src/build_from_sqlite.c
)

target_link_libraries(build_from_sqlite
    PRIVATE
        jamharah
        SQLite::SQLite3
)

enable_testing()

add_executable(test_index_format
    tests/test_index_format.c
)

target_link_libraries(test_index_format
    PRIVATE
        jamharah
)

add_test(NAME index_format_basic COMMAND test_index_format)

add_executable(test_books_layout
    tests/test_books_layout.c
)

target_link_libraries(test_books_layout
    PRIVATE
        jamharah
        SQLite::SQLite3
)

add_test(NAME books_layout COMMAND test_books_layout)
