add_executable (abt-io-test-basic basic.c)
target_link_libraries (abt-io-test-basic abt-io)
target_include_directories (abt-io-test-basic PRIVATE ${PROJECT_BINARY_DIR})
target_compile_definitions (abt-io-test-basic PRIVATE _GNU_SOURCE)
set_target_properties (abt-io-test-basic PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/tests
    OUTPUT_NAME basic)

add_executable (abt-io-test-init-ext abt-io-test-init-ext.c)
target_link_libraries (abt-io-test-init-ext abt-io)
target_include_directories (abt-io-test-init-ext PRIVATE ${PROJECT_BINARY_DIR})
target_compile_definitions (abt-io-test-init-ext PRIVATE _GNU_SOURCE)
set_target_properties (abt-io-test-init-ext PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/tests
    OUTPUT_NAME abt-io-test-init-ext)

add_test (NAME concurrent-write-bench
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/concurrent-write-bench.sh
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
set_tests_properties (concurrent-write-bench PROPERTIES
    ENVIRONMENT "srcdir=${PROJECT_SOURCE_DIR}")

add_test (NAME basic
    COMMAND tests/basic ${CMAKE_CURRENT_SOURCE_DIR}/abt-io-benchmark-example.json
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

add_test (NAME abt-io-test-init-ext
    COMMAND tests/abt-io-test-init-ext
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

if (ENABLE_BENCHMARK)
    add_test (NAME abt-io-benchmark
        COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/abt-io-benchmark.sh
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
    set_tests_properties (abt-io-benchmark PROPERTIES
        ENVIRONMENT "srcdir=${PROJECT_SOURCE_DIR}")
endif ()

if (ENABLE_LIBURING)
    add_test (NAME basic-uring
        COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/basic-uring.sh
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
    set_tests_properties (basic-uring PROPERTIES
        ENVIRONMENT "srcdir=${PROJECT_SOURCE_DIR}")

    if (ENABLE_BENCHMARK)
        add_test (NAME abt-io-benchmark-uring
            COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/abt-io-benchmark-uring.sh
            WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
        set_tests_properties (abt-io-benchmark-uring PROPERTIES
            ENVIRONMENT "srcdir=${PROJECT_SOURCE_DIR}")
    endif ()
endif ()
