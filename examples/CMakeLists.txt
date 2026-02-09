if (ENABLE_BENCHMARK)
    add_executable (abt-io-benchmark abt-io-benchmark.c)
    target_link_libraries (abt-io-benchmark abt-io Threads::Threads ZLIB::ZLIB PkgConfig::JSONC)
    target_include_directories (abt-io-benchmark PRIVATE
        ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR})
    target_compile_definitions (abt-io-benchmark PRIVATE _GNU_SOURCE)
    set_target_properties (abt-io-benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/examples)
endif ()

add_executable (abt-io-overlap abt-io-overlap.c)
target_link_libraries (abt-io-overlap abt-io PkgConfig::JSONC)
target_include_directories (abt-io-overlap PRIVATE
    ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR})
target_compile_definitions (abt-io-overlap PRIVATE _GNU_SOURCE)
set_target_properties (abt-io-overlap PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/examples)

add_executable (concurrent-write-bench concurrent-write-bench.c)
target_link_libraries (concurrent-write-bench abt-io Threads::Threads PkgConfig::JSONC)
target_include_directories (concurrent-write-bench PRIVATE
    ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR})
target_compile_definitions (concurrent-write-bench PRIVATE _GNU_SOURCE)
set_target_properties (concurrent-write-bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/examples)

add_executable (pthread-overlap pthread-overlap.c)
target_link_libraries (pthread-overlap Threads::Threads PkgConfig::JSONC)
target_include_directories (pthread-overlap PRIVATE
    ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR})
target_compile_definitions (pthread-overlap PRIVATE _GNU_SOURCE)
set_target_properties (pthread-overlap PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/examples)
