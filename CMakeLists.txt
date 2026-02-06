cmake_minimum_required (VERSION 3.14)
project (abt-io VERSION 0.8.1 LANGUAGES C CXX)

include (GNUInstallDirs)
include (CMakePackageConfigHelpers)
include (CheckCSourceCompiles)

# Options
option (ENABLE_BEDROCK "Enable Bedrock module support" OFF)
option (ENABLE_LIBURING "Enable liburing support" OFF)
option (ENABLE_COVERAGE "Enable code coverage" OFF)
option (ENABLE_BENCHMARK "Build abt-io-benchmark (requires zlib)" ON)
option (ENABLE_EXAMPLES "Build examples" ON)
option (ENABLE_TESTS "Build and register tests" ON)

# Coverage flags
if (ENABLE_COVERAGE)
    add_compile_options (--coverage -O0)
    add_link_options (--coverage)
endif ()

# Dependencies via pkg-config
find_package (PkgConfig REQUIRED)
pkg_check_modules (ARGOBOTS REQUIRED IMPORTED_TARGET argobots)
pkg_check_modules (JSONC REQUIRED IMPORTED_TARGET json-c)

# Strip /json-c from json-c include path if present, since we use #include <json-c/json.h>
get_target_property (_jsonc_include_dirs PkgConfig::JSONC INTERFACE_INCLUDE_DIRECTORIES)
if (_jsonc_include_dirs)
    set (_fixed_dirs)
    foreach (_dir ${_jsonc_include_dirs})
        string (REGEX REPLACE "/json-c$" "" _dir "${_dir}")
        list (APPEND _fixed_dirs "${_dir}")
    endforeach ()
    set_target_properties (PkgConfig::JSONC PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${_fixed_dirs}")
endif ()

# zlib (required for benchmark only)
if (ENABLE_BENCHMARK)
    find_package (ZLIB REQUIRED)
endif ()

# Threads (for examples)
find_package (Threads REQUIRED)

# Optional: liburing
if (ENABLE_LIBURING)
    pkg_check_modules (LIBURING REQUIRED IMPORTED_TARGET liburing)
    set (USE_LIBURING 1)
    set (PC_REQUIRES "argobots json-c liburing")
else ()
    set (PC_REQUIRES "argobots json-c")
endif ()

# Optional: bedrock
if (ENABLE_BEDROCK)
    pkg_check_modules (BEDROCK REQUIRED IMPORTED_TARGET bedrock-module-api)
    set (ABTIO_USE_BEDROCK 1)
endif ()

# Feature detection
set (CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)

check_c_source_compiles ("
#define _GNU_SOURCE
#include <fcntl.h>
int main() { int ret = fallocate(0, 0, 0, 0); return 0; }
" HAVE_FALLOCATE)

check_c_source_compiles ("
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
int main() { int fd = open(NULL, O_DIRECT); return 0; }
" HAVE_ODIRECT)

check_c_source_compiles ("
#define _GNU_SOURCE
#include <stdlib.h>
int main() { int fd = mkostemp(NULL, 0); return 0; }
" HAVE_MKOSTEMP)

unset (CMAKE_REQUIRED_DEFINITIONS)

# Generate config header
configure_file (
    ${CMAKE_CURRENT_SOURCE_DIR}/src/abt-io-config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/abt-io-config.h
)

# Subdirectories
add_subdirectory (src)

if (ENABLE_EXAMPLES)
    add_subdirectory (examples)
endif ()

if (ENABLE_TESTS)
    enable_testing ()
    add_subdirectory (tests)
endif ()

# Install header
install (FILES include/abt-io.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# pkg-config file
configure_file (
    ${CMAKE_CURRENT_SOURCE_DIR}/maint/abt-io.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/abt-io.pc
    @ONLY
)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/abt-io.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

# CMake package config
configure_package_config_file (
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/abt-io-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/abt-io-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/abt-io
)
write_basic_package_version_file (
    ${CMAKE_CURRENT_BINARY_DIR}/abt-io-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)
install (EXPORT abt-io-targets
    FILE abt-io-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/abt-io
)
install (FILES
    ${CMAKE_CURRENT_BINARY_DIR}/abt-io-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/abt-io-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/abt-io
)
