add_library (abt-io abt-io.c)
target_include_directories (abt-io
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_BINARY_DIR}
)
target_link_libraries (abt-io
    PUBLIC PkgConfig::ARGOBOTS
    PRIVATE PkgConfig::JSONC
)
target_compile_definitions (abt-io PRIVATE _GNU_SOURCE)
set_target_properties (abt-io PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    OUTPUT_NAME abt-io
)

if (ENABLE_LIBURING)
    target_link_libraries (abt-io PRIVATE PkgConfig::LIBURING)
endif ()

install (TARGETS abt-io
    EXPORT abt-io-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Bedrock module
if (ENABLE_BEDROCK)
    add_library (abt-io-bedrock-module bedrock-module.cpp)
    target_link_libraries (abt-io-bedrock-module
        PUBLIC abt-io PkgConfig::BEDROCK
    )
    target_compile_features (abt-io-bedrock-module PRIVATE cxx_std_17)
    set_target_properties (abt-io-bedrock-module PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
        OUTPUT_NAME abt-io-bedrock-module
    )
    install (TARGETS abt-io-bedrock-module
        EXPORT abt-io-targets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif ()
