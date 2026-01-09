cmake_minimum_required(VERSION 3.28)

# This is better specified per target, but cmake keeps ignoring these language version
# specification when building this project by itself, in particular the gnu extensions,
# so here we go.
# This will affects all following targets being defined.
set(CMAKE_CXX_EXTENSIONS OFF)
cmake_policy(SET CMP0091 NEW)

# MSVC debug information format flags are selected via
# CMAKE_MSVC_DEBUG_INFORMATION_FORMAT, instead of
# embedding flags in e.g. CMAKE_CXX_FLAGS_RELEASE.
# New in CMake 3.25.
#
# Supports debug info with SCCache
# (https://github.com/mozilla/sccache?tab=readme-ov-file#usage)
# avoiding “fatal error C1041: cannot open program database; if
# multiple CL.EXE write to the same .PDB file, please use /FS"
set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT Embedded)
cmake_policy(SET CMP0141 NEW)

project(sparrow-rockfinch CXX)

if(NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 20)
endif()

message(STATUS "🔧 C++ standard: ${CMAKE_CXX_STANDARD}")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF) # We don't use modules
include(CMakeDependentOption)

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
message(DEBUG "CMake module path: ${CMAKE_MODULE_PATH}")

include(external_dependencies)

set(SPARROW_ROCKFINCH_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SPARROW_ROCKFINCH_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(CMAKE_CXX_COMPILER_LAUNCHER)
    message(STATUS "Using C++ compiler launcher: ${CMAKE_CXX_COMPILER_LAUNCHER}")
endif()

# Versioning
# ===========

set(sparrow_rockfinch_version_path "${SPARROW_ROCKFINCH_INCLUDE_DIR}/sparrow-rockfinch/config/sparrow_rockfinch_version.hpp")
file(STRINGS "${sparrow_rockfinch_version_path}" sparrow_rockfinch_version_defines
    REGEX "constexpr int SPARROW_ROCKFINCH_VERSION_(MAJOR|MINOR|PATCH)")

foreach(ver ${sparrow_rockfinch_version_defines})
    if(ver MATCHES "constexpr int SPARROW_ROCKFINCH_VERSION_(MAJOR|MINOR|PATCH) = ([0-9]+);$")
        set(PROJECT_VERSION_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "")
    endif()
endforeach()

set(CMAKE_PROJECT_VERSION
    ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

message(STATUS "Building sparrow-rockfinch v${CMAKE_PROJECT_VERSION}")

# Binary version
# See the following URL for explanations about the binary versioning
# https://www.gnu.org/software/libtool/manual/html_node/Updating-version-info.html#Updating-version-info
file(STRINGS "${sparrow_rockfinch_version_path}" sparrow_rockfinch_version_defines
    REGEX "constexpr int SPARROW_ROCKFINCH_BINARY_(CURRENT|REVISION|AGE)")

foreach(ver ${sparrow_rockfinch_version_defines})
    if(ver MATCHES "constexpr int SPARROW_ROCKFINCH_BINARY_(CURRENT|REVISION|AGE) = ([0-9]+);$")
        set(SPARROW_ROCKFINCH_BINARY_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "")
    endif()
endforeach()

set(SPARROW_ROCKFINCH_BINARY_VERSION
    ${SPARROW_ROCKFINCH_BINARY_CURRENT}.${SPARROW_ROCKFINCH_BINARY_REVISION}.${SPARROW_ROCKFINCH_BINARY_AGE})

message(STATUS "sparrow-rockfinch binary version: v${SPARROW_ROCKFINCH_BINARY_VERSION}")

# Build options
# =============

option(SPARROW_ROCKFINCH_BUILD_TESTS "Build sparrow-rockfinch test suite" OFF)
message(STATUS "🔧 Build tests: ${SPARROW_ROCKFINCH_BUILD_TESTS}")
option(BUILD_DOCS "Build sparrow-rockfinch documentation" OFF)
message(STATUS "🔧 Build docs: ${BUILD_DOCS}")

set(CMAKE_DEBUG_POSTFIX "d")

option(SPARROW_CONTRACTS_THROW_ON_FAILURE "Throw exceptions instead of aborting on contract failures" OFF)
message(STATUS "🔧 Contracts throw on failure: ${SPARROW_CONTRACTS_THROW_ON_FAILURE}")

option(ENABLE_COVERAGE "Enable test coverage" OFF)
message(STATUS "🔧 Enable coverage: ${ENABLE_COVERAGE}")

if(ENABLE_COVERAGE)
    include(code_coverage)
endif()

include(CheckCXXSymbolExists)

if(cxx_std_20 IN_LIST CMAKE_CXX_COMPILE_FEATURES)
    set(header version)
else()
    set(header ciso646)
endif()

check_cxx_symbol_exists(_LIBCPP_VERSION ${header} LIBCPP)

if(LIBCPP)
    message(STATUS "Using libc++")

    # Allow the use of not visible yet available features, such
    # as some formatter for new types.
    add_compile_definitions(_LIBCPP_DISABLE_AVAILABILITY)
endif()

# Linter options
# =============
option(ACTIVATE_LINTER "Create targets to run clang-format" OFF)
message(STATUS "🔧 Activate linter: ${ACTIVATE_LINTER}")
cmake_dependent_option(ACTIVATE_LINTER_DURING_COMPILATION "Run linter during the compilation" ON "ACTIVATE_LINTER" OFF)

if(ACTIVATE_LINTER)
    include(clang-format)
    include(clang-tidy)
endif()

# Sanitizers
# ==========
# include(sanitizers) // TODO: add sanitizers support

# Dependencies
# ============

# include(external_dependencies) // TODO: add external dependencies support

set(SPARROW_ROCKFINCH_COMPILE_DEFINITIONS "" CACHE STRING "List of public compile definitions of the sparrow-rockfinch target")

# Build
# =====
set(BINARY_BUILD_DIR "${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG "${BINARY_BUILD_DIR}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE "${BINARY_BUILD_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG "${BINARY_BUILD_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE "${BINARY_BUILD_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${BINARY_BUILD_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${BINARY_BUILD_DIR}")

set(SPARROW_ROCKFINCH_HEADERS
    ${SPARROW_ROCKFINCH_INCLUDE_DIR}/sparrow-rockfinch/config/sparrow_rockfinch_version.hpp
    ${SPARROW_ROCKFINCH_INCLUDE_DIR}/sparrow-rockfinch/pycapsule.hpp
    ${SPARROW_ROCKFINCH_INCLUDE_DIR}/sparrow-rockfinch/sparrow_array_python_class.hpp
)

set(SPARROW_ROCKFINCH_SOURCES
    src/pycapsule.cpp
    src/sparrow_array_python_class.cpp
)

option(SPARROW_ROCKFINCH_BUILD_SHARED "Build sparrow-rockfinch as a shared library" ON)

if(SPARROW_ROCKFINCH_BUILD_SHARED)
    message(STATUS "🔧 Build shared library")
    set(SPARROW_ROCKFINCH_LIBRARY_TYPE SHARED)
else()
    message(STATUS "🔧 Build static library")
    set(SPARROW_ROCKFINCH_LIBRARY_TYPE STATIC)
    list(APPEND SPARROW_ROCKFINCH_COMPILE_DEFINITIONS SPARROW_ROCKFINCH_STATIC_LIB)
endif()

add_library(sparrow-rockfinch-cpp ${SPARROW_ROCKFINCH_LIBRARY_TYPE} ${SPARROW_ROCKFINCH_HEADERS} ${SPARROW_ROCKFINCH_SOURCES})

target_link_libraries(sparrow-rockfinch-cpp
    PUBLIC 
        sparrow::sparrow
        Python::Python)

set_target_properties(sparrow-rockfinch-cpp PROPERTIES
    POSITION_INDEPENDENT_CODE ON)

target_compile_definitions(sparrow-rockfinch-cpp PUBLIC ${SPARROW_ROCKFINCH_COMPILE_DEFINITIONS})

if(UNIX)
    # CMake does not compute the version number of so files as libtool
    # does on Linux. Strictly speaking, we should exclude FreeBSD and
    # Apple from this, but that would require having different version
    # numbers depending on the platform. We prefer to follow the
    # libtool pattern everywhere.
    math(EXPR SPARROW_ROCKFINCH_BINARY_COMPATIBLE "${SPARROW_ROCKFINCH_BINARY_CURRENT} - ${SPARROW_ROCKFINCH_BINARY_AGE}")
    set_target_properties(
        sparrow-rockfinch-cpp
        PROPERTIES
        VERSION "${SPARROW_ROCKFINCH_BINARY_COMPATIBLE}.${SPARROW_ROCKFINCH_BINARY_REVISION}.${SPARROW_ROCKFINCH_BINARY_AGE}"
        SOVERSION ${SPARROW_ROCKFINCH_BINARY_COMPATIBLE}
    )
    target_compile_options(sparrow-rockfinch-cpp PRIVATE "-fvisibility=hidden")
else()
    set_target_properties(
        sparrow-rockfinch-cpp
        PROPERTIES
        VERSION ${SPARROW_ROCKFINCH_BINARY_VERSION}
        SOVERSION ${SPARROW_ROCKFINCH_BINARY_CURRENT}
    )
    target_compile_definitions(sparrow-rockfinch-cpp PRIVATE SPARROW_ROCKFINCH_EXPORTS)
endif()

target_include_directories(sparrow-rockfinch-cpp PUBLIC
    $<BUILD_INTERFACE:${SPARROW_ROCKFINCH_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>)

# We do not use non-standard C++
set_target_properties(sparrow-rockfinch-cpp PROPERTIES CMAKE_CXX_EXTENSIONS OFF)
target_compile_features(sparrow-rockfinch-cpp PUBLIC cxx_std_20)

if(ENABLE_COVERAGE)
    enable_coverage(sparrow-rockfinch)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND WIN32)
    target_compile_options(sparrow-rockfinch
        PUBLIC
        -Wa,-mbig-obj)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (EMSCRIPTEN)
        target_compile_options(sparrow-rockfinch-cpp
        PRIVATE
        -fPIC)
    else ()
        target_compile_options(sparrow-rockfinch-cpp
        PUBLIC
        -Wno-c99-extensions)
    endif()
elseif(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    target_compile_options(sparrow-rockfinch-cpp
        PUBLIC
            /bigobj
    )
endif()

# Docs
# ====
if(BUILD_DOCS)
    add_subdirectory(docs)
endif()

# Python module
# =============
option(SPARROW_ROCKFINCH_BUILD_PYTHON_MODULE "Build the sparrow-rockfinch Python module" ON)
message(STATUS "🔧 Build Python module: ${SPARROW_ROCKFINCH_BUILD_PYTHON_MODULE}")

if(SPARROW_ROCKFINCH_BUILD_PYTHON_MODULE)
    nanobind_add_module(sparrow_rockfinch src/sparrow_module.cpp)
    target_link_libraries(sparrow_rockfinch PRIVATE sparrow-rockfinch-cpp sparrow::sparrow)
    target_compile_features(sparrow_rockfinch PRIVATE cxx_std_20)
    # Define the module name macro to match the output filename (includes debug suffix)
    target_compile_definitions(sparrow_rockfinch PRIVATE
        SPARROW_MODULE_NAME=sparrow_rockfinch
    )
    set_target_properties(sparrow_rockfinch PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${BINARY_BUILD_DIR}
        # Set RPATH so the module can find libsparrow-rockfinch at runtime
        BUILD_RPATH "$<TARGET_FILE_DIR:sparrow-rockfinch-cpp>"
        BUILD_RPATH_USE_ORIGIN ON
        MACOSX_RPATH ON
        INSTALL_RPATH "@loader_path"
    )

    install( 
        TARGETS sparrow_rockfinch
        EXPORT sparrow_rockfinchTargets
        LIBRARY DESTINATION ${Python_SITEARCH}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${BINDIR}
    )
endif()

# Tests
# =====
if(SPARROW_ROCKFINCH_BUILD_TESTS)
    add_subdirectory(test)
endif()

# Installation
# ============
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install target
set(SPARROW_ROCKFINCH_EXPORTED_TARGETS sparrow-rockfinch-cpp)

install(TARGETS ${SPARROW_ROCKFINCH_EXPORTED_TARGETS}
    EXPORT ${PROJECT_NAME}-targets)

# Makes the project importable from the build directory
export(EXPORT ${PROJECT_NAME}-targets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake"
    NAMESPACE sparrow::)

# Install headers
install(DIRECTORY ${SPARROW_ROCKFINCH_INCLUDE_DIR}/sparrow-rockfinch
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    PATTERN ".clang-*" EXCLUDE
    PATTERN "README.md" EXCLUDE)
# Install CMake configuration files
set(SPARROW_CMAKECONFIG_INSTALL_DIR "${CMAKE_INSTALL_DATADIR}/cmake/${PROJECT_NAME}" CACHE
    STRING "install path for sparrowConfig.cmake")

configure_package_config_file(${PROJECT_NAME}Config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION ${SPARROW_CMAKECONFIG_INSTALL_DIR})

write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
    VERSION ${CMAKE_PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion)

install(FILES 
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
    DESTINATION ${SPARROW_CMAKECONFIG_INSTALL_DIR})

install(EXPORT ${PROJECT_NAME}-targets
    FILE ${PROJECT_NAME}Targets.cmake
    NAMESPACE sparrow::
    DESTINATION ${SPARROW_CMAKECONFIG_INSTALL_DIR})

