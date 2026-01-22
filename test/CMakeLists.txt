enable_testing()

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting tests build type to Release")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
else()
    message(STATUS "Tests build type is ${CMAKE_BUILD_TYPE}")
endif()

set(SPARROW_ROCKFINCH_TESTS_SOURCES
    main.cpp
    test_pycapsule.cpp
    test_sparrow_stream.cpp
)

set(test_target test_sparrow_rockfinch_lib)

add_executable(${test_target} ${SPARROW_ROCKFINCH_TESTS_SOURCES})

target_link_libraries(${test_target}
    PRIVATE
    sparrow-rockfinch-cpp
    sparrow::sparrow
    doctest::doctest
)

if(MSVC)
    target_compile_options(${test_target} PRIVATE /W4)
else()
    target_compile_options(${test_target} PRIVATE -Wall -Wextra -Wpedantic)
endif()

target_compile_features(${test_target} PRIVATE cxx_std_20)

if(ENABLE_COVERAGE)
    enable_coverage(${test_target})
endif()

# Add a custom target to run tests
add_custom_target(run_rockfinch_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS ${test_target}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running rockfinch tests"
)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Disable some warnings for test code
    target_compile_options(${test_target} PRIVATE
        -Wno-unused-variable
        -Wno-unused-parameter
    )
endif()

set_target_properties(${test_target} PROPERTIES
    FOLDER tests
)

add_custom_target(run_tests
    COMMAND ${test_target}
    DEPENDS
    ${test_target}
    COMMENT "Running tests"
    USES_TERMINAL
)

set_target_properties(run_tests PROPERTIES FOLDER "Tests utilities")

set(JUNIT_REPORT_FILE_DOCTEST ${CMAKE_CURRENT_BINARY_DIR}/test_sparrow-rockfinch_lib_report_doctest.xml)

add_custom_target(run_tests_with_junit_report
    COMMAND ${test_target} --reporters=junit --out=${JUNIT_REPORT_FILE_DOCTEST} --no-path-filenames=true
    DEPENDS
    ${test_target}
    COMMENT "Running tests with JUnit reports saved to: ${JUNIT_REPORT_FILE_DOCTEST}"
    USES_TERMINAL
)

set_target_properties(run_tests_with_junit_report PROPERTIES FOLDER "Tests utilities")

nanobind_add_module(test_sparrow_helper test_sparrow_helper_module.cpp)

target_link_libraries(test_sparrow_helper
    PRIVATE
        sparrow-rockfinch-cpp
        sparrow::sparrow
)

target_compile_features(test_sparrow_helper PRIVATE cxx_std_20)

# Define the module name macro to match the output filename (includes debug suffix)
target_compile_definitions(test_sparrow_helper PRIVATE
    TEST_SPARROW_HELPER_MODULE_NAME=$<TARGET_FILE_BASE_NAME:test_sparrow_helper>
)
set_target_properties(test_sparrow_helper PROPERTIES
    FOLDER tests
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}
    # Set RPATH so the module can find libsparrow-rockfinch at runtime
    BUILD_RPATH "$<TARGET_FILE_DIR:sparrow-rockfinch-cpp>"
    BUILD_RPATH_USE_ORIGIN ON
    MACOSX_RPATH ON
    INSTALL_RPATH "@loader_path"
)

# Python integration test
# =======================
find_package(Python COMPONENTS Interpreter QUIET)

if(Python_Interpreter_FOUND)
    # Add a test that runs the Python integration script
    add_test(
        NAME test_sparrow_integration
        COMMAND ${Python_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_sparrow_integration.py
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    
    # Set environment variables so Python can find the libraries
    set_tests_properties(test_sparrow_integration PROPERTIES
        ENVIRONMENT "SPARROW_MODULE_PATH=$<TARGET_FILE:sparrow_rockfinch>;TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
        TIMEOUT 300
    )
    
    message(STATUS "Added sparrow integration test (Python ${Python_VERSION})")
else()
    message(WARNING "Python interpreter not found, skipping sparrow integration test")
endif()

if(Python_Interpreter_FOUND)
    # Get the directory containing the sparrow-rockfinch library for runtime
    set(SPARROW_LIB_DIR "$<TARGET_FILE_DIR:sparrow-rockfinch-cpp>")
    
    add_custom_target(run_sparrow_tests_direct
        COMMAND ${CMAKE_COMMAND} -E echo "=================================="
        COMMAND ${CMAKE_COMMAND} -E echo "Sparrow Integration Test Runner"
        COMMAND ${CMAKE_COMMAND} -E echo "=================================="
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "Checking Python dependencies..."
        COMMAND ${Python_EXECUTABLE} -c "import polars" || ${CMAKE_COMMAND} -E cmake_echo_color --red "ERROR: polars not installed. Install with: pip install polars"
        COMMAND ${Python_EXECUTABLE} -c "import pyarrow" || ${CMAKE_COMMAND} -E cmake_echo_color --red "ERROR: pyarrow not installed. Install with: pip install pyarrow"
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "Library paths:"
        COMMAND ${CMAKE_COMMAND} -E echo "  SPARROW_MODULE_PATH=$<TARGET_FILE:sparrow_rockfinch>"
        COMMAND ${CMAKE_COMMAND} -E echo "  TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
        COMMAND ${CMAKE_COMMAND} -E echo "  SPARROW_LIB_DIR=${SPARROW_LIB_DIR}"
        COMMAND ${CMAKE_COMMAND} -E echo "  SPARROW_ROCKFINCH_LIB=$<TARGET_FILE:sparrow-rockfinch-cpp>"
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "Checking library exists..."
        COMMAND ls -la ${SPARROW_LIB_DIR}
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "Running tests..."
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E env 
            "SPARROW_MODULE_PATH=$<TARGET_FILE:sparrow_rockfinch>"
            "TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
            "LD_LIBRARY_PATH=${SPARROW_LIB_DIR}:$ENV{LD_LIBRARY_PATH}"
            "DYLD_LIBRARY_PATH=${SPARROW_LIB_DIR}:$ENV{DYLD_LIBRARY_PATH}"
            ${Python_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_sparrow_integration.py
        COMMAND ${CMAKE_COMMAND} -E echo ""
        DEPENDS test_sparrow_helper sparrow_rockfinch sparrow-rockfinch-cpp
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running Sparrow integration tests directly"
        USES_TERMINAL
    )
    
    set_target_properties(run_sparrow_tests_direct PROPERTIES FOLDER "Tests utilities")
endif()
