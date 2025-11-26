enable_testing()

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting tests build type to Release")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
else()
    message(STATUS "Tests build type is ${CMAKE_BUILD_TYPE}")
endif()

set(SPARROW_PYCAPSULE_TESTS_SOURCES
    main.cpp
    test_pycapsule.cpp
)

set(test_target test_sparrow_pycapsule_lib)

add_executable(${test_target} ${SPARROW_PYCAPSULE_TESTS_SOURCES})

target_link_libraries(${test_target}
    PRIVATE
    sparrow-pycapsule
    sparrow::sparrow
    doctest::doctest
    Python::Python
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

add_test(NAME ${test_target} COMMAND ${test_target})

# For better output formatting
set_tests_properties(${test_target} PROPERTIES
    TIMEOUT 300
)

# Add a custom target to run tests
add_custom_target(run_pycapsule_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS ${test_target}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running pycapsule tests"
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

set(JUNIT_REPORT_FILE_DOCTEST ${CMAKE_CURRENT_BINARY_DIR}/test_sparrow-pycapsule_lib_report_doctest.xml)

add_custom_target(run_tests_with_junit_report
    COMMAND ${test_target} --reporters=junit --out=${JUNIT_REPORT_FILE_DOCTEST} --no-path-filenames=true
    DEPENDS
    ${test_target}
    COMMENT "Running tests with JUnit reports saved to: ${JUNIT_REPORT_FILE_DOCTEST}"
    USES_TERMINAL
)

set_target_properties(run_tests_with_junit_report PROPERTIES FOLDER "Tests utilities")

Python_add_library(test_sparrow_helper MODULE test_sparrow_helper_module.cpp)

target_link_libraries(test_sparrow_helper
    PRIVATE
        sparrow-pycapsule
        sparrow::sparrow
)

target_compile_features(test_sparrow_helper PRIVATE cxx_std_20)

if(MSVC)
    target_compile_options(test_sparrow_helper PRIVATE /W4)
else()
    target_compile_options(test_sparrow_helper PRIVATE -Wall -Wextra -Wpedantic)
endif()

set_target_properties(test_sparrow_helper PROPERTIES
    FOLDER tests
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}
    # Python modules must not have a debug suffix - Python won't find them
    DEBUG_POSTFIX ""
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
    # Use generator expressions to get the actual library paths from targets
    set_tests_properties(test_sparrow_integration PROPERTIES
        ENVIRONMENT "TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>;SPARROW_PYCAPSULE_LIB_PATH=$<TARGET_FILE:sparrow-pycapsule>"
        TIMEOUT 300
        DEPENDS test_sparrow_helper
    )
    
    message(STATUS "Added sparrow integration test (Python ${Python_VERSION})")
else()
    message(WARNING "Python interpreter not found, skipping sparrow integration test")
endif()

if(Python_Interpreter_FOUND)
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
        COMMAND ${CMAKE_COMMAND} -E echo "  TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
        COMMAND ${CMAKE_COMMAND} -E echo "  SPARROW_PYCAPSULE_LIB_PATH=$<TARGET_FILE:sparrow-pycapsule>"
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "Running tests..."
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E env 
            "TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
            "SPARROW_PYCAPSULE_LIB_PATH=$<TARGET_FILE:sparrow-pycapsule>"
            ${Python_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_sparrow_integration.py
        COMMAND ${CMAKE_COMMAND} -E echo ""
        DEPENDS test_sparrow_helper sparrow-pycapsule
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running Sparrow integration tests directly"
        USES_TERMINAL
    )
    
    set_target_properties(run_sparrow_tests_direct PROPERTIES FOLDER "Tests utilities")
    
    # Custom target to check Sparrow dependencies
    add_custom_target(check_sparrow_deps
        COMMAND ${CMAKE_COMMAND} -E echo "Checking Sparrow integration test dependencies..."
        COMMAND ${Python_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/check_deps.py
        COMMAND ${CMAKE_COMMAND} -E echo "Environment variables that will be set:"
        COMMAND ${CMAKE_COMMAND} -E echo "  TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
        COMMAND ${CMAKE_COMMAND} -E echo "  SPARROW_PYCAPSULE_LIB_PATH=$<TARGET_FILE:sparrow-pycapsule>"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Checking Sparrow test dependencies"
        USES_TERMINAL
    )
    
    set_target_properties(check_sparrow_deps PROPERTIES FOLDER "Tests utilities")
    
    # Minimal library loading test for debugging segfaults
    add_custom_target(test_library_load
        COMMAND ${CMAKE_COMMAND} -E env 
            "TEST_SPARROW_HELPER_LIB_PATH=$<TARGET_FILE:test_sparrow_helper>"
            "SPARROW_PYCAPSULE_LIB_PATH=$<TARGET_FILE:sparrow-pycapsule>"
            "PYTHONUNBUFFERED=1"
            ${Python_EXECUTABLE} -u ${CMAKE_CURRENT_SOURCE_DIR}/test_library_load.py
        DEPENDS 
            test_sparrow_helper
            sparrow-pycapsule
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Testing library loading step-by-step"
        USES_TERMINAL
    )
    
    set_target_properties(test_library_load PROPERTIES FOLDER "Tests utilities")
endif()
