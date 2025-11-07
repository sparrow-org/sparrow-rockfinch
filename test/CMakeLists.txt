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