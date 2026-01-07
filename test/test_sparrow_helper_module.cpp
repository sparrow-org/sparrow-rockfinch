/**
 * @file test_sparrow_helper_module.cpp
 * @brief Test utilities for sparrow-pycapsule Python integration tests.
 *
 * This module provides helper functions for creating test arrays in C++.
 * The main SparrowArray class is defined in the sparrow module.
 */

#include <cstdint>
#include <vector>

#include <nanobind/nanobind.h>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

#include <sparrow-pycapsule/sparrow_array_python_class.hpp>

namespace nb = nanobind;

namespace
{
    /**
     * @brief Create a test sparrow array with sample nullable int32 data.
     */
    sparrow::pycapsule::SparrowArray create_test_array()
    {
        std::vector<sparrow::nullable<int32_t>> values = {
            sparrow::make_nullable<int32_t>(10, true),
            sparrow::make_nullable<int32_t>(20, true),
            sparrow::make_nullable<int32_t>(0, false),  // null
            sparrow::make_nullable<int32_t>(40, true),
            sparrow::make_nullable<int32_t>(50, true)
        };

        sparrow::primitive_array<int32_t> prim_array(std::move(values));
        return sparrow::pycapsule::SparrowArray(sparrow::array(std::move(prim_array)));
    }
}

NB_MODULE(TEST_SPARROW_HELPER_MODULE_NAME, m)
{
    m.doc() = "Test utilities for sparrow-pycapsule integration tests.";

    m.def("create_test_array", &create_test_array,
        "Create a test int32 array with values [10, 20, null, 40, 50].\n\n"
        "Returns\n"
        "-------\n"
        "sparrow.SparrowArray\n"
        "    A SparrowArray for testing purposes.");
}
