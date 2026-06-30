/**
 * @file test_sparrow_helper_module.cpp
 * @brief Test utilities for sparrow-rockfinch Python integration tests.
 *
 * This module provides helper functions for creating test arrays in C++.
 * The main SparrowArray class is defined in the sparrow module.
 */

#include <cstdint>
#include <string>
#include <vector>

#include <nanobind/nanobind.h>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>
#include <sparrow/variable_size_binary_array.hpp>

#include <sparrow-rockfinch/sparrow_array_python_class.hpp>

namespace nb = nanobind;

namespace
{
    /**
     * @brief Create a test sparrow array with sample nullable int32 data.
     */
    sparrow::rockfinch::SparrowArray create_test_array()
    {
        std::vector<sparrow::nullable<int32_t>> values = {
            sparrow::make_nullable<int32_t>(10, true),
            sparrow::make_nullable<int32_t>(20, true),
            sparrow::make_nullable<int32_t>(0, false),  // null
            sparrow::make_nullable<int32_t>(40, true),
            sparrow::make_nullable<int32_t>(50, true)
        };

        sparrow::primitive_array<int32_t> prim_array(std::move(values));
        return sparrow::rockfinch::SparrowArray(sparrow::array(std::move(prim_array)));
    }

    sparrow::rockfinch::SparrowArray create_nullable_float_array()
    {
        std::vector<sparrow::nullable<float>> values = {
            sparrow::make_nullable<float>(1.5f, true),
            sparrow::make_nullable<float>(0.0f, false),
            sparrow::make_nullable<float>(3.5f, true)
        };

        sparrow::primitive_array<float> prim_array(std::move(values));
        return sparrow::rockfinch::SparrowArray(sparrow::array(std::move(prim_array)));
    }

    sparrow::rockfinch::SparrowArray create_nullable_bool_array()
    {
        std::vector<sparrow::nullable<bool>> values = {
            sparrow::make_nullable<bool>(true, true),
            sparrow::make_nullable<bool>(false, false),
            sparrow::make_nullable<bool>(false, true)
        };

        sparrow::primitive_array<bool> prim_array(std::move(values));
        return sparrow::rockfinch::SparrowArray(sparrow::array(std::move(prim_array)));
    }

    sparrow::rockfinch::SparrowArray create_string_array()
    {
        std::vector<std::string> values = {"alpha", "beta", "gamma"};
        sparrow::string_array string_array(std::move(values));
        return sparrow::rockfinch::SparrowArray(sparrow::array(std::move(string_array)));
    }

    // --- Non-nullable primitive helpers (for to_numpy / NumPy operation tests) ---

    template <typename T>
    sparrow::rockfinch::SparrowArray make_primitive_array(std::vector<T> values)
    {
        sparrow::primitive_array<T> prim_array(std::move(values));
        return sparrow::rockfinch::SparrowArray(sparrow::array(std::move(prim_array)));
    }

    sparrow::rockfinch::SparrowArray create_primitive_int32_array()
    {
        return make_primitive_array<int32_t>({1, 2, 3, 4, 5});
    }

    sparrow::rockfinch::SparrowArray create_primitive_int64_array()
    {
        return make_primitive_array<int64_t>({10, 20, 30, 40, 50});
    }

    sparrow::rockfinch::SparrowArray create_primitive_float32_array()
    {
        return make_primitive_array<float>({1.5f, 2.5f, 3.5f, 4.5f, 5.5f});
    }

    sparrow::rockfinch::SparrowArray create_primitive_float64_array()
    {
        return make_primitive_array<double>({1.0, 2.0, 3.0, 4.0, 5.0});
    }

    sparrow::rockfinch::SparrowArray create_primitive_uint32_array()
    {
        return make_primitive_array<uint32_t>({5, 6, 7, 8, 9});
    }
}

NB_MODULE(TEST_SPARROW_HELPER_MODULE_NAME, m)
{
    m.doc() = "Test utilities for sparrow-rockfinch integration tests.";

    m.def("create_test_array", &create_test_array,
        "Create a test int32 array with values [10, 20, null, 40, 50].\n\n"
        "Returns\n"
        "-------\n"
        "sparrow.SparrowArray\n"
        "    A SparrowArray for testing purposes.");

    m.def("create_nullable_float_array", &create_nullable_float_array,
        "Create a float32 SparrowArray with a null element.");

    m.def("create_nullable_bool_array", &create_nullable_bool_array,
        "Create a bool SparrowArray with a null element.");

    m.def("create_string_array", &create_string_array,
        "Create a non-primitive string SparrowArray.");

    m.def("create_primitive_int32_array", &create_primitive_int32_array,
        "Create a non-nullable int32 SparrowArray [1, 2, 3, 4, 5].");
    m.def("create_primitive_int64_array", &create_primitive_int64_array,
        "Create a non-nullable int64 SparrowArray [10, 20, 30, 40, 50].");
    m.def("create_primitive_float32_array", &create_primitive_float32_array,
        "Create a non-nullable float32 SparrowArray [1.5, 2.5, 3.5, 4.5, 5.5].");
    m.def("create_primitive_float64_array", &create_primitive_float64_array,
        "Create a non-nullable float64 SparrowArray [1.0, 2.0, 3.0, 4.0, 5.0].");
    m.def("create_primitive_uint32_array", &create_primitive_uint32_array,
        "Create a non-nullable uint32 SparrowArray [5, 6, 7, 8, 9].");
}
