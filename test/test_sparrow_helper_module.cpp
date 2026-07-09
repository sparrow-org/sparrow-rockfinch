/**
 * @file test_sparrow_helper_module.cpp
 * @brief Test utilities for sparrow-rockfinch Python integration tests.
 *
 * This module provides helper functions for creating test arrays in C++.
 * The main SparrowArray class is defined in the sparrow module.
 */

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include <nanobind/nanobind.h>

#include <sparrow/array.hpp>
#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/types/data_type.hpp>
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

    // --- ndarray view / copy helpers (exercise zero-copy buffer path) ---

    template <typename T>
    const char* numpy_dtype_spec()
    {
        if constexpr (std::same_as<T, std::int8_t>) return "int8";
        else if constexpr (std::same_as<T, std::uint8_t>) return "uint8";
        else if constexpr (std::same_as<T, std::int16_t>) return "int16";
        else if constexpr (std::same_as<T, std::uint16_t>) return "uint16";
        else if constexpr (std::same_as<T, std::int32_t>) return "int32";
        else if constexpr (std::same_as<T, std::uint32_t>) return "uint32";
        else if constexpr (std::same_as<T, std::int64_t>) return "int64";
        else if constexpr (std::same_as<T, std::uint64_t>) return "uint64";
        else if constexpr (std::same_as<T, float>) return "float32";
        else if constexpr (std::same_as<T, double>) return "float64";
        else static_assert(!sizeof(T*), "Unsupported NumPy dtype");
    }

    template <typename T>
    nb::object make_ndarray_view(
        const ArrowArray* arrow_array, std::size_t size, nb::handle owner
    )
    {
        static nb::module_ numpy = nb::module_::import_("numpy");
        const auto* data = static_cast<const T*>(arrow_array->buffers[1]) + arrow_array->offset;
        const Py_ssize_t nbytes = static_cast<Py_ssize_t>(size * sizeof(T));

        // Create a read-only contiguous memoryview over the Arrow buffer
        Py_buffer view;
        if (PyBuffer_FillInfo(&view, owner.ptr(), const_cast<T*>(data), nbytes, 1, PyBUF_CONTIG_RO) != 0)
        {
            throw nb::python_error();
        }
        nb::object memory_view = nb::steal(PyMemoryView_FromBuffer(&view));
        PyBuffer_Release(&view);

        return numpy.attr("frombuffer")(
            memory_view, nb::arg("dtype") = numpy_dtype_spec<T>(), nb::arg("count") = size
        );
    }

    template <typename T>
    nb::object make_ndarray_copy(
        const ArrowArray* arrow_array, std::size_t size, nb::handle owner
    )
    {
        // Use the view path then copy via numpy to get an independent writable array
        nb::object view = make_ndarray_view<T>(arrow_array, size, owner);
        return view.attr("copy")();
    }

    using supported_primitive_types = std::tuple<
        std::int8_t, std::uint8_t,
        std::int16_t, std::uint16_t,
        std::int32_t, std::uint32_t,
        std::int64_t, std::uint64_t,
        float, double
    >;

    template <typename Func>
    nb::object dispatch_on_primitive_type(
        sparrow::data_type dt, Func&& func
    )
    {
        switch (dt)
        {
            case sparrow::data_type::INT8:    return func.template operator()<std::int8_t>();
            case sparrow::data_type::UINT8:   return func.template operator()<std::uint8_t>();
            case sparrow::data_type::INT16:   return func.template operator()<std::int16_t>();
            case sparrow::data_type::UINT16:  return func.template operator()<std::uint16_t>();
            case sparrow::data_type::INT32:   return func.template operator()<std::int32_t>();
            case sparrow::data_type::UINT32:  return func.template operator()<std::uint32_t>();
            case sparrow::data_type::INT64:   return func.template operator()<std::int64_t>();
            case sparrow::data_type::UINT64:  return func.template operator()<std::uint64_t>();
            case sparrow::data_type::FLOAT:   return func.template operator()<float>();
            case sparrow::data_type::DOUBLE:  return func.template operator()<double>();
            case sparrow::data_type::BOOL:
                throw nb::type_error("ndarray_view/ndarray_copy do not support bool "
                    "(use to_numpy instead, which handles bit-packed expansion)");
            default:
                throw nb::type_error("ndarray_view/ndarray_copy only support primitive numeric Sparrow arrays");
        }
    }

    nb::object ndarray_view_from_sparrow(
        sparrow::rockfinch::SparrowArray& sparrow_array
    )
    {
        auto owner = nb::find(sparrow_array);
        if (!owner.is_valid())
        {
            throw nb::type_error("Could not resolve Python owner for SparrowArray");
        }

        auto& array = sparrow_array.get_array();
        if (array.null_count() != 0)
        {
            throw nb::type_error("ndarray_view_from_sparrow does not support nullable arrays");
        }

        auto* arrow_array = sparrow::get_arrow_array(array);
        const auto size = array.size();

        return dispatch_on_primitive_type(
            array.data_type(),
            [&]<typename T>() -> nb::object
            {
                return make_ndarray_view<T>(arrow_array, size, owner);
            }
        );
    }

    nb::object ndarray_copy_from_sparrow(
        sparrow::rockfinch::SparrowArray& sparrow_array
    )
    {
        auto owner = nb::find(sparrow_array);
        if (!owner.is_valid())
        {
            throw nb::type_error("Could not resolve Python owner for SparrowArray");
        }

        auto& array = sparrow_array.get_array();
        if (array.null_count() != 0)
        {
            throw nb::type_error("ndarray_copy_from_sparrow does not support nullable arrays");
        }

        auto* arrow_array = sparrow::get_arrow_array(array);
        const auto size = array.size();

        return dispatch_on_primitive_type(
            array.data_type(),
            [&]<typename T>() -> nb::object
            {
                return make_ndarray_copy<T>(arrow_array, size, owner);
            }
        );
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

    m.def("ndarray_view_from_sparrow", &ndarray_view_from_sparrow, nb::arg("sparrow_array"),
        "Create a read-only ndarray view directly from a SparrowArray's Arrow buffer.\n\n"
        "This bypasses the numpy_owner fast path and always creates a Python\n"
        "memoryview + np.frombuffer view, exercising the zero-copy buffer path.\n\n"
        "Raises TypeError for nullable or non-primitive arrays.");

    m.def("ndarray_copy_from_sparrow", &ndarray_copy_from_sparrow, nb::arg("sparrow_array"),
        "Create an independent writable ndarray copy from a SparrowArray's Arrow buffer.\n\n"
        "The returned array owns its own data and can be mutated freely.\n\n"
        "Raises TypeError for nullable or non-primitive arrays.");
}
