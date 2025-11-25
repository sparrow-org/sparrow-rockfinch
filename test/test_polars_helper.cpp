/**
 * @file test_polars_helper.cpp
 * @brief C++ helper library for Polars integration tests.
 *
 * This library provides C functions that can be called from Python via ctypes
 * to test the bidirectional data exchange between Polars and sparrow using
 * the sparrow::pycapsule interface.
 */

#include "test_polars_helper.hpp"

#include <cstdint>
#include <iostream>
#include <vector>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

#include <sparrow-pycapsule/pycapsule.hpp>

extern "C"
{
    int create_test_array_capsules(PyObject** schema_capsule_out, PyObject** array_capsule_out)
    {
        try
        {
            std::vector<sparrow::nullable<int32_t>> values = {
                sparrow::make_nullable<int32_t>(10, true),
                sparrow::make_nullable<int32_t>(20, true),
                sparrow::make_nullable<int32_t>(0, false),  // null
                sparrow::make_nullable<int32_t>(40, true),
                sparrow::make_nullable<int32_t>(50, true)
            };

            sparrow::primitive_array<int32_t> prim_array(std::move(values));
            sparrow::array arr(std::move(prim_array));

            auto [schema_capsule, array_capsule] = sparrow::pycapsule::export_array_to_capsules(arr);

            if (schema_capsule == nullptr || array_capsule == nullptr)
            {
                std::cerr << "Failed to create PyCapsules\n";
                Py_XDECREF(schema_capsule);
                Py_XDECREF(array_capsule);
                return -1;
            }

            *schema_capsule_out = schema_capsule;
            *array_capsule_out = array_capsule;

            return 0;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in create_test_array_capsules: " << e.what() << '\n';
            return -1;
        }
    }

    int roundtrip_array_capsules(
        PyObject* schema_capsule_in,
        PyObject* array_capsule_in,
        PyObject** schema_capsule_out,
        PyObject** array_capsule_out
    )
    {
        try
        {
            if (schema_capsule_in == nullptr || array_capsule_in == nullptr)
            {
                std::cerr << "Null input capsules\n";
                return -1;
            }

            sparrow::array arr = sparrow::pycapsule::import_array_from_capsules(
                schema_capsule_in,
                array_capsule_in
            );

            std::cout << "Roundtrip array size: " << arr.size() << '\n';

            auto [schema_capsule, array_capsule] = sparrow::pycapsule::export_array_to_capsules(arr);

            if (schema_capsule == nullptr || array_capsule == nullptr)
            {
                std::cerr << "Failed to create output PyCapsules\n";
                Py_XDECREF(schema_capsule);
                Py_XDECREF(array_capsule);
                return -1;
            }

            *schema_capsule_out = schema_capsule;
            *array_capsule_out = array_capsule;

            return 0;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in roundtrip_array_capsules: " << e.what() << '\n';
            return -1;
        }
    }

    int verify_array_size_from_capsules(PyObject* schema_capsule, PyObject* array_capsule, size_t expected_size)
    {
        try
        {
            if (schema_capsule == nullptr || array_capsule == nullptr)
            {
                std::cerr << "Null capsules provided\n";
                return -1;
            }

            sparrow::array arr = sparrow::pycapsule::import_array_from_capsules(
                schema_capsule,
                array_capsule
            );

            if (arr.size() == expected_size)
            {
                std::cout << "Array size verified: " << arr.size() << '\n';
                return 0;
            }
            else
            {
                std::cerr << "Size mismatch: expected " << expected_size << ", got " << arr.size() << '\n';
                return -1;
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in verify_array_size_from_capsules: " << e.what() << '\n';
            return -1;
        }
    }
}
