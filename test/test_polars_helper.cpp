/**
 * @file test_polars_helper.cpp
 * @brief C++ helper library for Polars integration tests.
 *
 * This library provides C functions that can be called from Python via ctypes
 * to test the bidirectional data exchange between Polars and sparrow.
 */

#include "test_polars_helper.hpp"

#include <cstdint>
#include <iostream>
#include <vector>

#include <Python.h>
#include <sparrow-pycapsule/config/config.hpp>
#include <sparrow-pycapsule/pycapsule.hpp>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

// Export C API functions for ctypes
extern "C"
{
    /**
     * @brief Initialize Python interpreter if not already initialized.
     *
     * Note: When called from Python (via ctypes), Python is already initialized.
     * This function only initializes if called from pure C++ context.
     */
    void init_python()
    {
        // When called from Python via ctypes, Python is already initialized
        // So this check should always be true, and we do nothing
        if (Py_IsInitialized())
        {
            // Python already initialized - this is the normal case when called from Python
            return;
        }

        // Only initialize if we're being called from C++ without Python
        Py_Initialize();
    }

    /**
     * @brief Create a simple test array and return raw Arrow C pointers.
     *
     * Instead of creating PyCapsules in C++, we return raw pointers that Python
     * will wrap in PyCapsules. This avoids Python C API calls from ctypes libraries.
     *
     * @param schema_ptr_out Output parameter for ArrowSchema pointer
     * @param array_ptr_out Output parameter for ArrowArray pointer
     * @return 0 on success, -1 on error
     */
    int create_test_array_as_pointers(void** schema_ptr_out, void** array_ptr_out)
    {
        try
        {
            // Create a test array with nullable integers
            std::vector<sparrow::nullable<int32_t>> values = {
                sparrow::make_nullable<int32_t>(10, true),
                sparrow::make_nullable<int32_t>(20, true),
                sparrow::make_nullable<int32_t>(0, false),  // null
                sparrow::make_nullable<int32_t>(40, true),
                sparrow::make_nullable<int32_t>(50, true)
            };

            sparrow::primitive_array<int32_t> prim_array(std::move(values));
            sparrow::array arr(std::move(prim_array));

            // Extract Arrow C structures
            auto [arrow_array, arrow_schema] = sparrow::extract_arrow_structures(std::move(arr));

            // Allocate on heap and transfer ownership to Python
            ArrowSchema* schema_ptr = new ArrowSchema(std::move(arrow_schema));
            ArrowArray* array_ptr = new ArrowArray(std::move(arrow_array));

            *schema_ptr_out = schema_ptr;
            *array_ptr_out = array_ptr;

            return 0;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in create_test_array_as_pointers: " << e.what() << std::endl;
            return -1;
        }
    }

    /**
     * @brief Import array from raw Arrow C pointers and return new pointers.
     *
     * @param schema_ptr_in Input ArrowSchema pointer
     * @param array_ptr_in Input ArrowArray pointer
     * @param schema_ptr_out Output ArrowSchema pointer
     * @param array_ptr_out Output ArrowArray pointer
     * @return 0 on success, -1 on error
     */
    int
    roundtrip_array_pointers(void* schema_ptr_in, void* array_ptr_in, void** schema_ptr_out, void** array_ptr_out)
    {
        try
        {
            if (schema_ptr_in == nullptr || array_ptr_in == nullptr)
            {
                std::cerr << "Null input pointers" << std::endl;
                return -1;
            }

            ArrowSchema* schema_in = static_cast<ArrowSchema*>(schema_ptr_in);
            ArrowArray* array_in = static_cast<ArrowArray*>(array_ptr_in);

            // Move the data (mark originals as released to prevent double-free)
            ArrowSchema schema_moved = *schema_in;
            ArrowArray array_moved = *array_in;
            schema_in->release = nullptr;
            array_in->release = nullptr;

            // Import into sparrow
            sparrow::array arr(std::move(array_moved), std::move(schema_moved));

            std::cout << "Roundtrip array size: " << arr.size() << std::endl;

            // Export back out
            auto [arrow_array_out, arrow_schema_out] = sparrow::extract_arrow_structures(std::move(arr));

            ArrowSchema* schema_out = new ArrowSchema(std::move(arrow_schema_out));
            ArrowArray* array_out = new ArrowArray(std::move(arrow_array_out));

            *schema_ptr_out = schema_out;
            *array_ptr_out = array_out;

            return 0;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in roundtrip_array_pointers: " << e.what() << std::endl;
            return -1;
        }
    }

    /**
     * @brief Verify that Arrow C structures have the expected size.
     *
     * @param schema_ptr ArrowSchema pointer
     * @param array_ptr ArrowArray pointer
     * @param expected_size Expected array size
     * @return 0 if size matches, -1 otherwise
     */
    int verify_array_size_from_pointers(void* schema_ptr, void* array_ptr, size_t expected_size)
    {
        try
        {
            if (schema_ptr == nullptr || array_ptr == nullptr)
            {
                std::cerr << "Null pointers provided" << std::endl;
                return -1;
            }

            ArrowSchema* schema = static_cast<ArrowSchema*>(schema_ptr);
            ArrowArray* array = static_cast<ArrowArray*>(array_ptr);

            // Move the data (mark originals as released)
            ArrowSchema schema_moved = *schema;
            ArrowArray array_moved = *array;
            schema->release = nullptr;
            array->release = nullptr;

            sparrow::array arr(std::move(array_moved), std::move(schema_moved));

            if (arr.size() == expected_size)
            {
                std::cout << "Array size verified: " << arr.size() << std::endl;
                return 0;
            }
            else
            {
                std::cerr << "Size mismatch: expected " << expected_size << ", got " << arr.size() << std::endl;
                return -1;
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in verify_array_size_from_pointers: " << e.what() << std::endl;
            return -1;
        }
    }
}
