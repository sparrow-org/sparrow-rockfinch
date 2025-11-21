/**
 * @file test_polars_helper.hpp
 * @brief C++ helper library declarations for Polars integration tests.
 *
 * This header declares C functions that can be called from Python via ctypes
 * to test the bidirectional data exchange between Polars and sparrow.
 */

#ifndef SPARROW_PYCAPSULE_TEST_POLARS_HELPER_HPP
#define SPARROW_PYCAPSULE_TEST_POLARS_HELPER_HPP

#include <cstddef>
#include <sparrow-pycapsule/config/config.hpp>

extern "C"
{
    /**
     * @brief Initialize Python interpreter if not already initialized.
     *
     * Note: When called from Python (via ctypes), Python is already initialized.
     * This function only initializes if called from pure C++ context.
     */
    SPARROW_PYCAPSULE_API void init_python();

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
    SPARROW_PYCAPSULE_API int create_test_array_as_pointers(void** schema_ptr_out, void** array_ptr_out);

    /**
     * @brief Import array from raw Arrow C pointers and return new pointers.
     *
     * @param schema_ptr_in Input ArrowSchema pointer
     * @param array_ptr_in Input ArrowArray pointer
     * @param schema_ptr_out Output ArrowSchema pointer
     * @param array_ptr_out Output ArrowArray pointer
     * @return 0 on success, -1 on error
     */
    SPARROW_PYCAPSULE_API int roundtrip_array_pointers(
        void* schema_ptr_in,
        void* array_ptr_in,
        void** schema_ptr_out,
        void** array_ptr_out
    );

    /**
     * @brief Verify that Arrow C structures have the expected size.
     *
     * @param schema_ptr ArrowSchema pointer
     * @param array_ptr ArrowArray pointer
     * @param expected_size Expected array size
     * @return 0 if size matches, -1 otherwise
     */
    SPARROW_PYCAPSULE_API int verify_array_size_from_pointers(
        void* schema_ptr,
        void* array_ptr,
        size_t expected_size
    );
}

#endif // SPARROW_PYCAPSULE_TEST_POLARS_HELPER_HPP
