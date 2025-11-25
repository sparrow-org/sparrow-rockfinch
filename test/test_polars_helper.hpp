/**
 * @file test_polars_helper.hpp
 * @brief C++ helper library declarations for Polars integration tests.
 *
 * This header declares C functions that can be called from Python via ctypes
 * to test the bidirectional data exchange between Polars and sparrow using
 * the sparrow::pycapsule interface.
 */

#ifndef SPARROW_PYCAPSULE_TEST_POLARS_HELPER_HPP
#define SPARROW_PYCAPSULE_TEST_POLARS_HELPER_HPP

#include <cstddef>

#include <Python.h>
#include <sparrow-pycapsule/config/config.hpp>

extern "C"
{
    /**
     * @brief Create a test array and return PyCapsules.
     *
     * Uses sparrow::pycapsule::export_array_to_capsules() to create the capsules.
     *
     * @param schema_capsule_out Output parameter for schema PyCapsule
     * @param array_capsule_out Output parameter for array PyCapsule
     * @return 0 on success, -1 on error
     */
    SPARROW_PYCAPSULE_API int create_test_array_capsules(PyObject** schema_capsule_out, PyObject** array_capsule_out);

    /**
     * @brief Import array from PyCapsules and return new PyCapsules.
     *
     * Uses sparrow::pycapsule::import_array_from_capsules() and
     * sparrow::pycapsule::export_array_to_capsules().
     *
     * @param schema_capsule_in Input schema PyCapsule
     * @param array_capsule_in Input array PyCapsule
     * @param schema_capsule_out Output schema PyCapsule
     * @param array_capsule_out Output array PyCapsule
     * @return 0 on success, -1 on error
     */
    SPARROW_PYCAPSULE_API int roundtrip_array_capsules(
        PyObject* schema_capsule_in,
        PyObject* array_capsule_in,
        PyObject** schema_capsule_out,
        PyObject** array_capsule_out
    );

    /**
     * @brief Verify that array imported from PyCapsules has the expected size.
     *
     * Uses sparrow::pycapsule::import_array_from_capsules().
     *
     * @param schema_capsule Schema PyCapsule
     * @param array_capsule Array PyCapsule
     * @param expected_size Expected array size
     * @return 0 if size matches, -1 otherwise
     */
    SPARROW_PYCAPSULE_API int verify_array_size_from_capsules(
        PyObject* schema_capsule,
        PyObject* array_capsule,
        size_t expected_size
    );
}

#endif // SPARROW_PYCAPSULE_TEST_POLARS_HELPER_HPP
