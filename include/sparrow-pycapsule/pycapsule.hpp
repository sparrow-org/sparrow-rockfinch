#pragma once

#include <utility>

#include <Python.h>
#include <sparrow-pycapsule/config/config.hpp>

// Forward declarations to avoid including heavy headers
namespace sparrow
{
    class array;
}

struct ArrowSchema;
struct ArrowArray;

namespace sparrow::pycapsule
{
    /**
     * @brief Capsule destructor for ArrowSchema PyCapsules.
     *
     * Calls the schema's release callback if not null, then frees the schema.
     * This is used as the PyCapsule destructor to ensure proper cleanup.
     *
     * @param capsule The PyCapsule containing an ArrowSchema pointer
     */
    SPARROW_PYCAPSULE_API void ReleaseArrowSchemaPyCapsule(PyObject* capsule);

    /**
     * @brief Exports a sparrow array's schema to a PyCapsule.
     *
     * Creates a new ArrowSchema on the heap and transfers ownership from the array.
     * The array is moved from and becomes invalid after this call.
     *
     * @param arr The sparrow array to export (will be moved from)
     * @return A new PyCapsule containing the ArrowSchema, or nullptr on error
     */
    SPARROW_PYCAPSULE_API PyObject* ExportArrowSchemaPyCapsule(array& arr);

    /**
     * @brief Retrieves the ArrowSchema pointer from a PyCapsule.
     *
     * @param capsule The PyCapsule to extract the schema from
     * @return Pointer to the ArrowSchema, or nullptr if the capsule is invalid (sets Python exception)
     */
    SPARROW_PYCAPSULE_API ArrowSchema* GetArrowSchemaPyCapsule(PyObject* capsule);

    /**
     * @brief Capsule destructor for ArrowArray PyCapsules.
     *
     * Calls the array's release callback if not null, then frees the array.
     * This is used as the PyCapsule destructor to ensure proper cleanup.
     *
     * @param capsule The PyCapsule containing an ArrowArray pointer
     */
    SPARROW_PYCAPSULE_API void ReleaseArrowArrayPyCapsule(PyObject* capsule);

    /**
     * @brief Exports a sparrow array's data to a PyCapsule.
     *
     * Creates a new ArrowArray on the heap and transfers ownership from the array.
     * The array is moved from and becomes invalid after this call.
     *
     * @param arr The sparrow array to export (will be moved from)
     * @return A new PyCapsule containing the ArrowArray, or nullptr on error
     */
    SPARROW_PYCAPSULE_API PyObject* ExportArrowArrayPyCapsule(array& arr);

    /**
     * @brief Retrieves the ArrowArray pointer from a PyCapsule.
     *
     * @param capsule The PyCapsule to extract the array from
     * @return Pointer to the ArrowArray, or nullptr if the capsule is invalid (sets Python exception)
     */
    SPARROW_PYCAPSULE_API ArrowArray* GetArrowArrayPyCapsule(PyObject* capsule);

    /**
     * @brief Imports a sparrow array from schema and array PyCapsules.
     *
     * Transfers ownership from the capsules to the returned array.
     * After successful import, the capsules' release callbacks are set to nullptr,
     * and the returned array owns the data.
     *
     * @param schema_capsule PyCapsule containing an ArrowSchema
     * @param array_capsule PyCapsule containing an ArrowArray
     * @return A sparrow array constructed from the capsules, or an empty array on error
     */
    SPARROW_PYCAPSULE_API array import_array_from_capsules(PyObject* schema_capsule, PyObject* array_capsule);

    /**
     * @brief Exports a sparrow array to both schema and array PyCapsules.
     *
     * This is the recommended way to export an array, as it creates both
     * required capsules in one call. The array is moved from and becomes invalid.
     *
     * @param arr The sparrow array to export (will be moved from)
     * @return A pair of (schema_capsule, array_capsule), or (nullptr, nullptr) on error
     */
    SPARROW_PYCAPSULE_API std::pair<PyObject*, PyObject*> export_array_to_capsules(array& arr);
}
