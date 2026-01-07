#pragma once

#include <utility>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <sparrow-rockfinch/config/config.hpp>

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
    SPARROW_ROCKFINCH_API array import_array_from_capsules(PyObject* schema_capsule, PyObject* array_capsule);

    /**
     * @brief Exports a sparrow array to both schema and array PyCapsules.
     *
     * This is the recommended way to export an array, as it creates both
     * required capsules in one call. The array is moved from and becomes invalid.
     *
     * @param arr The sparrow array to export (will be moved from)
     * @return A pair of (schema_capsule, array_capsule), or (nullptr, nullptr) on error
     */
    SPARROW_ROCKFINCH_API std::pair<PyObject*, PyObject*> export_array_to_capsules(array& arr);
}
