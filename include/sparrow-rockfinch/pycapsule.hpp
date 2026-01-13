#pragma once

#define PY_SSIZE_T_CLEAN
#include <utility>
#include <vector>

#include <Python.h>
#include <sparrow-rockfinch/config/config.hpp>

// Forward declarations to avoid including heavy headers
namespace sparrow
{
    class array;
    class arrow_array_stream_proxy;
}

struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

namespace sparrow::rockfinch
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

    // ========================================================================
    // ArrowSchema Export (PyCapsule Interface: __arrow_c_schema__)
    // ========================================================================

    /**
     * @brief Exports the schema of a sparrow array to a PyCapsule.
     *
     * Implements the ArrowSchemaExportable protocol (__arrow_c_schema__).
     * The capsule has the name "arrow_schema" as per the Arrow PyCapsule Interface.
     *
     * @param arr The sparrow array whose schema to export
     * @return A PyCapsule containing an ArrowSchema, or nullptr on error
     */
    SPARROW_ROCKFINCH_API PyObject* export_schema_to_capsule(const array& arr);

    // ========================================================================
    // ArrowArrayStream Export/Import (PyCapsule Interface: __arrow_c_stream__)
    // ========================================================================

    /**
     * @brief Exports a sparrow array as an ArrowArrayStream PyCapsule.
     *
     * Implements the ArrowStreamExportable protocol (__arrow_c_stream__).
     * The array is treated as a stream containing a single batch.
     * The capsule has the name "arrow_array_stream" as per the Arrow PyCapsule Interface.
     *
     * @param arr The sparrow array to export (will be moved from)
     * @return A PyCapsule containing an ArrowArrayStream, or nullptr on error
     */
    SPARROW_ROCKFINCH_API PyObject* export_array_to_stream_capsule(array& arr);

    /**
     * @brief Exports multiple sparrow arrays as an ArrowArrayStream PyCapsule.
     *
     * All arrays must have compatible schemas. The first array's schema is used
     * as the stream schema.
     *
     * @param arrays Vector of sparrow arrays to export (will be moved from)
     * @return A PyCapsule containing an ArrowArrayStream, or nullptr on error
     */
    SPARROW_ROCKFINCH_API PyObject* export_arrays_to_stream_capsule(std::vector<array>& arrays);

    /**
     * @brief Imports sparrow arrays from an ArrowArrayStream PyCapsule.
     *
     * Consumes all batches from the stream and returns them as a vector of arrays.
     * After successful import, the capsule's stream is exhausted.
     *
     * @param stream_capsule PyCapsule containing an ArrowArrayStream
     * @return A vector of sparrow arrays, or empty vector on error
     */
    SPARROW_ROCKFINCH_API std::vector<array> import_arrays_from_stream_capsule(PyObject* stream_capsule);

    /**
     * @brief Imports a single sparrow array from an ArrowArrayStream PyCapsule.
     *
     * Returns the first batch from the stream. If the stream contains multiple
     * batches, only the first is returned and the rest are discarded.
     *
     * @param stream_capsule PyCapsule containing an ArrowArrayStream
     * @return A sparrow array, or an empty array on error
     */
    SPARROW_ROCKFINCH_API array import_array_from_stream_capsule(PyObject* stream_capsule);
}
