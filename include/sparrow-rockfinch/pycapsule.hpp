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
     * @brief Exports an arrow_array_stream_proxy as an ArrowArrayStream PyCapsule.
     *
     * Exports the stream proxy's internal stream to a PyCapsule.
     *
     * @param proxy The stream proxy to export
     * @return A PyCapsule containing an ArrowArrayStream, or nullptr on error
     */
    SPARROW_ROCKFINCH_API PyObject* export_stream_proxy_to_capsule(arrow_array_stream_proxy& proxy);

    /**
     * @brief Imports an arrow_array_stream_proxy from an ArrowArrayStream PyCapsule.
     *
     * Creates a stream proxy from the capsule's stream. The proxy takes ownership
     * of the stream data.
     *
     * @param stream_capsule PyCapsule containing an ArrowArrayStream
     * @return An arrow_array_stream_proxy, or an empty proxy on error
     */
    SPARROW_ROCKFINCH_API arrow_array_stream_proxy import_stream_proxy_from_capsule(PyObject* stream_capsule);

}
