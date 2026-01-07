#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sparrow/array.hpp>

#include "sparrow-rockfinch/config/config.hpp"
#include "sparrow-rockfinch/pycapsule.hpp"

namespace sparrow::pycapsule
{
    /**
     * @brief C++ wrapper class for sparrow::array with Python interop.
     *
     * This class wraps a sparrow::array and provides methods for Arrow PyCapsule
     * Interface (ArrowArrayExportable protocol), allowing it to be passed
     * directly to libraries like Polars via pl.from_arrow().
     * 
     * Note: This class is designed to be wrapped by nanobind (or similar)
     * in a Python extension module.
     */
    class SPARROW_ROCKFINCH_API SparrowArray
    {
    public:
        /**
         * @brief Construct a SparrowArray by importing from PyCapsules.
         *
         * @param schema_capsule PyCapsule containing an ArrowSchema.
         * @param array_capsule PyCapsule containing an ArrowArray.
         */
        SparrowArray(PyObject* schema_capsule, PyObject* array_capsule);

        /**
         * @brief Construct a SparrowArray from an existing sparrow::array.
         *
         * @param arr The sparrow array to wrap (will be moved).
         */
        explicit SparrowArray(sparrow::array&& arr);

        /**
         * @brief Export the array via the Arrow PyCapsule interface.
         *
         * @return A pair of (schema_capsule, array_capsule). Caller owns the references.
         */
        std::pair<PyObject*, PyObject*> export_to_capsules() const;

        /**
         * @brief Get the number of elements in the array.
         *
         * @return The size of the array.
         */
        [[nodiscard]] size_t size() const;

        /**
         * @brief Get a const reference to the underlying sparrow array.
         *
         * @return The wrapped sparrow array.
         */
        [[nodiscard]] const sparrow::array& get_array() const;

    private:
        sparrow::array m_array;
    };

}  // namespace sparrow::pycapsule
