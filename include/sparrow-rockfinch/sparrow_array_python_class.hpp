#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sparrow/array.hpp>

#include "sparrow-rockfinch/config/config.hpp"
#include "sparrow-rockfinch/pycapsule.hpp"

namespace sparrow::rockfinch
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

        SparrowArray(const SparrowArray& other);
        SparrowArray(SparrowArray&& other) noexcept;
        SparrowArray& operator=(const SparrowArray& other);
        SparrowArray& operator=(SparrowArray&& other) noexcept;
        ~SparrowArray();

        /**
         * @brief Export the array via the Arrow PyCapsule interface (__arrow_c_array__).
         *
         * @return A pair of (schema_capsule, array_capsule). Caller owns the references.
         */
        [[nodiscard]] std::pair<PyObject*, PyObject*> export_to_capsules() const;

        /**
         * @brief Export the schema via the Arrow PyCapsule interface (__arrow_c_schema__).
         *
         * @return A PyCapsule containing an ArrowSchema. Caller owns the reference.
         */
        [[nodiscard]] PyObject* export_schema_to_capsule() const;

        /**
         * @brief Get the number of elements in the array.
         *
         * @return The size of the array.
         */
        [[nodiscard]] size_t size() const;

        /**
         * @brief Get a mutable reference to the underlying sparrow array.
         *
         * @return The wrapped sparrow array.
         */
        [[nodiscard]] sparrow::array& get_array();

        /**
         * @brief Get a const reference to the underlying sparrow array.
         *
         * @return The wrapped sparrow array.
         */
        [[nodiscard]] const sparrow::array& get_array() const;

        /**
         * @brief Set a NumPy array as the owner of the underlying data.
         *
         * This ensures the NumPy array stays alive as long as this SparrowArray
         * references its data buffer, preventing premature deallocation.
         *
         * @param owner The NumPy array Python object that owns the data.
         * @param writable Whether the buffer can be written to via this array.
         */
        void set_numpy_owner(PyObject* owner, bool writable);

        /**
         * @brief Get the NumPy array that owns the underlying data, if any.
         *
         * @return The PyObject* of the owning NumPy array, or nullptr if none was set.
         */
        [[nodiscard]] PyObject* numpy_owner() const;

        /**
         * @brief Check whether the NumPy-owned buffer is writable.
         *
         * @return true if the buffer is writable, false otherwise (or if no owner is set).
         */
        [[nodiscard]] bool numpy_owner_writable() const;

    private:
        /**
         * @brief Release the owned NumPy reference, if any, and reset metadata.
         *
         * ``m_numpy_owner`` stores an owned reference. Copy operations acquire a
         * fresh reference, while move operations transfer the pointer directly
         * and leave the source null, so there is no matching ``INCREF`` in the
         * move path.
         */
        void clear_numpy_owner();

        sparrow::array m_array;
        PyObject* m_numpy_owner = nullptr;
        bool m_numpy_owner_writable = false;
    };

}  // namespace sparrow::rockfinch
