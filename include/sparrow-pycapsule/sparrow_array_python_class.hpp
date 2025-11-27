#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sparrow/array.hpp>

#include "sparrow-pycapsule/config/config.hpp"
#include "sparrow-pycapsule/pycapsule.hpp"

namespace sparrow::pycapsule
{
    /**
     * @brief Python object structure for SparrowArray.
     *
     * This structure holds a pointer to a sparrow::array. The pointer is used
     * to avoid issues with C++ objects in C-style Python object structures.
     */
    struct SparrowArrayObject
    {
        PyObject_HEAD sparrow::array* arr;
    };

    /**
     * @brief Deallocator for SparrowArray Python objects.
     */
    SPARROW_PYCAPSULE_API void SparrowArray_dealloc(SparrowArrayObject* self);

    /**
     * @brief Implementation of __arrow_c_array__ method.
     *
     * This method exports the wrapped sparrow array as Arrow PyCapsules,
     * implementing the Arrow PyCapsule Interface (ArrowArrayExportable protocol).
     *
     * @param self The SparrowArray object.
     * @param args Positional arguments (unused).
     * @param kwargs Keyword arguments (optional requested_schema).
     * @return A tuple of (schema_capsule, array_capsule).
     */
    SPARROW_PYCAPSULE_API PyObject*
    SparrowArray_arrow_c_array(SparrowArrayObject* self, PyObject* args, PyObject* kwargs);

    /**
     * @brief Get the size of the wrapped array.
     *
     * @param self The SparrowArray object.
     * @param args Positional arguments (unused).
     * @return The size of the array as a Python integer.
     */
    SPARROW_PYCAPSULE_API PyObject* SparrowArray_size(SparrowArrayObject* self, PyObject* args);

    /**
     * @brief Get the Python type object for SparrowArray.
     *
     * This function returns a pointer to the SparrowArrayType. The type is
     * initialized on first call if necessary.
     *
     * @return Pointer to the SparrowArrayType, or nullptr on error.
     */
    SPARROW_PYCAPSULE_API PyTypeObject* get_sparrow_array_type();

    /**
     * @brief Create a new SparrowArray Python object from a sparrow::array.
     *
     * This function creates a new Python object that wraps the given sparrow array.
     * The array is moved into the Python object, so the caller should not use it
     * after this call.
     *
     * @param arr The sparrow array to wrap (will be moved).
     * @return A new reference to a SparrowArray Python object, or nullptr on error.
     */
    SPARROW_PYCAPSULE_API PyObject* create_sparrow_array_object(sparrow::array&& arr);

    /**
     * @brief Create a new SparrowArray Python object from PyCapsules.
     *
     * This function creates a new Python object by importing from existing
     * Arrow PyCapsules.
     *
     * @param schema_capsule The schema PyCapsule.
     * @param array_capsule The array PyCapsule.
     * @return A new reference to a SparrowArray Python object, or nullptr on error.
     */
    SPARROW_PYCAPSULE_API PyObject*
    create_sparrow_array_object_from_capsules(PyObject* schema_capsule, PyObject* array_capsule);

    /**
     * @brief Register the SparrowArray type with a Python module.
     *
     * This function adds the SparrowArray type to the given module.
     *
     * @param module The Python module to add the type to.
     * @return 0 on success, -1 on error.
     */
    SPARROW_PYCAPSULE_API int register_sparrow_array_type(PyObject* module);

}  // namespace sparrow::pycapsule
