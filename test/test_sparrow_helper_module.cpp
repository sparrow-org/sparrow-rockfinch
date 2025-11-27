#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <cstdint>
#include <vector>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

#include <sparrow-pycapsule/pycapsule.hpp>
#include <sparrow-pycapsule/sparrow_array_python_class.hpp>

/**
 * Create a test array and return a SparrowArray object.
 * 
 * Python signature: create_test_array() -> SparrowArray
 */
static PyObject* py_create_test_array(PyObject* self, PyObject* args)
{
    (void)self;
    (void)args;

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

        // Return a SparrowArray object that implements __arrow_c_array__
        return sparrow::pycapsule::create_sparrow_array_object(std::move(arr));
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// Method definitions
static PyMethodDef TestSparrowHelperMethods[] = {
    {
        "create_test_array",
        py_create_test_array,
        METH_NOARGS,
        "Create a test array and return a SparrowArray object implementing __arrow_c_array__."
    },
    {nullptr, nullptr, 0, nullptr}  // Sentinel
};

// Module definition
static struct PyModuleDef test_sparrow_helper_module = {
    PyModuleDef_HEAD_INIT,
    "test_sparrow_helper",  // Module name
    "Native Python extension providing SparrowArray type for Arrow data exchange.\n"
    "Higher-level helpers are available in sparrow_helpers.py.",
    -1,  // Module state size (-1 = no state)
    TestSparrowHelperMethods
};

// Module initialization function
PyMODINIT_FUNC PyInit_test_sparrow_helper(void)
{
    PyObject* module = PyModule_Create(&test_sparrow_helper_module);
    if (module == nullptr)
    {
        return nullptr;
    }

    // Register the SparrowArray type with this module
    if (sparrow::pycapsule::register_sparrow_array_type(module) < 0)
    {
        Py_DECREF(module);
        return nullptr;
    }

    return module;
}
