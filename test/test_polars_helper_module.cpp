/**
 * @file test_polars_helper_module.cpp
 * @brief Native Python extension module for Polars integration tests.
 *
 * This is a native Python extension module (not pybind11/nanobind) that tests
 * the sparrow::pycapsule interface. Being a proper extension module, it shares
 * the same Python runtime as the interpreter, avoiding the dual-runtime issues
 * that occur with ctypes.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <cstdint>
#include <iostream>
#include <vector>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

#include <sparrow-pycapsule/pycapsule.hpp>
#include <sparrow-pycapsule/SparrowPythonClass.hpp>

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

/**
 * Create a test array and return PyCapsules.
 * 
 * Python signature: create_test_array_capsules() -> tuple[capsule, capsule]
 */
static PyObject* py_create_test_array_capsules(PyObject* self, PyObject* args)
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

        auto [schema_capsule, array_capsule] = sparrow::pycapsule::export_array_to_capsules(arr);

        if (schema_capsule == nullptr || array_capsule == nullptr)
        {
            Py_XDECREF(schema_capsule);
            Py_XDECREF(array_capsule);
            PyErr_SetString(PyExc_RuntimeError, "Failed to create PyCapsules");
            return nullptr;
        }

        PyObject* result = PyTuple_Pack(2, schema_capsule, array_capsule);
        Py_DECREF(schema_capsule);
        Py_DECREF(array_capsule);
        return result;
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

/**
 * Import from an object implementing __arrow_c_array__ and return a SparrowArray.
 * 
 * Python signature: roundtrip_array(arrow_array) -> SparrowArray
 */
static PyObject* py_roundtrip_array(PyObject* self, PyObject* args)
{
    (void)self;

    PyObject* arrow_array = nullptr;

    if (!PyArg_ParseTuple(args, "O", &arrow_array))
    {
        return nullptr;
    }

    // Get __arrow_c_array__ method from the input object
    PyObject* arrow_c_array_method = PyObject_GetAttrString(arrow_array, "__arrow_c_array__");
    if (arrow_c_array_method == nullptr)
    {
        PyErr_SetString(PyExc_TypeError, "Object does not implement __arrow_c_array__");
        return nullptr;
    }

    // Call __arrow_c_array__() to get (schema_capsule, array_capsule)
    PyObject* capsules = PyObject_CallObject(arrow_c_array_method, nullptr);
    Py_DECREF(arrow_c_array_method);

    if (capsules == nullptr)
    {
        return nullptr;
    }

    if (!PyTuple_Check(capsules) || PyTuple_Size(capsules) != 2)
    {
        Py_DECREF(capsules);
        PyErr_SetString(PyExc_TypeError, "__arrow_c_array__ must return a tuple of 2 capsules");
        return nullptr;
    }

    PyObject* schema_capsule = PyTuple_GetItem(capsules, 0);
    PyObject* array_capsule = PyTuple_GetItem(capsules, 1);

    try
    {
        // Import from PyCapsules using sparrow::pycapsule
        sparrow::array arr = sparrow::pycapsule::import_array_from_capsules(
            schema_capsule,
            array_capsule
        );

        Py_DECREF(capsules);

        std::cout << "Roundtrip array size: " << arr.size() << '\n';

        // Return a SparrowArray object that implements __arrow_c_array__
        return sparrow::pycapsule::create_sparrow_array_object(std::move(arr));
    }
    catch (const std::exception& e)
    {
        Py_DECREF(capsules);
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

/**
 * Import array from PyCapsules and return new PyCapsules (roundtrip).
 * 
 * Python signature: roundtrip_array_capsules(schema_capsule, array_capsule) -> tuple[capsule, capsule]
 */
static PyObject* py_roundtrip_array_capsules(PyObject* self, PyObject* args)
{
    (void)self;

    PyObject* schema_capsule_in = nullptr;
    PyObject* array_capsule_in = nullptr;

    if (!PyArg_ParseTuple(args, "OO", &schema_capsule_in, &array_capsule_in))
    {
        return nullptr;
    }

    try
    {
        // Import from PyCapsules using sparrow::pycapsule
        sparrow::array arr = sparrow::pycapsule::import_array_from_capsules(
            schema_capsule_in,
            array_capsule_in
        );

        std::cout << "Roundtrip array size: " << arr.size() << '\n';

        // Export back to PyCapsules
        auto [schema_capsule, array_capsule] = sparrow::pycapsule::export_array_to_capsules(arr);

        if (schema_capsule == nullptr || array_capsule == nullptr)
        {
            Py_XDECREF(schema_capsule);
            Py_XDECREF(array_capsule);
            PyErr_SetString(PyExc_RuntimeError, "Failed to create output PyCapsules");
            return nullptr;
        }

        // Return as a tuple
        PyObject* result = PyTuple_Pack(2, schema_capsule, array_capsule);
        Py_DECREF(schema_capsule);
        Py_DECREF(array_capsule);
        return result;
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

/**
 * Verify that array imported from PyCapsules has the expected size.
 * 
 * Python signature: verify_array_size_from_capsules(schema_capsule, array_capsule, expected_size) -> bool
 */
static PyObject* py_verify_array_size_from_capsules(PyObject* self, PyObject* args)
{
    (void)self;

    PyObject* schema_capsule = nullptr;
    PyObject* array_capsule = nullptr;
    Py_ssize_t expected_size = 0;

    if (!PyArg_ParseTuple(args, "OOn", &schema_capsule, &array_capsule, &expected_size))
    {
        return nullptr;
    }

    try
    {
        // Import from PyCapsules using sparrow::pycapsule
        sparrow::array arr = sparrow::pycapsule::import_array_from_capsules(
            schema_capsule,
            array_capsule
        );

        std::cout << "Array size verified: " << arr.size() << '\n';

        if (static_cast<Py_ssize_t>(arr.size()) == expected_size)
        {
            Py_RETURN_TRUE;
        }
        else
        {
            std::cerr << "Size mismatch: expected " << expected_size << ", got " << arr.size() << '\n';
            Py_RETURN_FALSE;
        }
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// Method definitions
static PyMethodDef TestPolarsHelperMethods[] = {
    {
        "create_test_array",
        py_create_test_array,
        METH_NOARGS,
        "Create a test array and return a SparrowArray object implementing __arrow_c_array__."
    },
    {
        "create_test_array_capsules",
        py_create_test_array_capsules,
        METH_NOARGS,
        "Create a test array and return (schema_capsule, array_capsule) tuple."
    },
    {
        "roundtrip_array",
        py_roundtrip_array,
        METH_VARARGS,
        "Import from an object implementing __arrow_c_array__ and return a SparrowArray."
    },
    {
        "roundtrip_array_capsules",
        py_roundtrip_array_capsules,
        METH_VARARGS,
        "Import array from capsules and export back to new capsules."
    },
    {
        "verify_array_size_from_capsules",
        py_verify_array_size_from_capsules,
        METH_VARARGS,
        "Verify that array from capsules has the expected size."
    },
    {nullptr, nullptr, 0, nullptr}  // Sentinel
};

// Module definition
static struct PyModuleDef test_polars_helper_module = {
    PyModuleDef_HEAD_INIT,
    "test_polars_helper",  // Module name
    "Test helper module for sparrow-pycapsule Polars integration tests.\n"
    "This module tests the sparrow::pycapsule interface for Arrow data exchange.",
    -1,  // Module state size (-1 = no state)
    TestPolarsHelperMethods
};

// Module initialization function
PyMODINIT_FUNC PyInit_test_polars_helper(void)
{
    PyObject* module = PyModule_Create(&test_polars_helper_module);
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
