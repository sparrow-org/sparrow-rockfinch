#include "sparrow-pycapsule/SparrowPythonClass.hpp"

#include <new>
#include <utility>

namespace sparrow::pycapsule
{
    void SparrowArray_dealloc(SparrowArrayObject* self)
    {
        delete self->arr;
        self->arr = nullptr;
        Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
    }

    PyObject* SparrowArray_arrow_c_array(SparrowArrayObject* self, PyObject* args, PyObject* kwargs)
    {
        static const char* kwlist[] = {"requested_schema", nullptr};
        PyObject* requested_schema = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", const_cast<char**>(kwlist), &requested_schema))
        {
            return nullptr;
        }

        // requested_schema is typically ignored for simple cases
        // In a full implementation, you might use it to cast to a different type
        (void) requested_schema;

        if (self->arr == nullptr)
        {
            PyErr_SetString(PyExc_ValueError, "SparrowArray contains no data");
            return nullptr;
        }

        try
        {
            auto [schema_capsule, array_capsule] = export_array_to_capsules(*self->arr);

            if (schema_capsule == nullptr || array_capsule == nullptr)
            {
                Py_XDECREF(schema_capsule);
                Py_XDECREF(array_capsule);
                PyErr_SetString(PyExc_RuntimeError, "Failed to create Arrow PyCapsules");
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

    PyObject* SparrowArray_size(SparrowArrayObject* self, [[maybe_unused]] PyObject* args)
    {
        if (self->arr == nullptr)
        {
            PyErr_SetString(PyExc_ValueError, "SparrowArray contains no data");
            return nullptr;
        }

        return PyLong_FromSize_t(self->arr->size());
    }

    static PyMethodDef SparrowArray_methods[] = {
        {"__arrow_c_array__",
         reinterpret_cast<PyCFunction>(SparrowArray_arrow_c_array),
         METH_VARARGS | METH_KEYWORDS,
         "Export the array via the Arrow PyCapsule interface.\n\n"
         "Parameters\n"
         "----------\n"
         "requested_schema : object, optional\n"
         "    Requested schema for the output (typically ignored).\n\n"
         "Returns\n"
         "-------\n"
         "tuple[object, object]\n"
         "    A tuple of (schema_capsule, array_capsule)."},
        {"size",
         reinterpret_cast<PyCFunction>(SparrowArray_size),
         METH_NOARGS,
         "Get the number of elements in the array.\n\n"
         "Returns\n"
         "-------\n"
         "int\n"
         "    The size of the array."},
        {nullptr, nullptr, 0, nullptr}  // Sentinel
    };

    // The type object - defined as a static variable
    static PyTypeObject SparrowArrayType = {
        .ob_base = PyVarObject_HEAD_INIT(nullptr, 0).tp_name = "sparrow.SparrowArray",
        .tp_basicsize = sizeof(SparrowArrayObject),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(SparrowArray_dealloc),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = PyDoc_STR(
            "SparrowArray - Arrow array wrapper implementing __arrow_c_array__.\n\n"
            "This class wraps a sparrow array and implements the Arrow PyCapsule\n"
            "Interface (ArrowArrayExportable protocol), allowing it to be passed\n"
            "directly to libraries like Polars via pl.from_arrow()."
        ),
        .tp_methods = SparrowArray_methods,
    };

    static bool type_initialized = false;

    PyTypeObject* get_sparrow_array_type()
    {
        if (!type_initialized)
        {
            if (PyType_Ready(&SparrowArrayType) < 0)
            {
                return nullptr;
            }
            type_initialized = true;
        }
        return &SparrowArrayType;
    }

    PyObject* create_sparrow_array_object(sparrow::array&& arr)
    {
        PyTypeObject* type = get_sparrow_array_type();
        if (type == nullptr)
        {
            return nullptr;
        }

        SparrowArrayObject* obj = PyObject_New(SparrowArrayObject, type);
        if (obj == nullptr)
        {
            return nullptr;
        }

        try
        {
            obj->arr = new sparrow::array(std::move(arr));
        }
        catch (const std::bad_alloc&)
        {
            Py_DECREF(obj);
            PyErr_NoMemory();
            return nullptr;
        }
        catch (const std::exception& e)
        {
            Py_DECREF(obj);
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return nullptr;
        }

        return reinterpret_cast<PyObject*>(obj);
    }

    PyObject* create_sparrow_array_object_from_capsules(PyObject* schema_capsule, PyObject* array_capsule)
    {
        try
        {
            sparrow::array arr = import_array_from_capsules(schema_capsule, array_capsule);
            return create_sparrow_array_object(std::move(arr));
        }
        catch (const std::exception& e)
        {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return nullptr;
        }
    }

    int register_sparrow_array_type(PyObject* module)
    {
        PyTypeObject* type = get_sparrow_array_type();
        if (type == nullptr)
        {
            return -1;
        }

        Py_INCREF(type);
        if (PyModule_AddObject(module, "SparrowArray", reinterpret_cast<PyObject*>(type)) < 0)
        {
            Py_DECREF(type);
            return -1;
        }

        return 0;
    }

}  // namespace sparrow::pycapsule
