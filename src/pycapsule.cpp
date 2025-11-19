#include <string_view>

#include <sparrow-pycapsule/pycapsule.hpp>

#include <sparrow/array.hpp>
#include <sparrow/c_interface.hpp>

namespace sparrow::pycapsule
{
    namespace
    {
        // Internal capsule name constants
        constexpr std::string_view arrow_schema_str = "arrow_schema";
        constexpr std::string_view arrow_array_str = "arrow_array";
    }

    void release_arrow_schema_pycapsule(PyObject* capsule)
    {
        if (capsule == nullptr)
        {
            return;
        }
        auto schema = static_cast<ArrowSchema*>(PyCapsule_GetPointer(capsule, arrow_schema_str.data()));
        if (schema == nullptr)
        {
            return;
        }
        if (schema->release != nullptr)
        {
            schema->release(schema);
        }
        delete schema;
    }

    PyObject* export_arrow_schema_pycapsule(array& arr)
    {
        // Allocate a new ArrowSchema on the heap and extract (move) the schema
        ArrowSchema* arrow_schema_ptr = new ArrowSchema();
        *arrow_schema_ptr = extract_arrow_schema(std::move(arr));

        PyObject* capsule_ptr =  PyCapsule_New(arrow_schema_ptr, arrow_schema_str.data(), release_arrow_schema_pycapsule);
        if(capsule_ptr == nullptr)
        {
            arrow_schema_ptr->release(arrow_schema_ptr);
            delete arrow_schema_ptr;
            return nullptr;
        }
        return capsule_ptr;
    }

    ArrowSchema* get_arrow_schema_pycapsule(PyObject* capsule)
    {
        return static_cast<ArrowSchema*>(PyCapsule_GetPointer(capsule, arrow_schema_str.data()));
    }

    void release_arrow_array_pycapsule(PyObject* capsule)
    {
        if (capsule == nullptr)
        {
            return;
        }
        auto array = static_cast<ArrowArray*>(PyCapsule_GetPointer(capsule, arrow_array_str.data()));
        if (array == nullptr)
        {
            return;
        }
        if (array->release != nullptr)
        {
            array->release(array);
        }
        delete array;
    }

    PyObject* export_arrow_array_pycapsule(array& arr)
    {
        // Allocate a new ArrowArray on the heap and extract (move) the array
        ArrowArray* arrow_array_ptr = new ArrowArray();
        *arrow_array_ptr = extract_arrow_array(std::move(arr));

        PyObject* capsule_ptr =  PyCapsule_New(arrow_array_ptr, arrow_array_str.data(), release_arrow_array_pycapsule);
        if(capsule_ptr == nullptr)
        {
            arrow_array_ptr->release(arrow_array_ptr);
            delete arrow_array_ptr;
            return nullptr;
        }
        return capsule_ptr;
    }

    ArrowArray* get_arrow_array_pycapsule(PyObject* capsule)
    {
        return static_cast<ArrowArray*>(PyCapsule_GetPointer(capsule, arrow_array_str.data()));
    }

    array import_array_from_capsules(PyObject* schema_capsule, PyObject* array_capsule)
    {
        ArrowSchema* schema = get_arrow_schema_pycapsule(schema_capsule);
        if (schema == nullptr)
        {
            // Error already set by PyCapsule_GetPointer
            return array{};
        }

        ArrowArray* arr = get_arrow_array_pycapsule(array_capsule);
        if (arr == nullptr)
        {
            // Error already set by PyCapsule_GetPointer
            return array{};
        }

        // Move the data from the capsule structures
        // The capsule destructors will still be called, but they will see
        // that release is null and won't do anything
        ArrowSchema schema_moved = *schema;
        ArrowArray array_moved = *arr;

        // Mark as released to prevent the capsule destructors from freeing the data
        schema->release = nullptr;
        arr->release = nullptr;

        return array(std::move(array_moved), std::move(schema_moved));
    }

    std::pair<PyObject*, PyObject*> export_array_to_capsules(array& arr)
    {
        // Extract both schema and array from the sparrow array (moves ownership)
        auto [arrow_array, arrow_schema] = extract_arrow_structures(std::move(arr));

        // Allocate heap copies for the PyCapsules
        ArrowSchema* schema_ptr = new ArrowSchema(std::move(arrow_schema));
        ArrowArray* array_ptr = new ArrowArray(std::move(arrow_array));

        PyObject* schema_capsule = PyCapsule_New(schema_ptr, arrow_schema_str.data(), release_arrow_schema_pycapsule);
        if (!schema_capsule) {
            delete schema_ptr;
            delete array_ptr;
            return {nullptr, nullptr};
        }
        PyObject* array_capsule = PyCapsule_New(array_ptr, arrow_array_str.data(), release_arrow_array_pycapsule);
        if (!array_capsule) {
            delete schema_ptr;
            delete array_ptr;
            Py_DECREF(schema_capsule);
            return {nullptr, nullptr};
        }

        return {schema_capsule, array_capsule};
    }
}