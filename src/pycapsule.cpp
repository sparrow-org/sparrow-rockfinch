#include <sparrow-rockfinch/pycapsule.hpp>

#include <sparrow/array.hpp>
#include <sparrow/c_interface.hpp>
#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_array_stream.hpp>
#include <sparrow/arrow_interface/arrow_array_stream/private_data.hpp>

namespace sparrow::rockfinch
{
    namespace
    {
        // Internal capsule name constants
        constexpr const char* arrow_schema_str = "arrow_schema";
        constexpr const char* arrow_array_str = "arrow_array";
        constexpr const char* arrow_array_stream_str = "arrow_array_stream";

        // Capsule destructor for ArrowSchema
        void release_arrow_schema_pycapsule(PyObject* capsule)
        {
            if (capsule == nullptr)
            {
                return;
            }
            auto* schema = static_cast<ArrowSchema*>(PyCapsule_GetPointer(capsule, arrow_schema_str));
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

        // Capsule destructor for ArrowArray
        void release_arrow_array_pycapsule(PyObject* capsule)
        {
            if (capsule == nullptr)
            {
                return;
            }
            auto* array = static_cast<ArrowArray*>(PyCapsule_GetPointer(capsule, arrow_array_str));
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

        // Capsule destructor for ArrowArrayStream
        void release_arrow_array_stream_pycapsule(PyObject* capsule)
        {
            if (capsule == nullptr)
            {
                return;
            }
            auto* stream = static_cast<ArrowArrayStream*>(
                PyCapsule_GetPointer(capsule, arrow_array_stream_str)
            );
            if (stream == nullptr)
            {
                return;
            }
            if (stream->release != nullptr)
            {
                stream->release(stream);
            }
            delete stream;
        }
    }

    array import_array_from_capsules(PyObject* schema_capsule, PyObject* array_capsule)
    {
        // Get the raw pointers from the capsules
        ArrowSchema* schema = static_cast<ArrowSchema*>(
            PyCapsule_GetPointer(schema_capsule, arrow_schema_str)
        );
        if (schema == nullptr)
        {
            // Error already set by PyCapsule_GetPointer
            return array{};
        }

        ArrowArray* arr = static_cast<ArrowArray*>(
            PyCapsule_GetPointer(array_capsule, arrow_array_str)
        );
        if (arr == nullptr)
        {
            // Error already set by PyCapsule_GetPointer
            return array{};
        }

        // Move the data from the capsule structures
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
        auto* schema_ptr = new ArrowSchema(arrow_schema);
        auto* array_ptr = new ArrowArray(arrow_array);

        PyObject* schema_capsule = PyCapsule_New(
            schema_ptr,
            arrow_schema_str,
            release_arrow_schema_pycapsule
        );

        if (schema_capsule == nullptr)
        {
            if (schema_ptr->release != nullptr)
            {
                schema_ptr->release(schema_ptr);
            }
            delete schema_ptr;
            if (array_ptr->release != nullptr)
            {
                array_ptr->release(array_ptr);
            }
            delete array_ptr;
            return {nullptr, nullptr};
        }

        PyObject* array_capsule = PyCapsule_New(
            array_ptr,
            arrow_array_str,
            release_arrow_array_pycapsule
        );

        if (array_capsule == nullptr)
        {
            Py_DECREF(schema_capsule);
            if (array_ptr->release != nullptr)
            {
                array_ptr->release(array_ptr);
            }
            delete array_ptr;
            return {nullptr, nullptr};
        }

        return {schema_capsule, array_capsule};
    }

    PyObject* export_schema_to_capsule(const array& arr)
    {
        // Get pointer to the schema (does not move ownership)
        const ArrowSchema* schema = sparrow::get_arrow_schema(arr);
        
        // Allocate and copy the schema
        auto* schema_ptr = new ArrowSchema();
        sparrow::copy_schema(*schema, *schema_ptr);

        PyObject* capsule = PyCapsule_New(
            schema_ptr,
            arrow_schema_str,
            release_arrow_schema_pycapsule
        );

        if (capsule == nullptr)
        {
            if (schema_ptr->release != nullptr)
            {
                schema_ptr->release(schema_ptr);
            }
            delete schema_ptr;
            return nullptr;
        }

        return capsule;
    }

    PyObject* export_array_to_stream_capsule(array& arr)
    {
        // Extract Arrow structures from the array
        auto [arrow_arr, arrow_sch] = sparrow::extract_arrow_structures(std::move(arr));

        // Create a new stream on the heap
        auto* stream_ptr = new ArrowArrayStream();
        sparrow::fill_arrow_array_stream(*stream_ptr);

        // Get the private data and set the schema and array
        auto* private_data = static_cast<sparrow::arrow_array_stream_private_data*>(stream_ptr->private_data);
        
        // Copy schema into the stream
        sparrow::schema_unique_ptr schema_ptr(new ArrowSchema(), sparrow::arrow_schema_deleter{});
        sparrow::copy_schema(arrow_sch, *schema_ptr);
        private_data->import_schema(std::move(schema_ptr));
        
        // Import the array
        sparrow::array_unique_ptr array_ptr(new ArrowArray(arrow_arr), sparrow::arrow_array_deleter{});
        // Clear the source so it doesn't get double-released
        arrow_arr = ArrowArray{};
        private_data->import_array(std::move(array_ptr));
        
        // Release our copy of the schema since the stream now owns a copy
        if (arrow_sch.release != nullptr)
        {
            arrow_sch.release(&arrow_sch);
        }

        PyObject* capsule = PyCapsule_New(
            stream_ptr,
            arrow_array_stream_str,
            release_arrow_array_stream_pycapsule
        );

        if (capsule == nullptr)
        {
            if (stream_ptr->release != nullptr)
            {
                stream_ptr->release(stream_ptr);
            }
            delete stream_ptr;
            return nullptr;
        }

        return capsule;
    }

    PyObject* export_arrays_to_stream_capsule(std::vector<array>& arrays)
    {
        if (arrays.empty())
        {
            PyErr_SetString(PyExc_ValueError, "Cannot create stream from empty array list");
            return nullptr;
        }

        // Create a new stream on the heap
        auto* stream_ptr = new ArrowArrayStream();
        sparrow::fill_arrow_array_stream(*stream_ptr);

        // Get the private data
        auto* private_data = static_cast<sparrow::arrow_array_stream_private_data*>(stream_ptr->private_data);
        
        bool schema_set = false;
        
        for (auto& arr : arrays)
        {
            // Extract Arrow structures from the array
            auto [arrow_arr, arrow_sch] = sparrow::extract_arrow_structures(std::move(arr));
            
            // Set schema from first array
            if (!schema_set)
            {
                sparrow::schema_unique_ptr schema_ptr(new ArrowSchema(), sparrow::arrow_schema_deleter{});
                sparrow::copy_schema(arrow_sch, *schema_ptr);
                private_data->import_schema(std::move(schema_ptr));
                schema_set = true;
            }
            
            // Import the array
            sparrow::array_unique_ptr array_ptr(new ArrowArray(arrow_arr), sparrow::arrow_array_deleter{});
            // Clear the source so it doesn't get double-released
            arrow_arr = ArrowArray{};
            private_data->import_array(std::move(array_ptr));
            
            // Release our copy of the schema
            if (arrow_sch.release != nullptr)
            {
                arrow_sch.release(&arrow_sch);
            }
        }

        PyObject* capsule = PyCapsule_New(
            stream_ptr,
            arrow_array_stream_str,
            release_arrow_array_stream_pycapsule
        );

        if (capsule == nullptr)
        {
            if (stream_ptr->release != nullptr)
            {
                stream_ptr->release(stream_ptr);
            }
            delete stream_ptr;
            return nullptr;
        }

        return capsule;
    }

    std::vector<array> import_arrays_from_stream_capsule(PyObject* stream_capsule)
    {
        std::vector<array> result;

        // Get the stream pointer from the capsule
        auto* stream = static_cast<ArrowArrayStream*>(
            PyCapsule_GetPointer(stream_capsule, arrow_array_stream_str)
        );
        if (stream == nullptr)
        {
            // Error already set by PyCapsule_GetPointer
            return result;
        }

        // Create a proxy from the stream pointer (does not take ownership)
        sparrow::arrow_array_stream_proxy proxy(stream);

        // Pop all arrays from the stream
        while (auto arr_opt = proxy.pop())
        {
            result.push_back(std::move(*arr_opt));
        }

        // Mark the stream as consumed by setting release to null
        // This prevents the capsule destructor from releasing already-consumed data
        stream->release = nullptr;

        return result;
    }

    array import_array_from_stream_capsule(PyObject* stream_capsule)
    {
        auto arrays = import_arrays_from_stream_capsule(stream_capsule);
        if (arrays.empty())
        {
            return array{};
        }
        return std::move(arrays[0]);
    }
}