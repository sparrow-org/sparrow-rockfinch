#include <sparrow-rockfinch/pycapsule.hpp>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>
#include <sparrow/c_interface.hpp>
#include <sparrow/c_stream_interface.hpp>

#include "doctest/doctest.h"

namespace sparrow::rockfinch
{
    // Helper function to create a simple test array
    sparrow::array make_test_array()
    {
        std::vector<sparrow::nullable<int32_t>> values = {
            sparrow::make_nullable<int32_t>(1, true),
            sparrow::make_nullable<int32_t>(2, true),
            sparrow::make_nullable<int32_t>(0, false),  // null value
            sparrow::make_nullable<int32_t>(4, true),
            sparrow::make_nullable<int32_t>(5, true)
        };

        sparrow::primitive_array<int32_t> prim_array(std::move(values));
        return sparrow::array(std::move(prim_array));
    }

    // RAII wrapper for Python initialization
    struct PythonInitializer
    {
        PythonInitializer()
        {
            if (!Py_IsInitialized())
            {
                Py_Initialize();
            }
        }

        ~PythonInitializer()
        {
            // Note: We don't call Py_Finalize() here as it might interfere
            // with other tests or cause issues with static cleanup
        }

        // Prevent copying and moving
        PythonInitializer(const PythonInitializer&) = delete;
        PythonInitializer& operator=(const PythonInitializer&) = delete;
        PythonInitializer(PythonInitializer&&) = delete;
        PythonInitializer& operator=(PythonInitializer&&) = delete;
    };

    // RAII wrapper for managing PyObject* references
    struct PyObjectGuard
    {
        PyObject* ptr;

        explicit PyObjectGuard(PyObject* p)
            : ptr(p)
        {
        }

        ~PyObjectGuard()
        {
            Py_XDECREF(ptr);
        }

        PyObjectGuard(const PyObjectGuard&) = delete;
        PyObjectGuard& operator=(const PyObjectGuard&) = delete;

        PyObjectGuard(PyObjectGuard&& other) noexcept
            : ptr(other.ptr)
        {
            other.ptr = nullptr;
        }

        PyObjectGuard& operator=(PyObjectGuard&& other) noexcept
        {
            if (this != &other)
            {
                Py_XDECREF(ptr);
                ptr = other.ptr;
                other.ptr = nullptr;
            }
            return *this;
        }

        PyObject* get() const
        {
            return ptr;
        }

        PyObject* release()
        {
            PyObject* p = ptr;
            ptr = nullptr;
            return p;
        }
    };

    TEST_SUITE("pycapsule")
    {
        TEST_CASE("export_array_to_capsules")
        {
            PythonInitializer py_init;

            SUBCASE("exports_both_schema_and_array")
            {
                auto arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(arr);

                PyObjectGuard schema_guard(schema_capsule);
                PyObjectGuard array_guard(array_capsule);

                REQUIRE_NE(schema_capsule, nullptr);
                REQUIRE_NE(array_capsule, nullptr);

                CHECK(PyCapsule_CheckExact(schema_capsule));
                CHECK(PyCapsule_CheckExact(array_capsule));

                CHECK_EQ(std::string(PyCapsule_GetName(schema_capsule)), "arrow_schema");
                CHECK_EQ(std::string(PyCapsule_GetName(array_capsule)), "arrow_array");
            }

            SUBCASE("exported_capsules_contain_valid_data")
            {
                auto arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(arr);

                PyObjectGuard schema_guard(schema_capsule);
                PyObjectGuard array_guard(array_capsule);

                ArrowSchema* schema = static_cast<ArrowSchema*>(
                    PyCapsule_GetPointer(schema_capsule, "arrow_schema")
                );
                ArrowArray* array = static_cast<ArrowArray*>(
                    PyCapsule_GetPointer(array_capsule, "arrow_array")
                );

                REQUIRE_NE(schema, nullptr);
                REQUIRE_NE(array, nullptr);

                CHECK_NE(schema->release, nullptr);
                CHECK_NE(array->release, nullptr);
                CHECK_EQ(array->length, 5);
            }
        }

        TEST_CASE("import_array_from_capsules")
        {
            PythonInitializer py_init;

            SUBCASE("imports_valid_capsules")
            {
                // Export an array
                auto original_arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(original_arr);

                PyObjectGuard schema_guard(schema_capsule);
                PyObjectGuard array_guard(array_capsule);

                // Import it back
                auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);

                // Verify the imported array
                CHECK_EQ(imported_arr.size(), 5);

                // The capsules should still be valid but the release callbacks should be null
                ArrowSchema* schema = static_cast<ArrowSchema*>(
                    PyCapsule_GetPointer(schema_capsule, "arrow_schema")
                );
                ArrowArray* array = static_cast<ArrowArray*>(
                    PyCapsule_GetPointer(array_capsule, "arrow_array")
                );

                CHECK_EQ(schema->release, nullptr);
                CHECK_EQ(array->release, nullptr);
            }

            SUBCASE("ownership_transfer_is_correct")
            {
                // Export an array
                auto original_arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(original_arr);

                PyObjectGuard schema_guard(schema_capsule);
                PyObjectGuard array_guard(array_capsule);

                // Get pointers before import
                ArrowSchema* schema_before = static_cast<ArrowSchema*>(
                    PyCapsule_GetPointer(schema_capsule, "arrow_schema")
                );
                ArrowArray* array_before = static_cast<ArrowArray*>(
                    PyCapsule_GetPointer(array_capsule, "arrow_array")
                );

                CHECK_NE(schema_before->release, nullptr);
                CHECK_NE(array_before->release, nullptr);

                // Import (transfers ownership)
                auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);

                // After import, release callbacks should be null
                CHECK_EQ(schema_before->release, nullptr);
                CHECK_EQ(array_before->release, nullptr);

                // The imported array should still be valid
                CHECK(imported_arr.size() == 5);
            }
        }

        TEST_CASE("round_trip_export_import")
        {
            PythonInitializer py_init;

            SUBCASE("preserves_array_data")
            {
                // Create original array
                std::vector<sparrow::nullable<int32_t>> values = {
                    sparrow::make_nullable<int32_t>(10, true),
                    sparrow::make_nullable<int32_t>(20, true),
                    sparrow::make_nullable<int32_t>(0, false),
                    sparrow::make_nullable<int32_t>(40, true),
                    sparrow::make_nullable<int32_t>(50, true)
                };

                sparrow::primitive_array<int32_t> prim_array(std::move(values));
                sparrow::array original_arr(std::move(prim_array));

                size_t original_size = original_arr.size();  // Save size before move

                // Export (moves from original_arr)
                auto [schema_capsule, array_capsule] = export_array_to_capsules(original_arr);

                PyObjectGuard schema_guard(schema_capsule);
                PyObjectGuard array_guard(array_capsule);

                // Import
                auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);

                // Verify
                REQUIRE_EQ(imported_arr.size(), 5);
                CHECK_EQ(imported_arr.size(), original_size);
            }
        }

        TEST_CASE("memory_management")
        {
            PythonInitializer py_init;

            SUBCASE("imported_array_manages_memory_correctly")
            {
                {
                    auto original_arr = make_test_array();
                    auto [schema_capsule, array_capsule] = export_array_to_capsules(original_arr);

                    PyObjectGuard schema_guard(schema_capsule);
                    PyObjectGuard array_guard(array_capsule);

                    {
                        auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);
                        // imported_arr goes out of scope here
                    }

                    // Capsules go out of scope and clean up automatically
                }
            }
        }
    }

    TEST_SUITE("pycapsule_stream")
    {
        TEST_CASE("export_array_to_stream_capsule")
        {
            PythonInitializer py_init;

            SUBCASE("exports_valid_stream_capsule")
            {
                auto arr = make_test_array();
                PyObject* stream_capsule = export_array_to_stream_capsule(arr);

                PyObjectGuard stream_guard(stream_capsule);

                REQUIRE_NE(stream_capsule, nullptr);
                CHECK(PyCapsule_CheckExact(stream_capsule));
                CHECK_EQ(std::string(PyCapsule_GetName(stream_capsule)), "arrow_array_stream");
            }

            SUBCASE("stream_capsule_contains_valid_stream")
            {
                auto arr = make_test_array();
                PyObject* stream_capsule = export_array_to_stream_capsule(arr);

                PyObjectGuard stream_guard(stream_capsule);

                ArrowArrayStream* stream = static_cast<ArrowArrayStream*>(
                    PyCapsule_GetPointer(stream_capsule, "arrow_array_stream")
                );

                REQUIRE_NE(stream, nullptr);
                CHECK_NE(stream->release, nullptr);
                CHECK_NE(stream->get_schema, nullptr);
                CHECK_NE(stream->get_next, nullptr);
            }
        }

        TEST_CASE("export_arrays_to_stream_capsule")
        {
            PythonInitializer py_init;

            SUBCASE("exports_multiple_arrays_to_stream")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_test_array());
                arrays.push_back(make_test_array());
                arrays.push_back(make_test_array());

                PyObject* stream_capsule = export_arrays_to_stream_capsule(arrays);

                PyObjectGuard stream_guard(stream_capsule);

                REQUIRE_NE(stream_capsule, nullptr);
                CHECK(PyCapsule_CheckExact(stream_capsule));
                CHECK_EQ(std::string(PyCapsule_GetName(stream_capsule)), "arrow_array_stream");
            }

            SUBCASE("empty_array_list_returns_nullptr")
            {
                std::vector<sparrow::array> arrays;
                PyObject* stream_capsule = export_arrays_to_stream_capsule(arrays);

                // Clear the Python error that was set
                PyErr_Clear();

                CHECK_EQ(stream_capsule, nullptr);
            }
        }

        TEST_CASE("import_arrays_from_stream_capsule")
        {
            PythonInitializer py_init;

            SUBCASE("imports_single_array_from_stream")
            {
                auto arr = make_test_array();
                size_t original_size = arr.size();

                PyObject* stream_capsule = export_array_to_stream_capsule(arr);
                PyObjectGuard stream_guard(stream_capsule);

                auto imported_arrays = import_arrays_from_stream_capsule(stream_capsule);

                REQUIRE_EQ(imported_arrays.size(), 1);
                CHECK_EQ(imported_arrays[0].size(), original_size);
            }

            SUBCASE("imports_multiple_arrays_from_stream")
            {
                std::vector<sparrow::array> original_arrays;
                original_arrays.push_back(make_test_array());
                original_arrays.push_back(make_test_array());

                PyObject* stream_capsule = export_arrays_to_stream_capsule(original_arrays);
                PyObjectGuard stream_guard(stream_capsule);

                auto imported_arrays = import_arrays_from_stream_capsule(stream_capsule);

                REQUIRE_EQ(imported_arrays.size(), 2);
                CHECK_EQ(imported_arrays[0].size(), 5);
                CHECK_EQ(imported_arrays[1].size(), 5);
            }
        }

        TEST_CASE("import_array_from_stream_capsule")
        {
            PythonInitializer py_init;

            SUBCASE("imports_first_array_from_stream")
            {
                auto arr = make_test_array();
                size_t original_size = arr.size();

                PyObject* stream_capsule = export_array_to_stream_capsule(arr);
                PyObjectGuard stream_guard(stream_capsule);

                auto imported_arr = import_array_from_stream_capsule(stream_capsule);

                CHECK_EQ(imported_arr.size(), original_size);
            }
        }

        TEST_CASE("stream_round_trip")
        {
            PythonInitializer py_init;

            SUBCASE("preserves_array_data_through_stream")
            {
                // Create original array with specific values
                std::vector<sparrow::nullable<int32_t>> values = {
                    sparrow::make_nullable<int32_t>(100, true),
                    sparrow::make_nullable<int32_t>(200, true),
                    sparrow::make_nullable<int32_t>(0, false),  // null
                    sparrow::make_nullable<int32_t>(400, true)
                };

                sparrow::primitive_array<int32_t> prim_array(std::move(values));
                sparrow::array original_arr(std::move(prim_array));

                size_t original_size = original_arr.size();

                // Export to stream
                PyObject* stream_capsule = export_array_to_stream_capsule(original_arr);
                PyObjectGuard stream_guard(stream_capsule);

                REQUIRE_NE(stream_capsule, nullptr);

                // Import from stream
                auto imported_arr = import_array_from_stream_capsule(stream_capsule);

                // Verify
                CHECK_EQ(imported_arr.size(), original_size);
            }

            SUBCASE("multiple_arrays_round_trip")
            {
                std::vector<sparrow::array> original_arrays;
                original_arrays.push_back(make_test_array());
                original_arrays.push_back(make_test_array());
                original_arrays.push_back(make_test_array());

                // Export to stream
                PyObject* stream_capsule = export_arrays_to_stream_capsule(original_arrays);
                PyObjectGuard stream_guard(stream_capsule);

                REQUIRE_NE(stream_capsule, nullptr);

                // Import from stream
                auto imported_arrays = import_arrays_from_stream_capsule(stream_capsule);

                // Verify count and sizes
                REQUIRE_EQ(imported_arrays.size(), 3);
                for (const auto& arr : imported_arrays)
                {
                    CHECK_EQ(arr.size(), 5);
                }
            }
        }

        TEST_CASE("stream_memory_management")
        {
            PythonInitializer py_init;

            SUBCASE("stream_cleanup_on_capsule_destruction")
            {
                {
                    auto arr = make_test_array();
                    PyObject* stream_capsule = export_array_to_stream_capsule(arr);
                    PyObjectGuard stream_guard(stream_capsule);

                    // Get stream pointer before destruction
                    ArrowArrayStream* stream = static_cast<ArrowArrayStream*>(
                        PyCapsule_GetPointer(stream_capsule, "arrow_array_stream")
                    );
                    CHECK_NE(stream->release, nullptr);

                    // Stream guard goes out of scope, capsule is released
                }
                // No memory leaks or crashes expected
            }

            SUBCASE("consumed_stream_has_null_release")
            {
                auto arr = make_test_array();
                PyObject* stream_capsule = export_array_to_stream_capsule(arr);
                PyObjectGuard stream_guard(stream_capsule);

                // Get stream pointer
                ArrowArrayStream* stream = static_cast<ArrowArrayStream*>(
                    PyCapsule_GetPointer(stream_capsule, "arrow_array_stream")
                );

                CHECK_NE(stream->release, nullptr);

                // Consume the stream
                auto imported_arrays = import_arrays_from_stream_capsule(stream_capsule);

                // After consumption, release should be null
                CHECK_EQ(stream->release, nullptr);
            }
        }
    }
}
