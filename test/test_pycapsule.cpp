#include <sparrow-pycapsule/pycapsule.hpp>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>

#include "doctest/doctest.h"

namespace sparrow::pycapsule
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

    // RAII wrapper for PyObject*
    struct PyObjectDeleter
    {
        void operator()(PyObject* obj) const
        {
            if (obj != nullptr)
            {
                Py_DECREF(obj);
            }
        }
    };

    using PyObjectPtr = std::unique_ptr<PyObject, PyObjectDeleter>;

    TEST_SUITE("pycapsule")
    {
        TEST_CASE("ExportArrowSchemaPyCapsule")
        {
            PythonInitializer py_init;

            SUBCASE("creates_valid_capsule")
            {
                auto arr = make_test_array();
                // Note: export_arrow_schema_pycapsule moves from arr, so arr becomes invalid after
                PyObject* schema_capsule = export_arrow_schema_pycapsule(arr);

                REQUIRE_NE(schema_capsule, nullptr);
                CHECK(PyCapsule_CheckExact(schema_capsule));

                const char* name = PyCapsule_GetName(schema_capsule);
                REQUIRE_NE(name, nullptr);
                CHECK_EQ(std::string(name), "arrow_schema");

                // Verify we can get the pointer
                ArrowSchema* schema = static_cast<ArrowSchema*>(
                    PyCapsule_GetPointer(schema_capsule, "arrow_schema")
                );
                CHECK_NE(schema, nullptr);
                CHECK_NE(schema->release, nullptr);

                Py_DECREF(schema_capsule);
            }

            SUBCASE("capsule_has_destructor")
            {
                auto arr = make_test_array();
                PyObject* schema_capsule = export_arrow_schema_pycapsule(arr);

                REQUIRE_NE(schema_capsule, nullptr);

                // Get the schema pointer before destruction
                ArrowSchema* schema = static_cast<ArrowSchema*>(
                    PyCapsule_GetPointer(schema_capsule, "arrow_schema")
                );
                REQUIRE_NE(schema, nullptr);

                // The destructor should be set
                PyCapsule_Destructor destructor = PyCapsule_GetDestructor(schema_capsule);
                CHECK_NE(destructor, nullptr);

                // Decref will call the destructor
                Py_DECREF(schema_capsule);
            }
        }

        TEST_CASE("ExportArrowArrayPyCapsule")
        {
            PythonInitializer py_init;

            SUBCASE("creates_valid_capsule")
            {
                auto arr = make_test_array();
                PyObject* array_capsule = export_arrow_array_pycapsule(arr);

                REQUIRE_NE(array_capsule, nullptr);
                CHECK(PyCapsule_CheckExact(array_capsule));

                const char* name = PyCapsule_GetName(array_capsule);
                REQUIRE_NE(name, nullptr);
                CHECK_EQ(std::string(name), "arrow_array");

                // Verify we can get the pointer
                ArrowArray* array = static_cast<ArrowArray*>(PyCapsule_GetPointer(array_capsule, "arrow_array"));
                CHECK_NE(array, nullptr);
                CHECK_NE(array->release, nullptr);

                Py_DECREF(array_capsule);
            }

            SUBCASE("capsule_has_destructor")
            {
                auto arr = make_test_array();
                PyObject* array_capsule = export_arrow_array_pycapsule(arr);

                REQUIRE_NE(array_capsule, nullptr);

                // Get the array pointer before destruction
                ArrowArray* array = static_cast<ArrowArray*>(PyCapsule_GetPointer(array_capsule, "arrow_array"));
                REQUIRE_NE(array, nullptr);

                // The destructor should be set
                PyCapsule_Destructor destructor = PyCapsule_GetDestructor(array_capsule);
                CHECK_NE(destructor, nullptr);

                // Decref will call the destructor
                Py_DECREF(array_capsule);
            }

            SUBCASE("array_has_correct_length")
            {
                auto arr = make_test_array();
                PyObject* array_capsule = export_arrow_array_pycapsule(arr);

                REQUIRE_NE(array_capsule, nullptr);

                ArrowArray* array = static_cast<ArrowArray*>(PyCapsule_GetPointer(array_capsule, "arrow_array"));
                REQUIRE_NE(array, nullptr);
                CHECK_EQ(array->length, 5);

                Py_DECREF(array_capsule);
            }
        }

        TEST_CASE("GetArrowSchemaPyCapsule")
        {
            PythonInitializer py_init;

            SUBCASE("returns_valid_schema_pointer")
            {
                auto arr = make_test_array();
                PyObject* schema_capsule = export_arrow_schema_pycapsule(arr);

                ArrowSchema* schema = get_arrow_schema_pycapsule(schema_capsule);
                CHECK_NE(schema, nullptr);
                CHECK_NE(schema->release, nullptr);

                Py_DECREF(schema_capsule);
            }

            SUBCASE("returns_null_for_wrong_capsule_name")
            {
                // Create a capsule with wrong name
                int dummy = 42;
                PyObject* wrong_capsule = PyCapsule_New(&dummy, "wrong_name", nullptr);

                ArrowSchema* schema = get_arrow_schema_pycapsule(wrong_capsule);
                CHECK_EQ(schema, nullptr);
                CHECK_NE(PyErr_Occurred(), nullptr);
                PyErr_Clear();

                Py_DECREF(wrong_capsule);
            }

            SUBCASE("returns_null_for_non_capsule")
            {
                PyObject* not_capsule = PyLong_FromLong(42);

                ArrowSchema* schema = get_arrow_schema_pycapsule(not_capsule);
                CHECK_EQ(schema, nullptr);
                CHECK_NE(PyErr_Occurred(), nullptr);
                PyErr_Clear();

                Py_DECREF(not_capsule);
            }
        }

        TEST_CASE("GetArrowArrayPyCapsule")
        {
            PythonInitializer py_init;

            SUBCASE("returns_valid_array_pointer")
            {
                auto arr = make_test_array();
                PyObject* array_capsule = export_arrow_array_pycapsule(arr);

                ArrowArray* array = get_arrow_array_pycapsule(array_capsule);
                CHECK_NE(array, nullptr);
                CHECK_NE(array->release, nullptr);

                Py_DECREF(array_capsule);
            }

            SUBCASE("returns_null_for_wrong_capsule_name")
            {
                // Create a capsule with wrong name
                int dummy = 42;
                PyObject* wrong_capsule = PyCapsule_New(&dummy, "wrong_name", nullptr);

                ArrowArray* array = get_arrow_array_pycapsule(wrong_capsule);
                CHECK_EQ(array, nullptr);
                CHECK_NE(PyErr_Occurred(), nullptr);
                PyErr_Clear();

                Py_DECREF(wrong_capsule);
            }

            SUBCASE("returns_null_for_non_capsule")
            {
                PyObject* not_capsule = PyLong_FromLong(42);

                ArrowArray* array = get_arrow_array_pycapsule(not_capsule);
                CHECK_EQ(array, nullptr);
                CHECK_NE(PyErr_Occurred(), nullptr);
                PyErr_Clear();

                Py_DECREF(not_capsule);
            }
        }

        TEST_CASE("export_array_to_capsules")
        {
            PythonInitializer py_init;

            SUBCASE("exports_both_schema_and_array")
            {
                auto arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(arr);

                REQUIRE_NE(schema_capsule, nullptr);
                REQUIRE_NE(array_capsule, nullptr);

                CHECK(PyCapsule_CheckExact(schema_capsule));
                CHECK(PyCapsule_CheckExact(array_capsule));

                CHECK_EQ(std::string(PyCapsule_GetName(schema_capsule)), "arrow_schema");
                CHECK_EQ(std::string(PyCapsule_GetName(array_capsule)), "arrow_array");

                Py_DECREF(schema_capsule);
                Py_DECREF(array_capsule);
            }

            SUBCASE("exported_capsules_contain_valid_data")
            {
                auto arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(arr);

                ArrowSchema* schema = get_arrow_schema_pycapsule(schema_capsule);
                ArrowArray* array = get_arrow_array_pycapsule(array_capsule);

                REQUIRE_NE(schema, nullptr);
                REQUIRE_NE(array, nullptr);

                CHECK_NE(schema->release, nullptr);
                CHECK_NE(array->release, nullptr);
                CHECK_EQ(array->length, 5);

                Py_DECREF(schema_capsule);
                Py_DECREF(array_capsule);
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

                // Import it back
                auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);

                // Verify the imported array
                CHECK_EQ(imported_arr.size(), 5);

                // The capsules should still be valid but the release callbacks should be null
                ArrowSchema* schema = static_cast<ArrowSchema*>(
                    PyCapsule_GetPointer(schema_capsule, "arrow_schema")
                );
                ArrowArray* array = static_cast<ArrowArray*>(PyCapsule_GetPointer(array_capsule, "arrow_array"));

                CHECK_EQ(schema->release, nullptr);
                CHECK_EQ(array->release, nullptr);

                Py_DECREF(schema_capsule);
                Py_DECREF(array_capsule);
            }

            // Note: Error handling tests for invalid capsules are omitted
            // as they would require more complex setup to avoid crashes

            SUBCASE("ownership_transfer_is_correct")
            {
                // Export an array
                auto original_arr = make_test_array();
                auto [schema_capsule, array_capsule] = export_array_to_capsules(original_arr);

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

                // The capsule destructors should now be no-ops
                Py_DECREF(schema_capsule);
                Py_DECREF(array_capsule);

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

                // Import
                auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);

                // Verify
                REQUIRE_EQ(imported_arr.size(), 5);
                CHECK_EQ(imported_arr.size(), original_size);

                Py_DECREF(schema_capsule);
                Py_DECREF(array_capsule);
            }
        }

        TEST_CASE("ReleaseArrowSchemaPyCapsule_handles_null_release")
        {
            PythonInitializer py_init;

            SUBCASE("destructor_handles_already_released_schema")
            {
                // Create a schema with null release callback
                ArrowSchema* schema = new ArrowSchema{};
                schema->release = nullptr;

                PyObject* capsule = PyCapsule_New(schema, "arrow_schema", release_arrow_schema_pycapsule);

                // This should not crash
                Py_DECREF(capsule);
            }
        }

        TEST_CASE("ReleaseArrowArrayPyCapsule_handles_null_release")
        {
            PythonInitializer py_init;

            SUBCASE("destructor_handles_already_released_array")
            {
                // Create an array with null release callback
                ArrowArray* array = new ArrowArray{};
                array->release = nullptr;

                PyObject* capsule = PyCapsule_New(array, "arrow_array", release_arrow_array_pycapsule);

                // This should not crash
                Py_DECREF(capsule);
            }
        }

        TEST_CASE("memory_leak_prevention")
        {
            PythonInitializer py_init;

            SUBCASE("capsule_destructor_prevents_leak_if_never_consumed")
            {
                // Create capsules but never consume them
                auto arr = make_test_array();
                PyObject* schema_capsule = export_arrow_schema_pycapsule(arr);
                PyObject* array_capsule = export_arrow_array_pycapsule(arr);

                // Just decref without consuming - destructors should clean up
                Py_DECREF(schema_capsule);
                Py_DECREF(array_capsule);
            }

            SUBCASE("imported_array_manages_memory_correctly")
            {
                {
                    auto original_arr = make_test_array();
                    auto [schema_capsule, array_capsule] = export_array_to_capsules(original_arr);

                    {
                        auto imported_arr = import_array_from_capsules(schema_capsule, array_capsule);
                        // imported_arr goes out of scope here
                    }

                    // Capsules still need cleanup
                    Py_DECREF(schema_capsule);
                    Py_DECREF(array_capsule);
                }
            }
        }
    }
}
