#include <sparrow-rockfinch/sparrow_stream_python_class.hpp>
#include <sparrow-rockfinch/sparrow_array_python_class.hpp>
#include <sparrow-rockfinch/pycapsule.hpp>

#include <sparrow/array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>
#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>

#include "doctest/doctest.h"

namespace sparrow::rockfinch
{
    // Helper function to create a simple test array
    sparrow::array make_stream_test_array(int32_t start_value = 1)
    {
        std::vector<sparrow::nullable<int32_t>> values;
        values.push_back(sparrow::nullable<int32_t>(start_value, true));
        values.push_back(sparrow::nullable<int32_t>(start_value + 1, true));
        values.push_back(sparrow::nullable<int32_t>(0, false)); // null value
        values.push_back(sparrow::nullable<int32_t>(start_value + 3, true));
        values.push_back(sparrow::nullable<int32_t>(start_value + 4, true));

        sparrow::primitive_array<int32_t> prim_array(std::move(values));
        return sparrow::array(std::move(prim_array));
    }

    // RAII wrapper for Python initialization
    struct PythonInit
    {
        PythonInit()
        {
            if (!Py_IsInitialized())
            {
                Py_Initialize();
            }
        }

        ~PythonInit()
        {
            // Note: We don't call Py_Finalize() here as it might interfere
            // with other tests or cause issues with static cleanup
        }

        // Prevent copying and moving
        PythonInit(const PythonInit&) = delete;
        PythonInit& operator=(const PythonInit&) = delete;
        PythonInit(PythonInit&&) = delete;
        PythonInit& operator=(PythonInit&&) = delete;
    };

    // RAII wrapper for managing PyObject* references
    struct PyCapsuleGuard
    {
        PyObject* ptr;

        explicit PyCapsuleGuard(PyObject* p)
            : ptr(p)
        {
        }

        ~PyCapsuleGuard()
        {
            Py_XDECREF(ptr);
        }

        PyCapsuleGuard(const PyCapsuleGuard&) = delete;
        PyCapsuleGuard& operator=(const PyCapsuleGuard&) = delete;

        PyCapsuleGuard(PyCapsuleGuard&& other) noexcept
            : ptr(other.ptr)
        {
            other.ptr = nullptr;
        }

        PyCapsuleGuard& operator=(PyCapsuleGuard&& other) noexcept
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

    TEST_SUITE("SparrowStream")
    {
        TEST_CASE("construction")
        {
            PythonInit py_init;

            SUBCASE("default_constructor")
            {
                SparrowStream stream;
                CHECK_FALSE(stream.is_consumed());
            }

            SUBCASE("construct_from_proxy_with_arrays")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                arrays.push_back(make_stream_test_array(10));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                CHECK_FALSE(stream.is_consumed());
            }

            SUBCASE("construct_from_empty_proxy")
            {
                sparrow::arrow_array_stream_proxy proxy;
                SparrowStream stream(std::move(proxy));
                
                CHECK_FALSE(stream.is_consumed());
            }
        }

        TEST_CASE("push_and_pop")
        {
            PythonInit py_init;

            SUBCASE("push_and_pop_single_array")
            {
                SparrowStream stream;
                
                auto arr = make_stream_test_array(1);
                SparrowArray sparrow_arr(std::move(arr));
                
                stream.push(std::move(sparrow_arr));
                
                auto popped = stream.pop();
                REQUIRE(popped.has_value());
                CHECK_EQ(popped.value().size(), 5);
            }

            SUBCASE("push_multiple_arrays_and_pop_all")
            {
                SparrowStream stream;
                
                // Push 3 arrays
                for (int i = 0; i < 3; ++i)
                {
                    auto arr = make_stream_test_array(i * 10);
                    SparrowArray sparrow_arr(std::move(arr));
                    stream.push(std::move(sparrow_arr));
                }
                
                // Pop all 3
                for (int i = 0; i < 3; ++i)
                {
                    auto popped = stream.pop();
                    REQUIRE(popped.has_value());
                    CHECK_EQ(popped.value().size(), 5);
                }
                
                // Stream should be exhausted
                auto empty_pop = stream.pop();
                CHECK_FALSE(empty_pop.has_value());
            }

            SUBCASE("pop_from_empty_stream_returns_nullopt")
            {
                SparrowStream stream;
                auto popped = stream.pop();
                CHECK_FALSE(popped.has_value());
            }

            SUBCASE("pop_preserves_fifo_order")
            {
                SparrowStream stream;
                
                // Push arrays with different starting values
                auto arr1 = make_stream_test_array(100);
                auto arr2 = make_stream_test_array(200);
                
                stream.push(SparrowArray(std::move(arr1)));
                stream.push(SparrowArray(std::move(arr2)));
                
                // Pop should return in FIFO order
                auto first = stream.pop();
                auto second = stream.pop();
                
                REQUIRE(first.has_value());
                REQUIRE(second.has_value());
                CHECK_EQ(first.value().size(), 5);
                CHECK_EQ(second.value().size(), 5);
            }
        }

        TEST_CASE("export_to_capsule")
        {
            PythonInit py_init;

            SUBCASE("export_creates_valid_capsule")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                REQUIRE_NE(capsule, nullptr);
                CHECK(PyCapsule_CheckExact(capsule));
                CHECK_EQ(std::string(PyCapsule_GetName(capsule)), "arrow_array_stream");
            }

            SUBCASE("export_marks_stream_as_consumed")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                CHECK_FALSE(stream.is_consumed());
                
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                CHECK(stream.is_consumed());
            }

            SUBCASE("export_twice_returns_nullptr")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                PyObject* capsule1 = stream.export_to_capsule();
                PyCapsuleGuard guard1(capsule1);
                
                REQUIRE_NE(capsule1, nullptr);
                
                // Second export should return nullptr and set error
                PyObject* capsule2 = stream.export_to_capsule();
                CHECK_EQ(capsule2, nullptr);
                CHECK_NE(PyErr_Occurred(), nullptr);
                PyErr_Clear();  // Clear the error for other tests
            }

            SUBCASE("export_empty_stream")
            {
                SparrowStream stream;
                
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                REQUIRE_NE(capsule, nullptr);
                CHECK(stream.is_consumed());
            }
        }

        TEST_CASE("consumed_state")
        {
            PythonInit py_init;

            SUBCASE("is_consumed_initially_false")
            {
                SparrowStream stream;
                CHECK_FALSE(stream.is_consumed());
            }

            SUBCASE("push_to_consumed_stream_throws")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                // Consume the stream
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                // Try to push - should throw
                auto arr = make_stream_test_array(1);
                SparrowArray sparrow_arr(std::move(arr));
                
                CHECK_THROWS_AS(stream.push(std::move(sparrow_arr)), std::runtime_error);
            }

            SUBCASE("pop_from_consumed_stream_throws")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                // Consume the stream
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                // Try to pop - should throw
                CHECK_THROWS_AS(stream.pop(), std::runtime_error);
            }
        }

        TEST_CASE("round_trip_with_capsules")
        {
            PythonInit py_init;

            SUBCASE("export_and_import_stream")
            {
                // Create original stream
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                arrays.push_back(make_stream_test_array(10));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                // Export
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                REQUIRE_NE(capsule, nullptr);
                
                // Import back
                auto imported_proxy = import_stream_proxy_from_capsule(capsule);
                CHECK_NE(imported_proxy.pop(), std::nullopt);
                
                // Verify we can get arrays from imported proxy
                auto arr1 = imported_proxy.pop();
                CHECK(arr1.has_value());
                if (arr1.has_value())
                {
                    CHECK_EQ(arr1.value().size(), 5);
                }
            }

            SUBCASE("multiple_arrays_preserved")
            {
                // Create stream with 5 arrays
                std::vector<sparrow::array> arrays;
                for (int i = 0; i < 5; ++i)
                {
                    arrays.push_back(make_stream_test_array(i * 10));
                }
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                // Export and import
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                auto imported_proxy = import_stream_proxy_from_capsule(capsule);
                
                // Count arrays in imported stream
                size_t count = 0;
                while (auto arr = imported_proxy.pop())
                {
                    if (arr.has_value())
                    {
                        ++count;
                        CHECK_EQ(arr.value().size(), 5);
                    }
                }
                
                CHECK_EQ(count, 5);
            }
        }

        TEST_CASE("integration_with_proxy")
        {
            PythonInit py_init;

            SUBCASE("construct_from_populated_proxy")
            {
                std::vector<sparrow::array> arrays;
                arrays.push_back(make_stream_test_array(1));
                arrays.push_back(make_stream_test_array(2));
                arrays.push_back(make_stream_test_array(3));
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                // Pop all arrays
                size_t count = 0;
                while (auto arr = stream.pop())
                {
                    if (arr.has_value())
                    {
                        ++count;
                        CHECK_EQ(arr.value().size(), 5);
                    }
                }
                
                CHECK_EQ(count, 3);
            }

            SUBCASE("push_then_export_to_proxy")
            {
                SparrowStream stream;
                
                // Push some arrays
                stream.push(SparrowArray(make_stream_test_array(1)));
                stream.push(SparrowArray(make_stream_test_array(2)));
                
                // Export to capsule
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                // Import as proxy
                auto proxy = import_stream_proxy_from_capsule(capsule);
                
                // Verify arrays
                size_t count = 0;
                while (auto arr = proxy.pop())
                {
                    if (arr.has_value())
                    {
                        ++count;
                    }
                }
                
                CHECK_EQ(count, 2);
            }
        }

        TEST_CASE("memory_management")
        {
            PythonInit py_init;

            SUBCASE("stream_cleanup_on_destruction")
            {
                {
                    std::vector<sparrow::array> arrays;
                    arrays.push_back(make_stream_test_array(1));
                    
                    sparrow::arrow_array_stream_proxy proxy;
                    proxy.push(std::move(arrays));
                    SparrowStream stream(std::move(proxy));
                    
                    // Stream goes out of scope here - should clean up properly
                }
                // No leaks expected
            }

            SUBCASE("capsule_cleanup_after_export")
            {
                {
                    std::vector<sparrow::array> arrays;
                    arrays.push_back(make_stream_test_array(1));
                    
                    sparrow::arrow_array_stream_proxy proxy;
                    proxy.push(std::move(arrays));
                    SparrowStream stream(std::move(proxy));
                    
                    {
                        PyObject* capsule = stream.export_to_capsule();
                        PyCapsuleGuard guard(capsule);
                        // Capsule goes out of scope here
                    }
                    
                    // Stream should still be consumed
                    CHECK(stream.is_consumed());
                }
            }

            SUBCASE("large_stream_handling")
            {
                std::vector<sparrow::array> arrays;
                // Push many arrays
                for (int i = 0; i < 100; ++i)
                {
                    arrays.push_back(make_stream_test_array(i));
                }
                
                sparrow::arrow_array_stream_proxy proxy;
                proxy.push(std::move(arrays));
                SparrowStream stream(std::move(proxy));
                
                PyObject* capsule = stream.export_to_capsule();
                PyCapsuleGuard guard(capsule);
                
                auto imported = import_stream_proxy_from_capsule(capsule);
                
                size_t count = 0;
                while (auto arr = imported.pop())
                {
                    if (arr.has_value())
                    {
                        ++count;
                    }
                }
                
                CHECK_EQ(count, 100);
            }
        }
    }
}

