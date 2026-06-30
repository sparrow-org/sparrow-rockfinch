/**
 * @file sparrow_array_module.cpp
 * @brief Nanobind registration and NumPy interop for SparrowArray.
 */

#include "sparrow_array_module.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>

#include <nanobind/ndarray.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/vector.h>

#include <sparrow-rockfinch/pycapsule.hpp>
#include <sparrow-rockfinch/sparrow_array_python_class.hpp>

#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>
#include <sparrow/types/data_type.hpp>

namespace nb = nanobind;

namespace sparrow::rockfinch
{
    namespace
    {
        /**
         * @brief RAII wrapper for Python buffer-protocol access.
         *
         * This helper acquires a ``Py_buffer`` from a Python object in the constructor
         * and guarantees ``PyBuffer_Release()`` in the destructor. It keeps ndarray
         * import code exception-safe by ensuring that temporary buffer views are
         * released even when validation or array construction throws.
         */
        class python_buffer_guard
        {
        public:

            explicit python_buffer_guard(PyObject* object, int flags)
            {
                if (PyObject_GetBuffer(object, &m_view, flags) != 0)
                {
                    throw nb::python_error();
                }
                m_valid = true;
            }

            python_buffer_guard(const python_buffer_guard&) = delete;
            python_buffer_guard& operator=(const python_buffer_guard&) = delete;

            ~python_buffer_guard()
            {
                if (m_valid)
                {
                    PyBuffer_Release(&m_view);
                }
            }

            [[nodiscard]] const Py_buffer& view() const
            {
                return m_view;
            }

        private:

            Py_buffer m_view{};
            bool m_valid = false;
        };

        struct numpy_arrow_array_private_data
        {
            PyObject* owner = nullptr;
            const void** buffers = nullptr;
            bool writable = false;
        };

        struct ndarray_input_info
        {
            std::size_t size;
        };

        void release_numpy_arrow_array(ArrowArray* array)
        {
            if (array == nullptr || array->release == nullptr)
            {
                return;
            }

            auto* private_data = static_cast<numpy_arrow_array_private_data*>(array->private_data);
            if (private_data != nullptr)
            {
                delete[] private_data->buffers;
                Py_XDECREF(private_data->owner);
                delete private_data;
            }

            array->length = 0;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 0;
            array->n_children = 0;
            array->buffers = nullptr;
            array->children = nullptr;
            array->dictionary = nullptr;
            array->private_data = nullptr;
            array->release = nullptr;
        }

        template <typename T>
        constexpr std::string_view arrow_format_for()
        {
            return sparrow::data_type_to_format(
                sparrow::detail::get_data_type_from_array<sparrow::primitive_array<T>>::get()
            );
        }

        template <typename T>
        SparrowArray
        sparrow_array_from_typed_ndarray(const void* data, std::size_t size, const nb::object& owner, bool writable)
        {
            auto buffers = std::make_unique<const void*[]>(2);
            buffers[0] = nullptr;
            buffers[1] = data;

            Py_INCREF(owner.ptr());
            auto private_data = std::make_unique<numpy_arrow_array_private_data>();
            private_data->owner = owner.ptr();
            private_data->buffers = buffers.get();
            private_data->writable = writable;

            ArrowArray arrow_array{};
            arrow_array.length = static_cast<int64_t>(size);
            arrow_array.null_count = 0;
            arrow_array.offset = 0;
            arrow_array.n_buffers = 2;
            arrow_array.n_children = 0;
            arrow_array.buffers = buffers.get();
            arrow_array.children = nullptr;
            arrow_array.dictionary = nullptr;
            arrow_array.private_data = private_data.get();
            arrow_array.release = &release_numpy_arrow_array;

            try
            {
                ArrowSchema arrow_schema = sparrow::
                    make_arrow_schema<std::string_view, std::string_view, std::vector<sparrow::metadata_pair>>(
                        arrow_format_for<T>(),
                        std::string_view{},
                        std::nullopt,
                        std::nullopt,
                        nullptr,
                        std::array<bool, 0>{},
                        nullptr,
                        false
                    );

                SparrowArray result(sparrow::array(std::move(arrow_array), std::move(arrow_schema)));
                result.set_numpy_owner(owner.ptr(), writable);
                buffers.release();
                private_data.release();
                return result;
            }
            catch (...)
            {
                Py_DECREF(owner.ptr());
                throw;
            }
        }

        bool python_objects_equal(nb::handle lhs, nb::handle rhs)
        {
            const int result = PyObject_RichCompareBool(lhs.ptr(), rhs.ptr(), Py_EQ);
            if (result < 0)
            {
                throw nb::python_error();
            }
            return result == 1;
        }

        std::string numpy_dtype_to_string(const nb::object& dtype)
        {
            return {nb::str(dtype).c_str()};
        }

        ndarray_input_info validate_numpy_input(const Py_buffer& buffer)
        {
            if (buffer.ndim != 1)
            {
                throw nb::value_error("SparrowArray.from_ndarray() only supports 1D ndarrays");
            }

            const auto size = static_cast<std::size_t>(buffer.shape[0]);
            const auto itemsize = static_cast<std::size_t>(buffer.itemsize);

            if (size > 1 && buffer.strides != nullptr && buffer.strides[0] != static_cast<Py_ssize_t>(itemsize))
            {
                throw nb::value_error("SparrowArray.from_ndarray() requires a contiguous 1D ndarray");
            }

            return {size};
        }

        bool numpy_dtype_equals(const nb::object& dtype, const char* spec)
        {
            static nb::module_ numpy = nb::module_::import_("numpy");
            return python_objects_equal(dtype, numpy.attr("dtype")(spec));
        }

        template <typename T>
        const char* numpy_dtype_spec()
        {
            if constexpr (std::same_as<T, std::int8_t>) return "int8";
            else if constexpr (std::same_as<T, std::uint8_t>) return "uint8";
            else if constexpr (std::same_as<T, std::int16_t>) return "int16";
            else if constexpr (std::same_as<T, std::uint16_t>) return "uint16";
            else if constexpr (std::same_as<T, std::int32_t>) return "int32";
            else if constexpr (std::same_as<T, std::uint32_t>) return "uint32";
            else if constexpr (std::same_as<T, std::int64_t>) return "int64";
            else if constexpr (std::same_as<T, std::uint64_t>) return "uint64";
            else if constexpr (std::same_as<T, float>) return "float32";
            else if constexpr (std::same_as<T, double>) return "float64";
            else static_assert(!sizeof(T*), "Unsupported NumPy dtype");
        }

        SparrowArray sparrow_array_from_ndarray(const nb::object& array_obj)
        {
            python_buffer_guard buffer_guard(array_obj.ptr(), PyBUF_STRIDED_RO);
            const auto& buffer = buffer_guard.view();
            const auto input_info = validate_numpy_input(buffer);
            nb::object dtype = array_obj.attr("dtype");

            if (numpy_dtype_equals(dtype, "bool"))
            {
                const auto size = input_info.size;
                const auto* data = static_cast<const bool*>(buffer.buf);

                std::vector<bool> values(size);
                std::copy_n(data, size, values.begin());
                sparrow::primitive_array<bool> primitive(std::move(values));
                return SparrowArray(sparrow::array(std::move(primitive)));
            }

            using supported_numeric_types = std::tuple<
                std::int8_t, std::uint8_t,
                std::int16_t, std::uint16_t,
                std::int32_t, std::uint32_t,
                std::int64_t, std::uint64_t,
                float, double
            >;

            std::optional<SparrowArray> result;
            auto try_make = [&]<typename T>() -> bool
            {
                if (!numpy_dtype_equals(dtype, numpy_dtype_spec<T>()))
                {
                    return false;
                }
                result = sparrow_array_from_typed_ndarray<T>(buffer.buf, input_info.size, array_obj, !buffer.readonly);
                return true;
            };

            if (std::apply(
                    [&](auto... types)
                    {
                        return (try_make.template operator()<decltype(types)>() || ...);
                    },
                    supported_numeric_types{}
                ))
            {
                return std::move(*result);
            }

            throw nb::type_error(
                ("Unsupported ndarray dtype for SparrowArray.from_ndarray(): "
                 + std::string(numpy_dtype_to_string(dtype))
                 + ". Supported dtypes are bool, int8/16/32/64, uint8/16/32/64, float32, and float64.")
                    .c_str()
            );
        }

        nb::object mark_numpy_array_readonly(nb::object array)
        {
            array.attr("setflags")(false);
            return array;
        }

        template <typename T>
        nb::object make_numpy_copy(std::size_t size, const std::function<void(T*)>& fill, bool readonly = false)
        {
            std::unique_ptr<T[]> storage = std::make_unique<T[]>(size);
            fill(storage.get());

            auto* raw = storage.release();
            nb::capsule owner(
                raw,
                [](void* ptr) noexcept
                {
                    delete[] static_cast<T*>(ptr);
                }
            );

            const std::array<size_t, 1> shape = {size};
            const std::array<int64_t, 1> strides = {1};

            nb::ndarray<nb::numpy, T, nb::ndim<1>> result(raw, 1, shape.data(), owner, strides.data());
            nb::object array = nb::cast(result, nb::rv_policy::move);
            return readonly ? mark_numpy_array_readonly(std::move(array)) : array;
        }

        template <typename T>
        nb::object
        make_numpy_copy_from_arrow_values(const ArrowArray* arrow_array, std::size_t size, bool readonly)
        {
            return make_numpy_copy<T>(
                size,
                [&](T* out)
                {
                    const auto* src = static_cast<const T*>(arrow_array->buffers[1]) + arrow_array->offset;
                    std::copy_n(src, size, out);
                },
                readonly
            );
        }

        nb::object make_python_memory_view(const void* data, Py_ssize_t len, nb::handle owner, bool readonly)
        {
            Py_buffer view;
            if (PyBuffer_FillInfo(
                    &view,
                    owner.ptr(),
                    const_cast<void*>(data),
                    len,
                    readonly ? 1 : 0,
                    readonly ? PyBUF_CONTIG_RO : PyBUF_CONTIG
                )
                != 0)
            {
                throw nb::python_error();
            }

            PyObject* memory_view = PyMemoryView_FromBuffer(&view);
            PyBuffer_Release(&view);
            if (memory_view == nullptr)
            {
                throw nb::python_error();
            }
            return nb::steal(memory_view);
        }

        template <typename T>
        nb::object make_numpy_view_from_arrow_values(const ArrowArray* arrow_array, std::size_t size, nb::handle owner)
        {
            static nb::module_ numpy = nb::module_::import_("numpy");
            const auto* data = static_cast<const T*>(arrow_array->buffers[1]) + arrow_array->offset;
            nb::object memory_view = make_python_memory_view(
                data,
                static_cast<Py_ssize_t>(size * sizeof(T)),
                owner,
                true
            );
            return numpy.attr("frombuffer")(
                memory_view,
                nb::arg("dtype") = numpy_dtype_spec<T>(),
                nb::arg("count") = size
            );
        }

        template <typename T>
        nb::object make_numpy_from_arrow_buffer(
            const ArrowArray* arrow_array, std::size_t size, nb::handle owner, bool copy
        )
        {
            if (copy)
            {
                return make_numpy_copy_from_arrow_values<T>(arrow_array, size, false);
            }
            return make_numpy_view_from_arrow_values<T>(arrow_array, size, owner);
        }

        sparrow::dynamic_bitset_view<std::uint8_t>
        make_arrow_bitset_view(const void* buffer, std::size_t size, std::size_t offset)
        {
            return {const_cast<std::uint8_t*>(static_cast<const std::uint8_t*>(buffer)), size, offset};
        }

        bool parse_copy_argument(const nb::object& copy_arg)
        {
            if (copy_arg.is_none())
            {
                return false;
            }

            if (!nb::isinstance<nb::bool_>(copy_arg))
            {
                throw nb::type_error("__array__(copy=...) only accepts True, False, or None");
            }

            return nb::cast<bool>(copy_arg);
        }

        void validate_numpy_export_supported(const sparrow::array& array)
        {
            if (array.null_count() == 0)
            {
                return;
            }

            switch (array.data_type())
            {
                case sparrow::data_type::FLOAT:
                case sparrow::data_type::DOUBLE:
                    return;
                case sparrow::data_type::BOOL:
                    throw nb::type_error("SparrowArray.to_numpy() does not support nullable bool arrays");
                case sparrow::data_type::INT8:
                case sparrow::data_type::UINT8:
                case sparrow::data_type::INT16:
                case sparrow::data_type::UINT16:
                case sparrow::data_type::INT32:
                case sparrow::data_type::UINT32:
                case sparrow::data_type::INT64:
                case sparrow::data_type::UINT64:
                    throw nb::type_error("SparrowArray.to_numpy() does not support nullable integer arrays");
                default:
                    return;
            }
        }

        nb::object sparrow_array_to_numpy(SparrowArray& self, bool copy)
        {
            auto owner = nb::find(self);
            if (!owner.is_valid())
            {
                throw nb::type_error("Could not resolve Python owner for SparrowArray");
            }

            auto& array = self.get_array();
            auto* arrow_array = sparrow::get_arrow_array(array);
            validate_numpy_export_supported(array);

            if (!copy && self.numpy_owner() != nullptr)
            {
                return nb::borrow<nb::object>(self.numpy_owner());
            }

            switch (array.data_type())
            {
                case sparrow::data_type::BOOL:
                    return make_numpy_copy<bool>(
                        array.size(),
                        [&](bool* out)
                        {
                            const auto bitset = make_arrow_bitset_view(
                                arrow_array->buffers[1],
                                array.size(),
                                static_cast<std::size_t>(arrow_array->offset)
                            );
                            for (std::size_t i = 0; i < array.size(); ++i)
                            {
                                out[i] = bitset.test(i);
                            }
                        },
                        !copy
                    );
                case sparrow::data_type::INT8:
                    return make_numpy_from_arrow_buffer<std::int8_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::UINT8:
                    return make_numpy_from_arrow_buffer<std::uint8_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::INT16:
                    return make_numpy_from_arrow_buffer<std::int16_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::UINT16:
                    return make_numpy_from_arrow_buffer<std::uint16_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::INT32:
                    return make_numpy_from_arrow_buffer<std::int32_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::UINT32:
                    return make_numpy_from_arrow_buffer<std::uint32_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::INT64:
                    return make_numpy_from_arrow_buffer<std::int64_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::UINT64:
                    return make_numpy_from_arrow_buffer<std::uint64_t>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::FLOAT:
                    return make_numpy_from_arrow_buffer<float>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::DOUBLE:
                    return make_numpy_from_arrow_buffer<double>(arrow_array, array.size(), owner, copy);
                case sparrow::data_type::HALF_FLOAT:
                    throw nb::type_error("SparrowArray.to_numpy() does not yet support float16 arrays");
                default:
                    throw nb::type_error("SparrowArray.to_numpy() only supports primitive 1D Sparrow arrays");
            }
        }

        nb::object sparrow_array_dunder_array(SparrowArray& self, nb::object dtype, nb::object copy)
        {
            nb::object result = sparrow_array_to_numpy(self, parse_copy_argument(copy));

            if (!dtype.is_none())
            {
                nb::module_ numpy = nb::module_::import_("numpy");
                nb::object requested_dtype = numpy.attr("dtype")(dtype);
                nb::object native_dtype = result.attr("dtype");

                if (!python_objects_equal(requested_dtype, native_dtype))
                {
                    throw nb::type_error("SparrowArray.__array__() does not support dtype conversion requests");
                }
            }

            return result;
        }

        SparrowArray sparrow_array_from_arrow(const nb::object& arrow_array)
        {
            if (!nb::hasattr(arrow_array, "__arrow_c_array__"))
            {
                throw nb::type_error("Input object must implement __arrow_c_array__ (ArrowArrayExportable protocol)");
            }

            nb::object capsules = arrow_array.attr("__arrow_c_array__")();

            if (!nb::isinstance<nb::tuple>(capsules) || nb::len(capsules) != 2)
            {
                throw nb::type_error("__arrow_c_array__ must return a tuple of 2 elements");
            }

            auto capsule_tuple = nb::cast<nb::tuple>(capsules);
            return {capsule_tuple[0].ptr(), capsule_tuple[1].ptr()};
        }

        nb::tuple sparrow_array_to_arrow(const SparrowArray& self, nb::object /*requested_schema*/)
        {
            auto [schema, array] = self.export_to_capsules();
            return nb::make_tuple(nb::steal(schema), nb::steal(array));
        }

        nb::object sparrow_array_to_schema(const SparrowArray& self)
        {
            PyObject* capsule = self.export_schema_to_capsule();
            if (capsule == nullptr)
            {
                throw nb::python_error();
            }
            return nb::steal(capsule);
        }
    }

    void register_sparrow_array(nb::module_& m)
    {
        nb::class_<SparrowArray>(
            m,
            "SparrowArray",
            "SparrowArray - Arrow array wrapper implementing the Arrow PyCapsule Interface.\n\n"
            "This class wraps a sparrow array and implements:\n"
            "- __arrow_c_array__: Export as ArrowArray + ArrowSchema\n"
            "- __arrow_c_schema__: Export schema only\n\n"
            "This allows direct integration with libraries like Polars, PyArrow, and NumPy.\n\n"
            "Example\n"
            "-------\n"
            ">>> import pyarrow as pa\n"
            ">>> import sparrow_rockfinch as sp\n"
            ">>> pa_array = pa.array([1, 2, None, 4])\n"
            ">>> sparrow_array = sp.SparrowArray.from_arrow(pa_array)"
        )
            .def_static("from_arrow", &sparrow_array_from_arrow, nb::arg("arrow_array"),
                "Create a SparrowArray from an Arrow-compatible object.\n\n"
                "Parameters\n"
                "----------\n"
                "arrow_array : ArrowArrayExportable\n"
                "    An object implementing __arrow_c_array__ (e.g., PyArrow array).\n\n"
                "Returns\n"
                "-------\n"
                "SparrowArray\n"
                "    A new SparrowArray wrapping the input data.")
            .def_static("from_ndarray", &sparrow_array_from_ndarray, nb::arg("array"),
                "Create a SparrowArray from a 1D contiguous NumPy ndarray.\n\n"
                "Supported dtypes are bool, int8/16/32/64, uint8/16/32/64,\n"
                "float32, and float64.\n\n"
                "Parameters\n"
                "----------\n"
                "array : numpy.ndarray\n"
                "    A contiguous 1D ndarray on CPU memory.\n\n"
                "Returns\n"
                "-------\n"
                "SparrowArray\n"
                "    A new SparrowArray containing the ndarray data.")
            .def("__arrow_c_array__", &sparrow_array_to_arrow, nb::arg("requested_schema") = nb::none(),
                "Export the array via the Arrow PyCapsule interface.\n\n"
                "Parameters\n"
                "----------\n"
                "requested_schema : object, optional\n"
                "    A PyCapsule containing an ArrowSchema for requested format.\n"
                "    Currently ignored (best-effort conversion not implemented).\n\n"
                "Returns\n"
                "-------\n"
                "tuple[object, object]\n"
                "    A tuple of (schema_capsule, array_capsule).")
            .def("__arrow_c_schema__", &sparrow_array_to_schema,
                "Export the array's schema via the Arrow PyCapsule interface.\n\n"
                "Returns\n"
                "-------\n"
                "object\n"
                "    A PyCapsule containing an ArrowSchema.")
            .def("to_numpy", &sparrow_array_to_numpy, nb::arg("copy") = false,
                "Export the array as a NumPy ndarray.\n\n"
                "Primitive numeric arrays export as zero-copy views when possible.\n"
                "Bool arrays export via copy because Sparrow stores them bit-packed.\n"
                "Nullable float arrays export via copy using NaN sentinels for nulls.\n")
            .def("__array__", &sparrow_array_dunder_array, nb::arg("dtype") = nb::none(), nb::arg("copy") = nb::none(),
                "NumPy array protocol hook.\n\n"
                "This delegates to to_numpy() and rejects dtype coercions that would\n"
                "change the exported representation.")
            .def("size", &SparrowArray::size, "Get the number of elements in the array.")
            .def("__len__", &SparrowArray::size);
    }
}
