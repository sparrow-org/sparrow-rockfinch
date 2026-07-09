/**
 * @file sparrow_array_numpy_interop.hpp
 * @brief Internal detail helpers for SparrowArray ↔ NumPy interoperability.
 *
 * This header is **not** part of the public API. It lives in the ``detail``
 * namespace and ``detail/`` directory to prevent accidental inclusion by
 * downstream consumers.  Only the nanobind module sources should include it.
 */

#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>

#include <sparrow-rockfinch/sparrow_array_python_class.hpp>

#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>
#include <sparrow/types/data_type.hpp>

namespace sparrow::rockfinch::detail
{
    namespace nb = nanobind;

    /**
     * @brief RAII wrapper for Python buffer-protocol access.
     *
     * Acquires a ``Py_buffer`` from a Python object in the constructor and
     * guarantees ``PyBuffer_Release()`` in the destructor.  This keeps ndarray
     * import code exception-safe by ensuring that temporary buffer views are
     * released even when validation or array construction throws.
     */
    class python_buffer_guard
    {
    public:
        /**
         * @brief Acquire a buffer view from *object*.
         *
         * @param object  Python object supporting the buffer protocol.
         * @param flags   Flags passed to ``PyObject_GetBuffer`` (e.g. ``PyBUF_STRIDED_RO``).
         *
         * @throws nb::python_error  If ``PyObject_GetBuffer`` fails.
         */
        explicit python_buffer_guard(PyObject* object, int flags);

        python_buffer_guard(const python_buffer_guard&) = delete;
        python_buffer_guard& operator=(const python_buffer_guard&) = delete;
        python_buffer_guard(python_buffer_guard&&) = delete;
        python_buffer_guard& operator=(python_buffer_guard&&) = delete;

        /// Release the held ``Py_buffer``.
        ~python_buffer_guard();

        /**
         * @brief Return a const reference to the held ``Py_buffer``.
         *
         * The reference remains valid until this guard is destroyed.
         */
        [[nodiscard]] const Py_buffer& view() const;

    private:
        Py_buffer m_view{};
        bool m_valid = false;
    };

    /**
     * @brief Private data attached to an ``ArrowArray`` that borrows memory
     *        from a NumPy array.
     *
     * Keeps the owning Python object alive and tracks the buffer pointers and
     * writability flag so that the Arrow release callback can properly clean up.
     */
    struct numpy_arrow_array_private_data
    {
        /// The Python object that owns the backing memory (borrowed reference).
        PyObject* owner = nullptr;

        /// Array of buffer pointers passed to the ``ArrowArray``.
        const void** buffers = nullptr;

        /// Whether the NumPy buffer was writable when borrowed.
        bool writable = false;
    };

    /**
     * @brief Result of validating a NumPy ndarray for import.
     */
    struct ndarray_input_info
    {
        /// Number of elements in the (1-D) array.
        std::size_t size;
    };

    /**
     * @brief Arrow release callback for an ``ArrowArray`` backed by NumPy data.
     *
     * Frees the ``numpy_arrow_array_private_data``, decrements the owning
     * Python object, and nulls out all ``ArrowArray`` fields so the struct
     * cannot be accidentally reused.
     *
     * @param array  The ``ArrowArray`` whose backing NumPy data should be released.
     */
    void release_numpy_arrow_array(ArrowArray* array);

    /**
     * @brief Return the Arrow format string for a primitive C++ type.
     *
     * @tparam T  A primitive numeric type (e.g. ``float``, ``int32_t``).
     * @return    The Arrow format string (e.g. ``"f"``, ``"i"``).
     */
    template <typename T>
    [[nodiscard]] constexpr std::string_view arrow_format_for()
    {
        return sparrow::data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::primitive_array<T>>::get()
        );
    }

    /**
     * @brief Build a ``SparrowArray`` that borrows memory from a typed NumPy buffer.
     *
     * Constructs an ``ArrowArray`` whose second buffer (the value buffer) points
     * directly into *data*.  The returned ``SparrowArray`` keeps *owner* alive
     * so the NumPy array is not garbage-collected while the Arrow array still
     * references its memory.
     *
     * @tparam T         The C++ type corresponding to the ndarray's dtype.
     * @param data       Pointer to the raw values buffer.
     * @param size       Number of elements.
     * @param owner      The Python ndarray object that owns *data*.
     * @param writable   Whether the buffer was writable at borrow time.
     * @return           A ``SparrowArray`` wrapping the borrowed data.
     */
    template <typename T>
    [[nodiscard]] SparrowArray
    sparrow_array_from_typed_ndarray(const void* data, std::size_t size, const nb::object& owner, bool writable)
    {
        auto buffers = std::make_unique<const void*[]>(2);
        buffers[0] = nullptr;        // validity bitmap – none
        buffers[1] = data;           // value buffer

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

    /**
     * @brief Compare two Python objects for equality (``==``).
     *
     * Uses ``PyObject_RichCompareBool`` with ``Py_EQ``.
     *
     * @param lhs  Left-hand Python object.
     * @param rhs  Right-hand Python object.
     * @return     ``true`` if the objects are equal.
     *
     * @throws nb::python_error  If the comparison raises a Python exception.
     */
    [[nodiscard]] bool python_objects_equal(nb::handle lhs, nb::handle rhs);

    /**
     * @brief Convert a NumPy ``dtype`` object to a C++ string.
     *
     * @param dtype  A ``numpy.dtype`` instance.
     * @return       The string representation (e.g. ``"int32"``).
     */
    [[nodiscard]] std::string numpy_dtype_to_string(const nb::object& dtype);

    /**
     * @brief Validate that a ``Py_buffer`` is suitable for SparrowArray import.
     *
     * Checks that the buffer is 1-D, and for arrays with more than one element
     * requires contiguous strides.
     *
     * @param buffer  The ``Py_buffer`` to inspect.
     * @return        An ``ndarray_input_info`` with the validated element count.
     *
     * @throws nb::value_error  If the buffer is not 1-D or not contiguous.
     */
    [[nodiscard]] ndarray_input_info validate_numpy_input(const Py_buffer& buffer);

    /**
     * @brief Check whether a NumPy ``dtype`` matches a given dtype-spec string.
     *
     * @param dtype  A ``numpy.dtype`` instance.
     * @param spec   A NumPy dtype-spec string (e.g. ``"float64"``).
     * @return       ``true`` if the dtypes are equal.
     */
    [[nodiscard]] bool numpy_dtype_equals(const nb::object& dtype, const char* spec);

    /**
     * @brief Return the NumPy dtype-spec string for a C++ numeric type.
     *
     * @tparam T  A primitive numeric type.
     * @return    The dtype string (e.g. ``"float64"`` for ``double``).
     *
     * Produces a compile-time error if ``T`` is not a supported type.
     */
    template <typename T>
    [[nodiscard]] constexpr const char* numpy_dtype_spec() noexcept
    {
        if constexpr (std::same_as<T, std::int8_t>)
        {
            return "int8";
        }
        else if constexpr (std::same_as<T, std::uint8_t>)
        {
            return "uint8";
        }
        else if constexpr (std::same_as<T, std::int16_t>)
        {
            return "int16";
        }
        else if constexpr (std::same_as<T, std::uint16_t>)
        {
            return "uint16";
        }
        else if constexpr (std::same_as<T, std::int32_t>)
        {
            return "int32";
        }
        else if constexpr (std::same_as<T, std::uint32_t>)
        {
            return "uint32";
        }
        else if constexpr (std::same_as<T, std::int64_t>)
        {
            return "int64";
        }
        else if constexpr (std::same_as<T, std::uint64_t>)
        {
            return "uint64";
        }
        else if constexpr (std::same_as<T, float>)
        {
            return "float32";
        }
        else if constexpr (std::same_as<T, double>)
        {
            return "float64";
        }
        else
        {
            static_assert(!sizeof(T*), "Unsupported NumPy dtype");
        }
    }

    /**
     * @brief Import a 1-D contiguous NumPy ndarray into a ``SparrowArray``.
     *
     * Supported dtypes are bool, int8/16/32/64, uint8/16/32/64, float32, and
     * float64.  For bool arrays the data is copied (Sparrow stores bools
     * bit-packed); for numeric types the returned ``SparrowArray`` borrows the
     * ndarray's memory buffer.
     *
     * @param array_obj  A ``numpy.ndarray`` instance.
     * @return           A ``SparrowArray`` containing the ndarray data.
     *
     * @throws nb::value_error  If the array is not 1-D or not contiguous.
     * @throws nb::type_error   If the dtype is not supported.
     */
    [[nodiscard]] SparrowArray sparrow_array_from_ndarray(const nb::object& array_obj);

    /**
     * @brief Mark a NumPy array as read-only (``arr.setflags(write=False)``).
     *
     * @param array  A ``numpy.ndarray``.
     * @return       The same array, now read-only.
     */
    [[nodiscard]] nb::object mark_numpy_array_readonly(nb::object array);

    /**
     * @brief Allocate a new NumPy array and fill it via a callback.
     *
     * @tparam T       The C++ element type.
     * @param size     Number of elements.
     * @param fill     Callback invoked with a ``T*`` to populate the buffer.
     * @param readonly If true, the returned array is marked read-only.
     * @return         A 1-D ``numpy.ndarray`` owning its data.
     */
    template <typename T>
    [[nodiscard]] nb::object
    make_numpy_copy(std::size_t size, const std::function<void(T*)>& fill, bool readonly = false)
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

    /**
     * @brief Copy the value buffer of an ``ArrowArray`` into a new NumPy array.
     *
     * @tparam T           The C++ element type.
     * @param arrow_array  Source ``ArrowArray``.
     * @param size         Number of elements to copy.
     * @param readonly     If true, the returned array is marked read-only.
     * @return             A 1-D ``numpy.ndarray`` owning its data.
     */
    template <typename T>
    [[nodiscard]] nb::object
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

    /**
     * @brief Create a ``memoryview`` object from a raw data pointer.
     *
     * The returned ``memoryview`` keeps *owner* alive so the underlying memory
     * is not freed prematurely.
     *
     * @param data      Pointer to the raw bytes.
     * @param len       Number of bytes.
     * @param owner     Python object that owns the memory.
     * @param readonly  If true, the view is read-only.
     * @return          A ``memoryview`` Python object.
     *
     * @throws nb::python_error  If buffer creation fails.
     */
    [[nodiscard]] nb::object
    make_python_memory_view(const void* data, Py_ssize_t len, nb::handle owner, bool readonly);

    /**
     * @brief Create a zero-copy NumPy view over the value buffer of an
     *        ``ArrowArray``.
     *
     * Builds a ``memoryview`` over the buffer and wraps it with
     * ``numpy.frombuffer``.
     *
     * @tparam T           The C++ element type.
     * @param arrow_array  Source ``ArrowArray``.
     * @param size         Number of elements.
     * @param owner        Python object that keeps the backing memory alive.
     * @return             A 1-D ``numpy.ndarray`` (read-only view).
     */
    template <typename T>
    [[nodiscard]] nb::object
    make_numpy_view_from_arrow_values(const ArrowArray* arrow_array, std::size_t size, nb::handle owner)
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

    /**
     * @brief Export an ``ArrowArray`` value buffer as a NumPy array.
     *
     * When *copy* is ``false`` a zero-copy view is returned; when ``true``
     * a fresh copy is made.
     *
     * @tparam T           The C++ element type.
     * @param arrow_array  Source ``ArrowArray``.
     * @param size         Number of elements.
     * @param owner        Python object that keeps the backing memory alive.
     * @param copy         If true, force a copy; otherwise prefer a view.
     * @return             A 1-D ``numpy.ndarray``.
     */
    template <typename T>
    [[nodiscard]] nb::object
    make_numpy_from_arrow_buffer(const ArrowArray* arrow_array, std::size_t size, nb::handle owner, bool copy)
    {
        if (copy)
        {
            return make_numpy_copy_from_arrow_values<T>(arrow_array, size, false);
        }
        return make_numpy_view_from_arrow_values<T>(arrow_array, size, owner);
    }

    /**
     * @brief Wrap an Arrow validity bitmap buffer as a ``dynamic_bitset_view``.
     *
     * @param buffer  Raw pointer to the bitmap bytes.
     * @param size    Number of bits (elements).
     * @param offset  Bit offset into the bitmap.
     * @return        A ``dynamic_bitset_view<std::uint8_t>`` over the bitmap.
     */
    [[nodiscard]] sparrow::dynamic_bitset_view<std::uint8_t>
    make_arrow_bitset_view(const void* buffer, std::size_t size, std::size_t offset);

    /**
     * @brief Interpret the ``copy`` argument of ``__array__``.
     *
     * @param copy_arg  ``None``, ``True``, or ``False``.
     * @return          ``true`` if copying was requested, ``false`` otherwise.
     *
     * @throws nb::type_error  If *copy_arg* is not a bool or ``None``.
     */
    [[nodiscard]] bool parse_copy_argument(const nb::object& copy_arg);

    /**
     * @brief Verify that a sparrow array can be safely exported to NumPy.
     *
     * Nullable float arrays are allowed (nulls become NaN sentinels after a
     * copy).  Nullable bool and integer arrays are rejected because NumPy has
     * no universal sentinel for those types.
     *
     * @param array  The sparrow array to validate.
     *
     * @throws nb::type_error  If the array is a nullable bool or integer type.
     */
    void validate_numpy_export_supported(const sparrow::array& array);

    /**
     * @brief Convert a ``SparrowArray`` to a ``numpy.ndarray``.
     *
     * Primitive numeric arrays export as zero-copy views when possible.
     * Bool arrays always copy because Sparrow stores them bit-packed.
     * Nullable float arrays export via copy using NaN sentinels.
     *
     * @param self  The ``SparrowArray`` to export.
     * @param copy  If true, always produce a copy.
     * @return      A 1-D ``numpy.ndarray``.
     *
     * @throws nb::type_error  If the array type cannot be exported.
     */
    [[nodiscard]] nb::object sparrow_array_to_numpy(SparrowArray& self, bool copy);

    /**
     * @brief Implementation of ``SparrowArray.__array__`` (NumPy array protocol).
     *
     * Delegates to ``sparrow_array_to_numpy`` and rejects *dtype* coercion
     * requests that would change the exported representation.
     *
     * @param self   The ``SparrowArray`` instance.
     * @param dtype  Requested dtype (``None`` for default).
     * @param copy   ``None``, ``True``, or ``False``.
     * @return       A 1-D ``numpy.ndarray``.
     *
     * @throws nb::type_error  If a dtype conversion is requested.
     */
    [[nodiscard]] nb::object
    sparrow_array_dunder_array(SparrowArray& self, nb::object dtype, nb::object copy);

    /**
     * @brief Create a ``SparrowArray`` from any object implementing
     *        ``__arrow_c_array__``.
     *
     * @param arrow_array  An ``ArrowArrayExportable`` Python object.
     * @return             A ``SparrowArray`` wrapping the imported data.
     *
     * @throws nb::type_error  If the object does not implement the protocol
     *                         or returns malformed capsules.
     */
    [[nodiscard]] SparrowArray sparrow_array_from_arrow(const nb::object& arrow_array);

    /**
     * @brief Implementation of ``SparrowArray.__arrow_c_array__``.
     *
     * Exports the array via the Arrow PyCapsule Interface.
     *
     * @param self              The ``SparrowArray`` instance.
     * @param requested_schema  Ignored (best-effort conversion not implemented).
     * @return                  A tuple ``(schema_capsule, array_capsule)``.
     */
    [[nodiscard]] nb::tuple
    sparrow_array_to_arrow(const SparrowArray& self, nb::object requested_schema);

    /**
     * @brief Implementation of ``SparrowArray.__arrow_c_schema__``.
     *
     * Exports only the schema capsule.
     *
     * @param self  The ``SparrowArray`` instance.
     * @return      A PyCapsule containing an ``ArrowSchema``.
     *
     * @throws nb::python_error  If schema export fails.
     */
    [[nodiscard]] nb::object sparrow_array_to_schema(const SparrowArray& self);

}  // namespace sparrow::rockfinch::detail
