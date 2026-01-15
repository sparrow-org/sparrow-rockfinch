#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sparrow/array.hpp>
#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>
#include "sparrow-rockfinch/sparrow_array_python_class.hpp"

#include "sparrow-rockfinch/config/config.hpp"

namespace sparrow::rockfinch
{
    /**
     * @brief C++ wrapper class for Arrow streams with Python interop.
     *
     * This class wraps sparrow arrays consumed from an arrow_array_stream_proxy
     * and provides methods for the Arrow PyCapsule Interface (ArrowStreamExportable protocol),
     * allowing it to be passed to libraries that expect Arrow streams.
     * 
     * Note: This class is designed to be wrapped by nanobind (or similar)
     * in a Python extension module.
     */
    class SPARROW_ROCKFINCH_API SparrowStream
    {
    public:
        /**
         * Construct a SparrowStream
         */
        SparrowStream() = default;

        /**
         * Construct a SparrowStream from an arrow_array_stream_proxy.
         *
         * Consumes all arrays from the proxy and stores them internally.
         *
         * @param proxy The stream proxy to consume arrays from (will be moved).
         */
        explicit SparrowStream(sparrow::arrow_array_stream_proxy&& proxy);

        /**
         * Push a SparrowArray into the stream.
         *
         * @param arr The SparrowArray to push (will be moved).
         */
        void push(SparrowArray&& arr);

        /**
         * Pop the next SparrowArray from the stream.
         *
         * @return An optional SparrowArray; std::nullopt if the stream is exhausted.
         */
        std::optional<SparrowArray> pop();

        /**
         * Export the stream via the Arrow PyCapsule interface.
         *
         * The stream can only be consumed once. After calling this method,
         * the stream is marked as consumed.
         *
         * @return A PyCapsule containing an ArrowArrayStream.
         */
        PyObject* export_to_capsule();

        /**
         * Check if the SparrowStream has been consumed via export.
         *
         * @return True if the stream has been consumed, False otherwise.
         */
        [[nodiscard]] bool is_consumed() const noexcept;

    private:
        sparrow::arrow_array_stream_proxy m_stream_proxy;
        bool m_consumed = false;
    };

}  // namespace sparrow::rockfinch
