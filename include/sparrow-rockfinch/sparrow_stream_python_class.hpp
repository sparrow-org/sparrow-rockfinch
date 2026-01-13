#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sparrow/array.hpp>
#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>

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
         * @brief Construct a SparrowStream from a single sparrow::array.
         *
         * @param arr The sparrow array to wrap (will be moved).
         */
        explicit SparrowStream(sparrow::array&& arr);

        /**
         * @brief Construct a SparrowStream from an arrow_array_stream_proxy.
         *
         * Consumes all arrays from the proxy and stores them internally.
         *
         * @param proxy The stream proxy to consume arrays from (will be moved).
         */
        explicit SparrowStream(sparrow::arrow_array_stream_proxy&& proxy);

        /**
         * @brief Export as a stream via the Arrow PyCapsule interface (__arrow_c_stream__).
         *
         * Each array in the stream is exported as a separate batch.
         *
         * @return A PyCapsule containing an ArrowArrayStream. Caller owns the reference.
         */
        PyObject* export_to_capsule();

        /**
         * @brief Check if the stream has been consumed.
         *
         * @return True if the stream has been exported and consumed.
         */
        [[nodiscard]] bool is_consumed() const;

    private:
        sparrow::arrow_array_stream_proxy m_stream_proxy;
        bool m_consumed = false;
    };

}  // namespace sparrow::rockfinch
