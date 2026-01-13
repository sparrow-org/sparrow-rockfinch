#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <vector>

#include <sparrow/array.hpp>

#include "sparrow-rockfinch/config/config.hpp"

namespace sparrow::rockfinch
{
    /**
     * @brief C++ wrapper class for Arrow streams with Python interop.
     *
     * This class wraps one or more sparrow::array objects and provides methods
     * for the Arrow PyCapsule Interface (ArrowStreamExportable protocol),
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
         * @brief Construct a SparrowStream from multiple sparrow::arrays.
         *
         * @param arrays Vector of sparrow arrays to wrap (will be moved).
         */
        explicit SparrowStream(std::vector<sparrow::array>&& arrays);

        /**
         * @brief Export as a stream via the Arrow PyCapsule interface (__arrow_c_stream__).
         *
         * Each array in the stream is exported as a separate batch.
         *
         * @return A PyCapsule containing an ArrowArrayStream. Caller owns the reference.
         */
        PyObject* export_to_capsule();

        /**
         * @brief Get the number of arrays/batches in the stream.
         *
         * @return The number of batches.
         */
        [[nodiscard]] size_t batch_count() const;

        /**
         * @brief Check if the stream has been consumed.
         *
         * @return True if the stream has been exported and consumed.
         */
        [[nodiscard]] bool is_consumed() const;

    private:
        std::vector<sparrow::array> m_arrays;
        bool m_consumed = false;
    };

}  // namespace sparrow::rockfinch
