/**
 * @file sparrow_stream_python_class.cpp
 * @brief Implementation of the SparrowStream class.
 */

#include <sparrow-rockfinch/sparrow_stream_python_class.hpp>
#include <sparrow-rockfinch/pycapsule.hpp>

namespace sparrow::rockfinch
{
    SparrowStream::SparrowStream(sparrow::array&& arr)
    {
        m_arrays.push_back(std::move(arr));
    }

    SparrowStream::SparrowStream(std::vector<sparrow::array>&& arrays)
        : m_arrays(std::move(arrays))
    {
    }

    PyObject* SparrowStream::export_to_capsule()
    {
        if (m_consumed)
        {
            PyErr_SetString(PyExc_RuntimeError, "Stream has already been consumed");
            return nullptr;
        }

        PyObject* capsule = sparrow::rockfinch::export_arrays_to_stream_capsule(m_arrays);
        if (capsule != nullptr)
        {
            m_consumed = true;
        }
        return capsule;
    }

    size_t SparrowStream::batch_count() const
    {
        return m_arrays.size();
    }

    bool SparrowStream::is_consumed() const
    {
        return m_consumed;
    }

}  // namespace sparrow::rockfinch
