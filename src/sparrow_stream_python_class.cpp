/**
 * @file sparrow_stream_python_class.cpp
 * @brief Implementation of the SparrowStream class.
 */

#include <sparrow-rockfinch/sparrow_stream_python_class.hpp>
#include <sparrow-rockfinch/pycapsule.hpp>

namespace sparrow::rockfinch
{
    SparrowStream::SparrowStream(sparrow::array&& arr)
        : m_stream_proxy(create_stream_proxy_from_array(std::move(arr)))
    {
    }

    SparrowStream::SparrowStream(sparrow::arrow_array_stream_proxy&& proxy)
        : m_stream_proxy(std::move(proxy))
    {
    }

    PyObject* SparrowStream::export_to_capsule()
    {
        if (m_consumed)
        {
            PyErr_SetString(PyExc_RuntimeError, "Stream has already been consumed");
            return nullptr;
        }

        PyObject* capsule = sparrow::rockfinch::export_stream_proxy_to_capsule(m_stream_proxy);
        if (capsule != nullptr)
        {
            m_consumed = true;
        }
        return capsule;
    }

    bool SparrowStream::is_consumed() const
    {
        return m_consumed;
    }

}  // namespace sparrow::rockfinch
