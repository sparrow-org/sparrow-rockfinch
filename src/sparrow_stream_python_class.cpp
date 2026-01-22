/**
 * @file sparrow_stream_python_class.cpp
 * @brief Implementation of the SparrowStream class.
 */

#include <sparrow-rockfinch/pycapsule.hpp>
#include <sparrow-rockfinch/sparrow_stream_python_class.hpp>

namespace sparrow::rockfinch
{
    SparrowStream::SparrowStream(sparrow::arrow_array_stream_proxy&& proxy)
        : m_stream_proxy(std::move(proxy))
    {
    }

    void SparrowStream::push(SparrowArray&& arr)
    {
        if (m_consumed)
        {
            throw std::runtime_error("Cannot push to a consumed SparrowStream");
        }
        m_stream_proxy.push(sparrow::array(std::move(arr.get_array())));
    }

    PyObject* SparrowStream::export_to_capsule()
    {
        if (m_consumed)
        {
            PyErr_SetString(PyExc_RuntimeError, "SparrowStream has already been consumed");
            return nullptr;
        }
        m_consumed = true;
        PyObject* capsule = sparrow::rockfinch::export_stream_proxy_to_capsule(m_stream_proxy);
        return capsule;
    }

    std::optional<SparrowArray> SparrowStream::pop()
    {
        if (m_consumed)
        {
            throw std::runtime_error("Cannot pop from a consumed SparrowStream");
        }
        auto arr_opt = m_stream_proxy.pop();
        if (!arr_opt.has_value())
        {
            return std::nullopt;
        }
        return SparrowArray(std::move(arr_opt.value()));
    }

    bool SparrowStream::is_consumed() const noexcept
    {
        return m_consumed;
    }
}  // namespace sparrow::rockfinch
