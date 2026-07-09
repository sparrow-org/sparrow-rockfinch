#include "sparrow-rockfinch/sparrow_array_python_class.hpp"

#include <utility>

namespace sparrow::rockfinch
{
    SparrowArray::SparrowArray(PyObject* schema_capsule, PyObject* array_capsule)
        : m_array(import_array_from_capsules(schema_capsule, array_capsule))
    {
    }

    SparrowArray::SparrowArray(sparrow::array&& arr)
        : m_array(std::move(arr))
    {
    }

    SparrowArray::SparrowArray(const SparrowArray& other)
        : m_array(other.m_array)
        , m_numpy_owner(other.m_numpy_owner)
        , m_numpy_owner_writable(other.m_numpy_owner_writable)
    {
        Py_XINCREF(m_numpy_owner);
    }

    SparrowArray::SparrowArray(SparrowArray&& other) noexcept
        : m_array(std::move(other.m_array))
        , m_numpy_owner(other.m_numpy_owner)
        , m_numpy_owner_writable(other.m_numpy_owner_writable)
    {
        other.m_numpy_owner = nullptr;
        other.m_numpy_owner_writable = false;
    }

    SparrowArray& SparrowArray::operator=(const SparrowArray& other)
    {
        if (this != &other)
        {
            clear_numpy_owner();
            m_array = other.m_array;
            m_numpy_owner = other.m_numpy_owner;
            m_numpy_owner_writable = other.m_numpy_owner_writable;
            Py_XINCREF(m_numpy_owner);
        }
        return *this;
    }

    SparrowArray& SparrowArray::operator=(SparrowArray&& other) noexcept
    {
        if (this != &other)
        {
            clear_numpy_owner();
            m_array = std::move(other.m_array);
            m_numpy_owner = other.m_numpy_owner;
            m_numpy_owner_writable = other.m_numpy_owner_writable;
            other.m_numpy_owner = nullptr;
            other.m_numpy_owner_writable = false;
        }
        return *this;
    }

    SparrowArray::~SparrowArray()
    {
        clear_numpy_owner();
    }

    std::pair<PyObject*, PyObject*> SparrowArray::export_to_capsules() const
    {
        // We need a non-const copy since export moves from the array
        sparrow::array arr_copy = m_array;
        return export_array_to_capsules(arr_copy);
    }

    PyObject* SparrowArray::export_schema_to_capsule() const
    {
        return sparrow::rockfinch::export_schema_to_capsule(m_array);
    }

    size_t SparrowArray::size() const
    {
        return m_array.size();
    }

    sparrow::array& SparrowArray::get_array()
    {
        return m_array;
    }

    const sparrow::array& SparrowArray::get_array() const
    {
        return m_array;
    }

    void SparrowArray::set_numpy_owner(PyObject* owner, bool writable)
    {
        clear_numpy_owner();
        m_numpy_owner = owner;
        m_numpy_owner_writable = writable;
        Py_XINCREF(m_numpy_owner);
    }

    PyObject* SparrowArray::numpy_owner() const
    {
        return m_numpy_owner;
    }

    bool SparrowArray::numpy_owner_writable() const
    {
        return m_numpy_owner_writable;
    }

    void SparrowArray::clear_numpy_owner()
    {
        Py_XDECREF(m_numpy_owner);
        m_numpy_owner = nullptr;
        m_numpy_owner_writable = false;
    }

}  // namespace sparrow::rockfinch
