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

    const sparrow::array& SparrowArray::get_array() const
    {
        return m_array;
    }

}  // namespace sparrow::rockfinch
