/**
 * @file sparrow_module.cpp
 * @brief Python module definition for sparrow-pycapsule using nanobind.
 *
 * This file defines the "sparrow" Python extension module that exposes
 * the SparrowArray class implementing the Arrow PyCapsule Interface.
 */

#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>

#include <sparrow-pycapsule/pycapsule.hpp>
#include <sparrow-pycapsule/sparrow_array_python_class.hpp>

namespace nb = nanobind;

namespace sparrow::pycapsule
{
    /**
     * @brief Create a SparrowArray from an object implementing __arrow_c_array__.
     */
    SparrowArray sparrow_array_from_arrow(const nb::object& arrow_array)
    {
        if (!nb::hasattr(arrow_array, "__arrow_c_array__"))
        {
            throw nb::type_error(
                "Input object must implement __arrow_c_array__ (ArrowArrayExportable protocol)"
            );
        }

        nb::object capsules = arrow_array.attr("__arrow_c_array__")();

        if (!nb::isinstance<nb::tuple>(capsules) || nb::len(capsules) != 2)
        {
            throw nb::type_error("__arrow_c_array__ must return a tuple of 2 elements");
        }

        auto capsule_tuple = nb::cast<nb::tuple>(capsules);
        PyObject* schema_capsule = capsule_tuple[0].ptr();
        PyObject* array_capsule = capsule_tuple[1].ptr();

        return {schema_capsule, array_capsule};
    }

    /**
     * @brief Export a SparrowArray to Arrow PyCapsules.
     */
    nb::tuple sparrow_array_to_arrow(const SparrowArray& self, nb::object /*requested_schema*/)
    {
        auto [schema, array] = self.export_to_capsules();
        return nb::make_tuple(nb::steal(schema), nb::steal(array));
    }

    /**
     * @brief Register the SparrowArray class with a nanobind module.
     */
    void register_sparrow_array(nb::module_& m)
    {
        nb::class_<SparrowArray>(m, "SparrowArray",
            "SparrowArray - Arrow array wrapper implementing __arrow_c_array__.\n\n"
            "This class wraps a sparrow array and implements the Arrow PyCapsule\n"
            "Interface, allowing direct integration with libraries like Polars.\n\n"
            "Example\n"
            "-------\n"
            ">>> import pyarrow as pa\n"
            ">>> import sparrow_rockfinch as sp\n"
            ">>> pa_array = pa.array([1, 2, None, 4])\n"
            ">>> sparrow_array = sparrow.SparrowArray.from_arrow(pa_array)")
            .def_static("from_arrow", &sparrow_array_from_arrow,
                nb::arg("arrow_array"),
                "Create a SparrowArray from an Arrow-compatible object.\n\n"
                "Parameters\n"
                "----------\n"
                "arrow_array : ArrowArrayExportable\n"
                "    An object implementing __arrow_c_array__ (e.g., PyArrow array).\n\n"
                "Returns\n"
                "-------\n"
                "SparrowArray\n"
                "    A new SparrowArray wrapping the input data.")
            .def("__arrow_c_array__", &sparrow_array_to_arrow,
                nb::arg("requested_schema") = nb::none(),
                "Export the array via the Arrow PyCapsule interface.\n\n"
                "Returns\n"
                "-------\n"
                "tuple[object, object]\n"
                "    A tuple of (schema_capsule, array_capsule).")
            .def("size", &SparrowArray::size,
                "Get the number of elements in the array.")
            .def("__len__", &SparrowArray::size);
    }

}  // namespace sparrow::pycapsule

NB_MODULE(SPARROW_MODULE_NAME, m)
{
    m.doc() = "Sparrow Rockfinch - High-performance Arrow array library for Python.\n\n"
              "This module provides the SparrowArray class which implements the\n"
              "Arrow PyCapsule Interface for zero-copy data exchange with other\n"
              "Arrow-compatible libraries like Polars and PyArrow.";

    sparrow::pycapsule::register_sparrow_array(m);
}
