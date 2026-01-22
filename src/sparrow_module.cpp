/**
 * @file sparrow_module.cpp
 * @brief Python module definition for sparrow-rockfinch using nanobind.
 *
 * This file defines the "sparrow" Python extension module that exposes
 * the SparrowArray and SparrowStream classes implementing the Arrow PyCapsule Interface.
 */

#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/optional.h>

#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>

#include <sparrow-rockfinch/pycapsule.hpp>
#include <sparrow-rockfinch/sparrow_array_python_class.hpp>
#include <sparrow-rockfinch/sparrow_stream_python_class.hpp>
#include <sparrow-rockfinch/config/sparrow_rockfinch_version.hpp>

namespace nb = nanobind;

namespace sparrow::rockfinch
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
     * @brief Export a SparrowArray schema to a PyCapsule.
     */
    nb::object sparrow_array_to_schema(const SparrowArray& self)
    {
        PyObject* capsule = self.export_schema_to_capsule();
        if (capsule == nullptr)
        {
            throw nb::python_error();
        }
        return nb::steal(capsule);
    }

    /**
     * @brief Create a SparrowStream from an object implementing __arrow_c_stream__.
     */
    SparrowStream sparrow_stream_from_stream(const nb::object& stream_obj)
    {
        PyObject* stream_capsule = nullptr;
        nb::object capsule_holder;

        if (PyCapsule_CheckExact(stream_obj.ptr()))
        {
            stream_capsule = stream_obj.ptr();
        }
        else if (nb::hasattr(stream_obj, "__arrow_c_stream__"))
        {
            capsule_holder = stream_obj.attr("__arrow_c_stream__")();
            stream_capsule = capsule_holder.ptr();
        }
        else
        {
            throw nb::type_error(
                "Input object must implement __arrow_c_stream__ (ArrowStreamExportable protocol) "
                "or be an arrow_array_stream PyCapsule"
            );
        }

        sparrow::arrow_array_stream_proxy proxy = import_stream_proxy_from_capsule(stream_capsule);
        if (PyErr_Occurred())
        {
            throw nb::python_error();
        }

        return SparrowStream(std::move(proxy));
    }

    /**
     * @brief Export a SparrowStream to a stream PyCapsule.
     */
    nb::object sparrow_stream_to_stream(SparrowStream& self, nb::object /*requested_schema*/)
    {
        PyObject* capsule = self.export_to_capsule();
        if (capsule == nullptr)
        {
            throw nb::python_error();
        }
        return nb::steal(capsule);
    }

    /**
     * @brief Register the SparrowArray class with a nanobind module.
     */
    void register_sparrow_array(nb::module_& m)
    {
        nb::class_<SparrowArray>(m, "SparrowArray",
            "SparrowArray - Arrow array wrapper implementing the Arrow PyCapsule Interface.\n\n"
            "This class wraps a sparrow array and implements:\n"
            "- __arrow_c_array__: Export as ArrowArray + ArrowSchema\n"
            "- __arrow_c_schema__: Export schema only\n\n"
            "This allows direct integration with libraries like Polars and PyArrow.\n\n"
            "Example\n"
            "-------\n"
            ">>> import pyarrow as pa\n"
            ">>> import sparrow_rockfinch as sp\n"
            ">>> pa_array = pa.array([1, 2, None, 4])\n"
            ">>> sparrow_array = sp.SparrowArray.from_arrow(pa_array)")
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
                "Parameters\n"
                "----------\n"
                "requested_schema : object, optional\n"
                "    A PyCapsule containing an ArrowSchema for requested format.\n"
                "    Currently ignored (best-effort conversion not implemented).\n\n"
                "Returns\n"
                "-------\n"
                "tuple[object, object]\n"
                "    A tuple of (schema_capsule, array_capsule).")
            .def("__arrow_c_schema__", &sparrow_array_to_schema,
                "Export the array's schema via the Arrow PyCapsule interface.\n\n"
                "Returns\n"
                "-------\n"
                "object\n"
                "    A PyCapsule containing an ArrowSchema.")
            .def("size", &SparrowArray::size,
                "Get the number of elements in the array.")
            .def("__len__", &SparrowArray::size);
    }

    /**
     * @brief Register the SparrowStream class with a nanobind module.
     */
    void register_sparrow_stream(nb::module_& m)
    {
        nb::class_<SparrowStream>(m, "SparrowStream",
            "SparrowStream - Arrow stream wrapper implementing the Arrow PyCapsule Interface.\n\n"
            "This class wraps one or more sparrow arrays and implements:\n"
            "- __arrow_c_stream__: Export as ArrowArrayStream\n\n"
            "This allows direct integration with libraries that consume Arrow streams.\n\n"
            "Example\n"
            "-------\n"
            ">>> import sparrow_rockfinch as sp\n"
            ">>> import pyarrow as pa\n"
            ">>> reader = pa.RecordBatchReader.from_batches(...)\n"
            ">>> stream = sp.SparrowStream.from_stream(reader)\n"
            ">>> # Use stream with Arrow-compatible libraries")
            .def_static("from_stream", &sparrow_stream_from_stream,
                nb::arg("stream"),
                "Create a SparrowStream from a stream-compatible object.\n\n"
                "Parameters\n"
                "----------\n"
                "stream : ArrowStreamExportable\n"
                "    An object implementing __arrow_c_stream__ or a PyCapsule.\n\n"
                "Returns\n"
                "-------\n"
                "SparrowStream\n"
                "    A new SparrowStream containing all batches from the source.")
            .def("__arrow_c_stream__", &sparrow_stream_to_stream,
                nb::arg("requested_schema") = nb::none(),
                "Export the stream via the Arrow PyCapsule interface.\n\n"
                "The stream can only be consumed once. After calling this method,\n"
                "the stream is marked as consumed.\n\n"
                "Parameters\n"
                "----------\n"
                "requested_schema : object, optional\n"
                "    A PyCapsule containing an ArrowSchema for requested format.\n"
                "    Currently ignored (best-effort conversion not implemented).\n\n"
                "Returns\n"
                "-------\n"
                "object\n"
                "    A PyCapsule containing an ArrowArrayStream.")
            .def("push", &SparrowStream::push,
                nb::arg("arr"),
                "Push a SparrowArray into the stream.")
            .def("pop", &SparrowStream::pop,
                "Pop the next SparrowArray from the stream.\n\n"
                "Returns\n"
                "-------\n"
                "Optional[SparrowArray]\n"
                "    The next SparrowArray in the stream, or None if the stream is exhausted.")
            .def("is_consumed", &SparrowStream::is_consumed,
                "Check if the SparrowStream has been consumed via export.\n\n"
                "Returns\n"
                "-------\n"
                "bool\n"
                "    True if the stream has been consumed, False otherwise.");
    }

}  // namespace sparrow::rockfinch

NB_MODULE(sparrow_rockfinch, m)
{
    m.doc() = "Sparrow Rockfinch - High-performance Arrow array library for Python.\n\n"
              "This module provides the SparrowArray and SparrowStream classes which\n"
              "implement the Arrow PyCapsule Interface for zero-copy data exchange with\n"
              "other Arrow-compatible libraries like Polars and PyArrow.";
    m.attr("__version__") = sparrow::rockfinch::SPARROW_ROCKFINCH_VERSION_STRING.c_str();
    sparrow::rockfinch::register_sparrow_array(m);
    sparrow::rockfinch::register_sparrow_stream(m);
}
