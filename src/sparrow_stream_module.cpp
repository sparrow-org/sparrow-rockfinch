/**
 * @file sparrow_stream_module.cpp
 * @brief Nanobind registration for SparrowStream.
 */

#include "sparrow_stream_module.hpp"

#include <nanobind/stl/optional.h>

#include <sparrow-rockfinch/pycapsule.hpp>
#include <sparrow-rockfinch/sparrow_stream_python_class.hpp>

#include <sparrow/arrow_interface/arrow_array_stream_proxy.hpp>

namespace nb = nanobind;

namespace sparrow::rockfinch
{
    namespace
    {
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

        nb::object sparrow_stream_to_stream(SparrowStream& self, nb::object /*requested_schema*/)
        {
            PyObject* capsule = self.export_to_capsule();
            if (capsule == nullptr)
            {
                throw nb::python_error();
            }
            return nb::steal(capsule);
        }
    }

    void register_sparrow_stream(nb::module_& m)
    {
        nb::class_<SparrowStream>(
            m,
            "SparrowStream",
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
            ">>> # Use stream with Arrow-compatible libraries"
        )
            .def_static(
                "from_stream",
                &sparrow_stream_from_stream,
                nb::arg("stream"),
                "Create a SparrowStream from a stream-compatible object.\n\n"
                "Parameters\n"
                "----------\n"
                "stream : ArrowStreamExportable\n"
                "    An object implementing __arrow_c_stream__ or a PyCapsule.\n\n"
                "Returns\n"
                "-------\n"
                "SparrowStream\n"
                "    A new SparrowStream containing all batches from the source."
            )
            .def(
                "__arrow_c_stream__",
                &sparrow_stream_to_stream,
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
                "    A PyCapsule containing an ArrowArrayStream."
            )
            .def("push", &SparrowStream::push, nb::arg("arr"), "Push a SparrowArray into the stream.")
            .def(
                "pop",
                &SparrowStream::pop,
                "Pop the next SparrowArray from the stream.\n\n"
                "Returns\n"
                "-------\n"
                "Optional[SparrowArray]\n"
                "    The next SparrowArray in the stream, or None if the stream is exhausted."
            )
            .def(
                "is_consumed",
                &SparrowStream::is_consumed,
                "Check if the SparrowStream has been consumed via export.\n\n"
                "Returns\n"
                "-------\n"
                "bool\n"
                "    True if the stream has been consumed, False otherwise."
            );
    }
}
