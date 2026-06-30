/**
 * @file sparrow_module.cpp
 * @brief Python module entrypoint for sparrow-rockfinch using nanobind.
 */

#include "sparrow_array_module.hpp"
#include "sparrow_stream_module.hpp"

#include <nanobind/nanobind.h>

#include <sparrow-rockfinch/config/sparrow_rockfinch_version.hpp>

namespace nb = nanobind;

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
