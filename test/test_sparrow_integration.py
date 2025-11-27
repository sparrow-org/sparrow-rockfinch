#!/usr/bin/env python3
"""
Integration test for sparrow-pycapsule with Polars and PyArrow.

This test demonstrates:
1. Sparrow → Polars: Create array in C++ (sparrow), import to Polars
2. PyArrow → Sparrow: Create array in PyArrow, import to sparrow

The C++ SparrowArray class implements the Arrow PyCapsule Interface (__arrow_c_array__),
allowing direct integration with Polars without going through PyArrow.
"""

import sys

import pytest
import polars as pl
import pyarrow as pa
from polars._plr import PySeries
from polars._utils.wrap import wrap_s


# Import helpers from our Python module
from sparrow_helpers import (
    ArrowArrayExportable,
    SparrowArray,
    SparrowArrayType,
)

# Import the C++ module for create_test_array
import test_sparrow_helper  # noqa: E402


def arrow_array_to_series(
    arrow_array: ArrowArrayExportable, name: str = ""
) -> pl.Series:
    """
    Convert an object implementing __arrow_c_array__ to a Polars Series.

    This function uses Polars' internal PySeries.from_arrow_c_array to create
    a Series directly from an Arrow array.

    Parameters
    ----------
    arrow_array : ArrowArrayExportable
        An object that implements __arrow_c_array__ method.
    name : str, optional
        Name for the resulting Series. Default is empty string.

    Returns
    -------
    pl.Series
        A Polars Series containing the array data.
    """
    ps = PySeries.from_arrow_c_array(arrow_array)
    series = wrap_s(ps)
    if name:
        series = series.alias(name)
    return series


# =============================================================================
# Test 1: Sparrow → Polars (Create array in C++, import to Polars)
# =============================================================================


class TestSparrowToPolars:
    """Test creating an array in C++ (sparrow) and importing to Polars."""

    def test_create_sparrow_array(self):
        """Create a SparrowArray in C++ that implements __arrow_c_array__."""
        sparrow_array = test_sparrow_helper.create_test_array()

        assert sparrow_array is not None, "Received null SparrowArray from C++"
        assert hasattr(sparrow_array, "__arrow_c_array__"), (
            "SparrowArray missing __arrow_c_array__ method"
        )
        assert sparrow_array.size() == 5, f"Expected size 5, got {sparrow_array.size()}"

    def test_sparrow_array_type(self):
        """Verify that created array is a sparrow.SparrowArray instance."""
        sparrow_array = test_sparrow_helper.create_test_array()

        # Check the type name
        type_name = type(sparrow_array).__name__
        assert type_name == "SparrowArray", (
            f"Expected type 'SparrowArray', got '{type_name}'"
        )

        # Check the module-qualified name
        full_name = f"{type(sparrow_array).__module__}.{type_name}"
        assert full_name == "sparrow.SparrowArray", (
            f"Expected 'sparrow.SparrowArray', got '{full_name}'"
        )

    def test_sparrow_to_polars_series(self):
        """Convert SparrowArray to Polars Series using the Arrow PyCapsule Interface."""
        sparrow_array = test_sparrow_helper.create_test_array()
        polars_series = arrow_array_to_series(sparrow_array)

        assert polars_series.dtype == pl.Int32, (
            f"Expected Int32, got {polars_series.dtype}"
        )
        expected = [10, 20, None, 40, 50]
        actual = polars_series.to_list()
        assert expected == actual, (
            f"Data mismatch! Expected: {expected}, Actual: {actual}"
        )

    def test_sparrow_to_polars_preserves_nulls(self):
        """Verify that null values from sparrow are preserved in Polars."""
        sparrow_array = test_sparrow_helper.create_test_array()
        polars_series = arrow_array_to_series(sparrow_array)

        # The test array has a null at index 2
        values = polars_series.to_list()
        assert values[2] is None, "Null value not preserved at index 2"


# =============================================================================
# Test 2: PyArrow → Sparrow (Create array in PyArrow, import to sparrow)
# =============================================================================


class TestPyArrowToSparrow:
    """Test creating an array in PyArrow and importing to sparrow."""

    def test_create_sparrow_array_from_pyarrow(self):
        """Create a SparrowArray directly from a PyArrow array using the constructor."""
        # Create a PyArrow array
        pa_array = pa.array([100, 200, None, 400, 500], type=pa.int32())

        # Create SparrowArray directly using the type constructor
        sparrow_array = SparrowArray(pa_array)

        # Verify it's a SparrowArray
        assert type(sparrow_array).__name__ == "SparrowArray"
        assert sparrow_array.size() == 5

        # Verify we can convert it to Polars
        polars_series = arrow_array_to_series(sparrow_array)
        expected = [100, 200, None, 400, 500]
        assert polars_series.to_list() == expected

    def test_pyarrow_to_sparrow(self):
        """Import PyArrow array to sparrow using Arrow PyCapsule Interface."""
        # Create a PyArrow array
        pa_array = pa.array([100, 200, None, 400, 500], type=pa.int32())

        # Verify sparrow can import and read the data via __arrow_c_array__

        sparrow_array: SparrowArrayType = SparrowArray(pa_array)
        assert sparrow_array.size() == 5

    def test_pyarrow_roundtrip_through_sparrow(self):
        """Round-trip: PyArrow → sparrow → Polars."""
        # Create a PyArrow array
        pa_array = pa.array([1, 2, None, 4, 5], type=pa.int32())

        # Round-trip through sparrow (import then export as SparrowArray)
        sparrow_array = SparrowArray(pa_array)

        # Import the result into Polars
        result_series = arrow_array_to_series(sparrow_array)

        # Verify data matches
        expected = [1, 2, None, 4, 5]
        actual = result_series.to_list()
        assert expected == actual, (
            f"Data mismatch! Expected: {expected}, Actual: {actual}"
        )

    def test_pyarrow_nulls_preserved_in_sparrow(self):
        """Verify that null values from PyArrow are preserved through sparrow."""
        # Create a PyArrow array with nulls
        pa_array = pa.array([None, 1, None, 3, None], type=pa.int32())

        # Round-trip through sparrow
        sparrow_array = SparrowArray(pa_array)

        # Import into Polars
        result_series = arrow_array_to_series(sparrow_array)

        # Check null positions
        values = result_series.to_list()
        assert values[0] is None, "Null not preserved at index 0"
        assert values[1] == 1, "Value changed at index 1"
        assert values[2] is None, "Null not preserved at index 2"
        assert values[3] == 3, "Value changed at index 3"
        assert values[4] is None, "Null not preserved at index 4"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
