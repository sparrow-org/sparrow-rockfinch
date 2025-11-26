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
import os
from pathlib import Path

import pytest
import polars as pl
import pyarrow as pa
from polars._plr import PySeries
from polars._utils.wrap import wrap_s


def setup_module_path():
    """Add the build directory to Python path so we can import test_polars_helper."""
    # Check for environment variable first
    helper_path = os.environ.get('TEST_POLARS_HELPER_PATH')
    if helper_path:
        module_dir = Path(helper_path).parent
        if module_dir.exists():
            sys.path.insert(0, str(module_dir))
            return

    # Try to find in build directory
    test_dir = Path(__file__).parent
    build_dirs = [
        test_dir.parent / "build" / "bin" / "Debug",
        test_dir.parent / "build" / "bin" / "Release",
        test_dir.parent / "build" / "bin",
    ]

    for build_dir in build_dirs:
        if build_dir.exists():
            sys.path.insert(0, str(build_dir))
            return

    raise ImportError(
        "Could not find test_polars_helper module. "
        "Build the project first or set TEST_POLARS_HELPER_PATH."
    )


# Set up module path before importing
setup_module_path()

# Import the native Python extension module
import test_polars_helper  # noqa: E402


# =============================================================================
# Helper function to convert ArrowArrayExportable to Polars Series
# =============================================================================


def arrow_array_to_series(arrow_array, name: str = "") -> pl.Series:
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
        sparrow_array = test_polars_helper.create_test_array()
        
        assert sparrow_array is not None, "Received null SparrowArray from C++"
        assert hasattr(sparrow_array, '__arrow_c_array__'), "SparrowArray missing __arrow_c_array__ method"
        assert sparrow_array.size() == 5, f"Expected size 5, got {sparrow_array.size()}"

    def test_sparrow_to_polars_series(self):
        """Convert SparrowArray to Polars Series using the Arrow PyCapsule Interface."""
        sparrow_array = test_polars_helper.create_test_array()
        polars_series = arrow_array_to_series(sparrow_array)

        assert polars_series.dtype == pl.Int32, f"Expected Int32, got {polars_series.dtype}"
        expected = [10, 20, None, 40, 50]
        actual = polars_series.to_list()
        assert expected == actual, f"Data mismatch! Expected: {expected}, Actual: {actual}"

    def test_sparrow_to_polars_preserves_nulls(self):
        """Verify that null values from sparrow are preserved in Polars."""
        sparrow_array = test_polars_helper.create_test_array()
        polars_series = arrow_array_to_series(sparrow_array)
        
        # The test array has a null at index 2
        values = polars_series.to_list()
        assert values[2] is None, "Null value not preserved at index 2"


# =============================================================================
# Test 2: PyArrow → Sparrow (Create array in PyArrow, import to sparrow)
# =============================================================================


class TestPyArrowToSparrow:
    """Test creating an array in PyArrow and importing to sparrow."""

    def test_pyarrow_to_sparrow_via_capsules(self):
        """Import PyArrow array to sparrow using PyCapsules."""
        # Create a PyArrow array
        pa_array = pa.array([100, 200, None, 400, 500], type=pa.int32())
        
        # Export to PyCapsules using Arrow PyCapsule Interface
        schema_capsule, array_capsule = pa_array.__arrow_c_array__()
        
        # Verify sparrow can import and read the data
        result = test_polars_helper.verify_array_size_from_capsules(
            schema_capsule, array_capsule, 5
        )
        assert result is True, "Sparrow failed to import PyArrow array"

    def test_pyarrow_roundtrip_through_sparrow(self):
        """Round-trip: PyArrow → sparrow → Polars."""
        # Create a PyArrow array
        pa_array = pa.array([1, 2, None, 4, 5], type=pa.int32())
        
        # Export to PyCapsules
        schema_capsule, array_capsule = pa_array.__arrow_c_array__()
        
        # Round-trip through sparrow (import then export)
        schema_out, array_out = test_polars_helper.roundtrip_array_capsules(
            schema_capsule, array_capsule
        )
        
        # Import the result into Polars using a wrapper
        class CapsuleWrapper:
            def __init__(self, schema, array):
                self._schema = schema
                self._array = array
            def __arrow_c_array__(self, requested_schema=None):
                return self._schema, self._array
        
        wrapper = CapsuleWrapper(schema_out, array_out)
        result_series = arrow_array_to_series(wrapper)
        
        # Verify data matches
        expected = [1, 2, None, 4, 5]
        actual = result_series.to_list()
        assert expected == actual, f"Data mismatch! Expected: {expected}, Actual: {actual}"

    def test_pyarrow_nulls_preserved_in_sparrow(self):
        """Verify that null values from PyArrow are preserved through sparrow."""
        # Create a PyArrow array with nulls
        pa_array = pa.array([None, 1, None, 3, None], type=pa.int32())
        
        # Export to PyCapsules
        schema_capsule, array_capsule = pa_array.__arrow_c_array__()
        
        # Round-trip through sparrow
        schema_out, array_out = test_polars_helper.roundtrip_array_capsules(
            schema_capsule, array_capsule
        )
        
        # Import into Polars
        class CapsuleWrapper:
            def __init__(self, schema, array):
                self._schema = schema
                self._array = array
            def __arrow_c_array__(self, requested_schema=None):
                return self._schema, self._array
        
        wrapper = CapsuleWrapper(schema_out, array_out)
        result_series = arrow_array_to_series(wrapper)
        
        # Check null positions
        values = result_series.to_list()
        assert values[0] is None, "Null not preserved at index 0"
        assert values[1] == 1, "Value changed at index 1"
        assert values[2] is None, "Null not preserved at index 2"
        assert values[3] == 3, "Value changed at index 3"
        assert values[4] is None, "Null not preserved at index 4"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
