#!/usr/bin/env python3
"""
Integration test for sparrow-rockfinch with Polars and PyArrow.

This file contains:
1. TestSparrowToPolars: Unique tests for Sparrow → Polars (C++ created arrays)
2. Imports shared test classes from test_common to avoid duplication
"""

# Import from sparrow_helpers first to trigger module setup
from sparrow_helpers import (
    ArrowArrayExportable,
)

# Import the C++ module for create_test_array (try release first, then debug)
try:
    import test_sparrow_helper  # noqa: E402
except ImportError:
    import test_sparrow_helperd as test_sparrow_helper  # noqa: E402

import sys

import pytest
import polars as pl

# Import shared test classes from test_common
from test_common import (
    TestPyArrowToSparrow,
    TestSparrowStreamWithPolars,
    arrow_array_to_series,
)


# =============================================================================
# Test 1: Sparrow → Polars (Create array in C++, import to Polars)
# This is unique to this file as it requires the C++ test helper module
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

        # Check the module-qualified name (allow debug 'd' suffix)
        full_name = f"{type(sparrow_array).__module__}.{type_name}"
        valid_names = ("sparrow_rockfinch.SparrowArray", "sparrow_rockfinchd.SparrowArray")
        assert full_name in valid_names, (
            f"Expected one of {valid_names}, got '{full_name}'"
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
