#!/usr/bin/env python3
"""
Integration test for sparrow-pycapsule with Polars.

This test demonstrates bidirectional data exchange between sparrow (C++) and Polars (Python)
using the Arrow C Data Interface via sparrow::pycapsule. The test_polars_helper module is
a native Python extension that uses sparrow::pycapsule::export_array_to_capsules() and
import_array_from_capsules() to create and consume Arrow PyCapsules directly.
"""

import sys
import os
from pathlib import Path

import pytest
import polars as pl
import pyarrow as pa


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
# Test 1: C++ -> Python (Create array in sparrow, import to Polars)
# =============================================================================


class TestCppToPython:
    """Test creating an array in C++ (sparrow) and importing to Python/Polars."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Create test array capsules from C++."""
        self.schema_capsule, self.array_capsule = (
            test_polars_helper.create_test_array_capsules()
        )

    def test_step1_create_capsules_in_cpp(self):
        """Step 1: Create PyCapsules in C++ using sparrow::pycapsule."""
        assert self.schema_capsule is not None, "Received null schema capsule from C++"
        assert self.array_capsule is not None, "Received null array capsule from C++"

    def test_step2_import_to_pyarrow(self):
        """Step 2: Import PyCapsules to PyArrow."""
        arrow_array = pa.Array._import_from_c_capsule(
            self.schema_capsule, self.array_capsule
        )
        assert arrow_array.type == pa.int32(), f"Expected int32, got {arrow_array.type}"
        assert arrow_array.to_pylist() == [10, 20, None, 40, 50]

    def test_step3_convert_to_polars(self):
        """Step 3: Convert PyArrow array to Polars series."""
        arrow_array = pa.Array._import_from_c_capsule(
            self.schema_capsule, self.array_capsule
        )
        polars_series = pl.from_arrow(arrow_array)

        expected = [10, 20, None, 40, 50]
        actual = polars_series.to_list()
        assert expected == actual, f"Data mismatch! Expected: {expected}, Actual: {actual}"


# =============================================================================
# Test 2: Python -> C++ (Export Polars to sparrow)
# =============================================================================


class TestPythonToCpp:
    """Test exporting Polars data to C++ (sparrow)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Create Polars series and export to capsules."""
        self.test_series = pl.Series([100, 200, None, 400, 500], dtype=pl.Int32)
        self.arrow_array = self.test_series.to_arrow()
        self.schema_capsule, self.array_capsule = self.arrow_array.__arrow_c_array__()

    def test_step1_create_polars_series(self):
        """Step 1: Create Polars series."""
        assert self.test_series.to_list() == [100, 200, None, 400, 500]
        assert self.test_series.dtype == pl.Int32

    def test_step2_export_to_capsules(self):
        """Step 2: Export Polars series to Arrow PyCapsules."""
        assert self.schema_capsule is not None, "Schema capsule is None"
        assert self.array_capsule is not None, "Array capsule is None"

    def test_step3_import_in_sparrow(self):
        """Step 3: Import and verify in sparrow using sparrow::pycapsule."""
        result = test_polars_helper.verify_array_size_from_capsules(
            self.schema_capsule, self.array_capsule, 5
        )
        assert result is True, "C++ verification failed"


# =============================================================================
# Test 3: Round-trip (Python -> sparrow -> Python)
# =============================================================================


class TestRoundtrip:
    """Test round-trip: Python -> C++ (sparrow) -> Python."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Create original series and export to capsules."""
        self.original_series = pl.Series([1, 2, None, 4, 5], dtype=pl.Int32)
        self.arrow_array = self.original_series.to_arrow()
        self.schema_capsule_in, self.array_capsule_in = (
            self.arrow_array.__arrow_c_array__()
        )

    def test_step1_create_original_series(self):
        """Step 1: Create original Polars series."""
        assert self.original_series.to_list() == [1, 2, None, 4, 5]

    def test_step2_export_to_capsules(self):
        """Step 2: Export to Arrow PyCapsules."""
        assert self.schema_capsule_in is not None
        assert self.array_capsule_in is not None

    def test_step3_roundtrip_through_sparrow(self):
        """Step 3: Round-trip through sparrow using sparrow::pycapsule."""
        schema_capsule_out, array_capsule_out = (
            test_polars_helper.roundtrip_array_capsules(
                self.schema_capsule_in, self.array_capsule_in
            )
        )
        assert schema_capsule_out is not None, "Received null schema capsule from C++"
        assert array_capsule_out is not None, "Received null array capsule from C++"

    def test_step4_import_back_to_python(self):
        """Step 4: Import back to Python and verify data matches."""
        schema_capsule_out, array_capsule_out = (
            test_polars_helper.roundtrip_array_capsules(
                self.schema_capsule_in, self.array_capsule_in
            )
        )
        arrow_array_out = pa.Array._import_from_c_capsule(
            schema_capsule_out, array_capsule_out
        )
        result_series = pl.from_arrow(arrow_array_out)

        original_data = self.original_series.to_list()
        result_data = result_series.to_list()
        assert (
            original_data == result_data
        ), f"Data mismatch! Original: {original_data}, Result: {result_data}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
