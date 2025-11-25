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


def test_create_array_in_cpp():
    """Test creating an array in C++ (sparrow) and importing to Python/Polars."""
    print("\n" + "=" * 70)
    print("Test 1: C++ -> Python (Create array in sparrow, import to Polars)")
    print("=" * 70)
    
    print("\n1. Creating test array in C++ using sparrow::pycapsule...")
    schema_capsule, array_capsule = test_polars_helper.create_test_array_capsules()
    
    assert schema_capsule is not None, "Received null schema capsule from C++"
    assert array_capsule is not None, "Received null array capsule from C++"
    print("   PyCapsules created by sparrow::pycapsule::export_array_to_capsules()")
    
    print("\n2. Importing to PyArrow...")
    arrow_array = pa.Array._import_from_c_capsule(schema_capsule, array_capsule)
    print(f"   Arrow type: {arrow_array.type}")
    print(f"   Arrow values: {arrow_array.to_pylist()}")
    
    print("\n3. Converting to Polars...")
    polars_series = pl.from_arrow(arrow_array)
    print(f"   Polars series: {polars_series.to_list()}")
    
    expected = [10, 20, None, 40, 50]
    actual = polars_series.to_list()
    
    assert expected == actual, f"Data mismatch! Expected: {expected}, Actual: {actual}"
    print("   Data matches expected values!")
    print("\n" + "=" * 70)
    print("Test 1 PASSED")
    print("=" * 70)


def test_polars_to_cpp():
    """Test exporting Polars data to C++ (sparrow)."""
    print("\n" + "=" * 70)
    print("Test 2: Python -> C++ (Export Polars to sparrow)")
    print("=" * 70)
    
    print("\n1. Creating Polars series...")
    test_series = pl.Series([100, 200, None, 400, 500], dtype=pl.Int32)
    print(f"   Polars series: {test_series.to_list()}")
    
    print("\n2. Exporting to Arrow PyCapsules...")
    arrow_array = test_series.to_arrow()
    schema_capsule, array_capsule = arrow_array.__arrow_c_array__()
    print("   PyCapsules created by PyArrow")
    
    print("\n3. Importing and verifying in sparrow using sparrow::pycapsule...")
    result = test_polars_helper.verify_array_size_from_capsules(schema_capsule, array_capsule, 5)
    
    assert result is True, "C++ verification failed"
    print("   sparrow::pycapsule::import_array_from_capsules() succeeded!")
    print("   sparrow successfully imported and verified the array!")
    print("\n" + "=" * 70)
    print("Test 2 PASSED")
    print("=" * 70)


def test_roundtrip():
    """Test round-trip: Python -> C++ (sparrow) -> Python."""
    print("\n" + "=" * 70)
    print("Test 3: Round-trip (Python -> sparrow -> Python)")
    print("=" * 70)
    
    print("\n1. Creating Polars series...")
    original_series = pl.Series([1, 2, None, 4, 5], dtype=pl.Int32)
    print(f"   Original: {original_series.to_list()}")
    
    print("\n2. Exporting to Arrow PyCapsules...")
    arrow_array = original_series.to_arrow()
    schema_capsule_in, array_capsule_in = arrow_array.__arrow_c_array__()
    print("   PyCapsules created by PyArrow")
    
    print("\n3. Round-tripping through sparrow using sparrow::pycapsule...")
    schema_capsule_out, array_capsule_out = test_polars_helper.roundtrip_array_capsules(
        schema_capsule_in,
        array_capsule_in
    )
    
    assert schema_capsule_out is not None, "Received null schema capsule from C++"
    assert array_capsule_out is not None, "Received null array capsule from C++"
    print("   sparrow::pycapsule import/export succeeded!")
    
    print("\n4. Importing back to Python...")
    arrow_array_out = pa.Array._import_from_c_capsule(schema_capsule_out, array_capsule_out)
    result_series = pl.from_arrow(arrow_array_out)
    print(f"   Result: {result_series.to_list()}")
    
    original_data = original_series.to_list()
    result_data = result_series.to_list()
    assert original_data == result_data, f"Data mismatch! Original: {original_data}, Result: {result_data}"
    
    print("   Round-trip successful - data matches!")
    print("\n" + "=" * 70)
    print("Test 3 PASSED")
    print("=" * 70)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-s"]))
