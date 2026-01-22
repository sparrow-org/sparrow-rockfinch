#!/usr/bin/env python3
"""
Common test classes shared across multiple test files.

This module contains reusable test classes to avoid duplication:
- TestPyArrowToSparrow: Tests for PyArrow → Sparrow integration
- TestSparrowStreamWithPolars: Tests for SparrowStream with Polars
"""

import polars as pl
import pyarrow as pa
from polars._plr import PySeries
from polars._utils.wrap import wrap_s

# Import sparrow_rockfinch module (try release first, then debug)
try:
    from sparrow_rockfinch import SparrowArray, SparrowStream
except ImportError:
    from sparrow_rockfinchd import SparrowArray, SparrowStream


def arrow_array_to_series(
    arrow_array, name: str = ""
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
# Test: PyArrow → Sparrow (Create array in PyArrow, import to sparrow)
# =============================================================================


class TestPyArrowToSparrow:
    """Test creating an array in PyArrow and importing to sparrow."""

    def test_create_sparrow_array_from_pyarrow(self):
        """Create a SparrowArray directly from a PyArrow array using from_arrow()."""
        # Create a PyArrow array
        pa_array = pa.array([100, 200, None, 400, 500], type=pa.int32())

        # Create SparrowArray using the factory method
        sparrow_array = SparrowArray.from_arrow(pa_array)

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
        sparrow_array = SparrowArray.from_arrow(pa_array)
        assert sparrow_array.size() == 5

    def test_pyarrow_roundtrip_through_sparrow(self):
        """Round-trip: PyArrow → sparrow → Polars."""
        
        # Create a PyArrow array
        pa_array = pa.array([1, 2, None, 4, 5], type=pa.int32())

        # Round-trip through sparrow (import then export as SparrowArray)
        sparrow_array = SparrowArray.from_arrow(pa_array)

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
        sparrow_array = SparrowArray.from_arrow(pa_array)

        # Import into Polars
        result_series = arrow_array_to_series(sparrow_array)

        # Check null positions
        values = result_series.to_list()
        assert values[0] is None, "Null not preserved at index 0"
        assert values[1] == 1, "Value changed at index 1"
        assert values[2] is None, "Null not preserved at index 2"
        assert values[3] == 3, "Value changed at index 3"
        assert values[4] is None, "Null not preserved at index 4"


# =============================================================================
# Test: SparrowStream with Polars DataFrame and Series
# =============================================================================


class TestSparrowStreamWithPolars:
    """Test SparrowStream integration with Polars DataFrame and Series."""

    def test_create_stream_from_polars_dataframe(self):
        """Create SparrowStream from a Polars DataFrame."""
        
        # Create a Polars DataFrame
        df = pl.DataFrame({
            "integers": [1, 2, 3, 4, 5],
            "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
            "strings": ["a", "b", "c", "d", "e"]
        })

        # Create SparrowStream from the DataFrame
        stream = SparrowStream.from_stream(df)

        assert stream is not None
        assert type(stream).__name__ == "SparrowStream"

    def test_create_stream_from_polars_series(self):
        """Create SparrowStream from a Polars Series."""
        
        # Create a Polars Series
        series = pl.Series("values", [10, 20, 30, 40, 50])

        # Create SparrowStream from the Series
        stream = SparrowStream.from_stream(series)

        assert stream is not None
        assert type(stream).__name__ == "SparrowStream"

    def test_polars_dataframe_to_sparrow_stream_roundtrip(self):
        """Round-trip: Polars DataFrame → SparrowStream → PyArrow → Polars."""
        
        # Create original DataFrame
        original_df = pl.DataFrame({
            "integers": [1, 2, 3, 4, 5],
            "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
            "strings": ["a", "b", "c", "d", "e"]
        })

        # Import into SparrowStream
        stream = SparrowStream.from_stream(original_df)

        # Export to PyArrow then convert back to Polars
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()
        result_df = pl.from_arrow(result_table)

        # Verify data integrity
        assert result_df.shape == original_df.shape
        assert result_df.columns == original_df.columns
        assert result_df.equals(original_df)

    def test_polars_series_to_sparrow_stream_roundtrip(self):
        """Round-trip: Polars Series → SparrowStream → PyArrow → Polars."""
        
        # Create original Series and convert to DataFrame for streaming
        # (Series exports as non-struct schema which doesn't work with RecordBatchReader)
        original_series = pl.Series("values", [10, 20, 30, 40, 50])
        df = original_series.to_frame()

        # Import into SparrowStream
        stream = SparrowStream.from_stream(df)

        # Export to PyArrow
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()

        # Convert back to Polars DataFrame
        result_df = pl.from_arrow(result_table)

        # Verify data integrity
        assert result_df.shape == (5, 1)
        assert result_df.columns == ["values"]
        assert result_df["values"].equals(original_series)

    def test_polars_dataframe_with_nulls_through_stream(self):
        """Test Polars DataFrame with null values through SparrowStream."""
        
        # Create DataFrame with nulls
        df = pl.DataFrame({
            "nullable_ints": [1, None, 3, None, 5],
            "nullable_strings": ["a", None, "c", None, "e"]
        })

        # Create SparrowStream
        stream = SparrowStream.from_stream(df)

        # Export and verify
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()
        result_df = pl.from_arrow(result_table)

        assert result_df.equals(df)

    def test_polars_series_with_nulls_through_stream(self):
        """Test Polars Series with null values through SparrowStream."""
        
        # Create Series with nulls and convert to DataFrame for streaming
        series = pl.Series("nullable", [1, None, 3, None, 5])
        df = series.to_frame()

        # Create SparrowStream
        stream = SparrowStream.from_stream(df)

        # Export and verify
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()
        result_df = pl.from_arrow(result_table)

        assert result_df["nullable"].equals(series)

    def test_polars_dataframe_various_types_through_stream(self):
        """Test Polars DataFrame with various data types through SparrowStream."""
        
        # Create DataFrame with different types
        df = pl.DataFrame({
            "int32": pl.Series([1000, 2000, 3000], dtype=pl.Int32),
            "int64": pl.Series([10000, 20000, 30000], dtype=pl.Int64),
            "float32": pl.Series([1.5, 2.5, 3.5], dtype=pl.Float32),
            "float64": pl.Series([1.125, 2.25, 3.375], dtype=pl.Float64),
            "boolean": pl.Series([True, False, True], dtype=pl.Boolean),
            "string": pl.Series(["hello", "world", "test"]),
        })

        # Create SparrowStream
        stream = SparrowStream.from_stream(df)

        # Export and verify
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()
        result_df = pl.from_arrow(result_table)

        assert result_df.shape == df.shape
        assert result_df.columns == df.columns

    def test_polars_empty_dataframe_through_stream(self):
        """Test Polars empty DataFrame through SparrowStream."""
        
        # Create empty DataFrame with schema
        df = pl.DataFrame({
            "col1": pl.Series([], dtype=pl.Int64),
            "col2": pl.Series([], dtype=pl.Float64)
        })

        # Create SparrowStream
        stream = SparrowStream.from_stream(df)

        # Export and verify
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()

        assert result_table.num_rows == 0
        assert result_table.num_columns == 2

    def test_polars_large_dataframe_through_stream(self):
        """Test Polars DataFrame with larger dataset through SparrowStream."""
        
        # Create larger DataFrame
        n = 10000
        df = pl.DataFrame({
            "id": range(n),
            "value": [i * 2.5 for i in range(n)],
            "category": [f"cat_{i % 10}" for i in range(n)]
        })

        # Create SparrowStream
        stream = SparrowStream.from_stream(df)

        # Export and verify
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()
        result_df = pl.from_arrow(result_table)

        assert result_df.shape == df.shape
        assert result_df.equals(df)

    def test_polars_stream_consumed_state(self):
        """Test that stream from Polars gets properly consumed."""
        
        df = pl.DataFrame({"x": [1, 2, 3]})
        stream = SparrowStream.from_stream(df)

        assert stream.is_consumed() is False

        # Export should consume
        _ = stream.__arrow_c_stream__()
        assert stream.is_consumed() is True

    def test_pop_from_polars_dataframe_stream(self):
        """Test popping batches from a Polars DataFrame stream."""
        
        # Create DataFrame
        df = pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        stream = SparrowStream.from_stream(df)
        
        # Pop the batch
        batch = stream.pop()
        assert batch is not None
        assert type(batch).__name__ == "SparrowArray"
        assert batch.size() == 3
        
        # Stream should be exhausted
        assert stream.pop() is None

    def test_pop_from_polars_series_stream(self):
        """Test popping from a Polars Series stream."""
        
        # Create Series
        series = pl.Series([10, 20, 30, 40])
        stream = SparrowStream.from_stream(series)
        
        # Pop the batch
        batch = stream.pop()
        assert batch is not None
        assert batch.size() == 4
        
        # Stream should be exhausted
        assert stream.pop() is None

    def test_create_sparrow_arrays_from_polars_for_push(self):
        """Test creating SparrowArrays from Polars data that can be used with push."""
        
        # Create arrays from Polars data
        series1 = pl.Series([10, 20, 30])
        series2 = pl.Series([40, 50])
        
        # Convert to SparrowArrays
        arr1 = SparrowArray.from_arrow(series1.to_arrow())
        arr2 = SparrowArray.from_arrow(series2.to_arrow())
        
        # Verify arrays were created successfully
        assert arr1.size() == 3
        assert arr2.size() == 2
        assert type(arr1).__name__ == "SparrowArray"
        assert type(arr2).__name__ == "SparrowArray"
