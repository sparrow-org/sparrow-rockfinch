#!/usr/bin/env python3
"""
Tests for SparrowStream Arrow PyCapsule Interface implementation.

This test validates:
1. SparrowStream creation from stream-compatible objects
2. __arrow_c_stream__ protocol export
3. Stream consumption behavior
4. Round-trip data integrity
5. Integration with PyArrow streams
"""

import pytest
import pyarrow as pa

# Import helpers from our Python module
from sparrow_helpers import SparrowArray

# Import the module (try release first, then debug)
try:
    import sparrow_rockfinch as sr
except ImportError:
    import sparrow_rockfinchd as sr


class TestSparrowStreamCreation:
    """Test SparrowStream creation and basic properties."""

    def test_sparrow_stream_exists(self):
        """Verify SparrowStream class is available in the module."""
        assert hasattr(sr, "SparrowStream"), "Module should have SparrowStream class"

    def test_sparrow_stream_has_from_stream(self):
        """Verify SparrowStream has from_stream static method."""
        assert hasattr(sr.SparrowStream, "from_stream"), (
            "SparrowStream should have from_stream method"
        )

    def test_sparrow_stream_no_from_array(self):
        """Verify SparrowStream does NOT have from_array method."""
        assert not hasattr(sr.SparrowStream, "from_array"), (
            "SparrowStream should NOT have from_array method"
        )

    def test_create_stream_from_pyarrow_table(self):
        """Create SparrowStream from a PyArrow Table (which has __arrow_c_stream__)."""
        # Create a simple PyArrow table
        table = pa.table({"values": [1, 2, 3, 4, 5]})

        # Create SparrowStream from the table
        stream = sr.SparrowStream.from_stream(table)

        assert stream is not None
        assert isinstance(stream, sr.SparrowStream)

    def test_create_stream_from_pyarrow_record_batch_reader(self):
        """Create SparrowStream from a PyArrow RecordBatchReader."""
        # Create a record batch
        batch = pa.record_batch({"values": [10, 20, 30]})

        # Create a reader from the batch
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

        # Create SparrowStream from the reader
        stream = sr.SparrowStream.from_stream(reader)

        assert stream is not None
        assert isinstance(stream, sr.SparrowStream)


class TestSparrowStreamProperties:
    """Test SparrowStream instance properties."""

    def test_batch_count_single_batch(self):
        """Verify batch_count returns correct count for single batch."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.batch_count() == 1

    def test_batch_count_multiple_batches(self):
        """Verify batch_count returns correct count for multiple batches."""
        batch1 = pa.record_batch({"x": [1, 2, 3]})
        batch2 = pa.record_batch({"x": [4, 5, 6]})
        batch3 = pa.record_batch({"x": [7, 8, 9]})
        reader = pa.RecordBatchReader.from_batches(batch1.schema, [batch1, batch2, batch3])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.batch_count() == 3

    def test_len_returns_batch_count(self):
        """Verify __len__ returns batch count."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert len(stream) == 1

    def test_is_consumed_initially_false(self):
        """Verify is_consumed is False for new stream."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.is_consumed() is False


class TestSparrowStreamExport:
    """Test SparrowStream __arrow_c_stream__ export."""

    def test_has_arrow_c_stream_method(self):
        """Verify SparrowStream has __arrow_c_stream__ method."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert hasattr(stream, "__arrow_c_stream__")

    def test_export_returns_capsule(self):
        """Verify __arrow_c_stream__ returns a PyCapsule."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        capsule = stream.__arrow_c_stream__()

        assert capsule is not None
        # PyCapsule type check
        assert type(capsule).__name__ == "PyCapsule"

    def test_export_marks_stream_consumed(self):
        """Verify exporting marks the stream as consumed."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.is_consumed() is False
        _ = stream.__arrow_c_stream__()
        assert stream.is_consumed() is True

    def test_double_export_raises_error(self):
        """Verify exporting twice raises RuntimeError."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        # First export should succeed
        _ = stream.__arrow_c_stream__()

        # Second export should fail
        with pytest.raises(RuntimeError, match="consumed"):
            stream.__arrow_c_stream__()


class TestSparrowStreamRoundTrip:
    """Test round-trip data integrity through SparrowStream."""

    def test_pyarrow_roundtrip_single_batch(self):
        """Test PyArrow -> SparrowStream -> PyArrow round-trip."""
        # Create original data
        original_batch = pa.record_batch({"values": [10, 20, 30, None, 50]})
        reader = pa.RecordBatchReader.from_batches(original_batch.schema, [original_batch])

        # Import into SparrowStream
        stream = sr.SparrowStream.from_stream(reader)

        # Export back to PyArrow
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_batches = list(result_reader)

        assert len(result_batches) == 1
        result_batch = result_batches[0]

        # Verify data integrity
        assert result_batch.schema.equals(original_batch.schema)
        assert result_batch.num_rows == original_batch.num_rows

    def test_pyarrow_roundtrip_multiple_batches(self):
        """Test PyArrow -> SparrowStream -> PyArrow with multiple batches."""
        # Create original batches
        batch1 = pa.record_batch({"x": [1, 2, 3]})
        batch2 = pa.record_batch({"x": [4, 5, 6]})
        reader = pa.RecordBatchReader.from_batches(batch1.schema, [batch1, batch2])

        # Import into SparrowStream
        stream = sr.SparrowStream.from_stream(reader)

        # Export back to PyArrow
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_batches = list(result_reader)

        assert len(result_batches) == 2
        assert result_batches[0].num_rows == 3
        assert result_batches[1].num_rows == 3

    def test_pyarrow_table_roundtrip(self):
        """Test PyArrow Table -> SparrowStream -> PyArrow Table."""
        # Create original table
        original_table = pa.table({
            "integers": [1, 2, 3, 4, 5],
            "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
        })

        # Import into SparrowStream
        stream = sr.SparrowStream.from_stream(original_table)

        # Export to PyArrow RecordBatchReader
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_table = result_reader.read_all()

        # Tables may have different chunking, so compare column by column
        assert result_table.num_columns == original_table.num_columns
        assert result_table.num_rows == original_table.num_rows


class TestSparrowStreamWithDifferentTypes:
    """Test SparrowStream with various Arrow data types."""

    def test_integer_types(self):
        """Test stream with various integer types."""
        batch = pa.record_batch({
            "int8": pa.array([1, 2, 3], type=pa.int8()),
            "int16": pa.array([100, 200, 300], type=pa.int16()),
            "int32": pa.array([1000, 2000, 3000], type=pa.int32()),
            "int64": pa.array([10000, 20000, 30000], type=pa.int64()),
        })
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.batch_count() == 1
        capsule = stream.__arrow_c_stream__()
        assert capsule is not None

    def test_floating_point_types(self):
        """Test stream with floating point types."""
        batch = pa.record_batch({
            "float32": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
            "float64": pa.array([1.125, 2.25, 3.375], type=pa.float64()),
        })
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.batch_count() == 1

    def test_string_types(self):
        """Test stream with string types."""
        batch = pa.record_batch({
            "strings": pa.array(["hello", "world", "test"]),
        })
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.batch_count() == 1

    def test_with_nulls(self):
        """Test stream with null values."""
        batch = pa.record_batch({
            "nullable_ints": pa.array([1, None, 3, None, 5]),
            "nullable_strings": pa.array(["a", None, "c", None, "e"]),
        })
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        assert stream.batch_count() == 1


class TestSparrowStreamErrorHandling:
    """Test SparrowStream error handling."""

    def test_from_stream_invalid_object_raises(self):
        """Verify from_stream raises TypeError for invalid objects."""
        with pytest.raises(TypeError):
            sr.SparrowStream.from_stream("not a stream")

    def test_from_stream_none_raises(self):
        """Verify from_stream raises TypeError for None."""
        with pytest.raises(TypeError):
            sr.SparrowStream.from_stream(None)

    def test_from_stream_list_raises(self):
        """Verify from_stream raises TypeError for plain lists."""
        with pytest.raises(TypeError):
            sr.SparrowStream.from_stream([1, 2, 3])


class TestSparrowArrayNoStream:
    """Test that SparrowArray no longer has stream-related methods."""

    def test_sparrow_array_no_from_stream(self):
        """Verify SparrowArray does NOT have from_stream method."""
        assert not hasattr(sr.SparrowArray, "from_stream"), (
            "SparrowArray should NOT have from_stream method"
        )

    def test_sparrow_array_no_arrow_c_stream(self):
        """Verify SparrowArray does NOT have __arrow_c_stream__ method."""
        arr = sr.SparrowArray.from_arrow(pa.array([1, 2, 3]))
        assert not hasattr(arr, "__arrow_c_stream__"), (
            "SparrowArray should NOT have __arrow_c_stream__ method"
        )

    def test_sparrow_array_still_has_arrow_c_array(self):
        """Verify SparrowArray still has __arrow_c_array__ method."""
        arr = sr.SparrowArray.from_arrow(pa.array([1, 2, 3]))
        assert hasattr(arr, "__arrow_c_array__"), (
            "SparrowArray should still have __arrow_c_array__ method"
        )

    def test_sparrow_array_still_has_arrow_c_schema(self):
        """Verify SparrowArray still has __arrow_c_schema__ method."""
        arr = sr.SparrowArray.from_arrow(pa.array([1, 2, 3]))
        assert hasattr(arr, "__arrow_c_schema__"), (
            "SparrowArray should still have __arrow_c_schema__ method"
        )
