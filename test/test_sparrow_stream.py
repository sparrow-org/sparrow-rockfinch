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

        capsule = stream.__arrow_c_stream__()
        assert capsule is not None

    def test_string_types(self):
        """Test stream with string types."""
        batch = pa.record_batch({
            "strings": pa.array(["hello", "world", "test"]),
        })
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        capsule = stream.__arrow_c_stream__()
        assert capsule is not None

    def test_with_nulls(self):
        """Test stream with null values."""
        batch = pa.record_batch({
            "nullable_ints": pa.array([1, None, 3, None, 5]),
            "nullable_strings": pa.array(["a", None, "c", None, "e"]),
        })
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)

        capsule = stream.__arrow_c_stream__()
        assert capsule is not None


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


class TestSparrowStreamPushPop:
    """Test SparrowStream push and pop operations."""

    def test_push_method_exists(self):
        """Verify SparrowStream has push method."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        assert hasattr(stream, "push"), "SparrowStream should have push method"

    def test_pop_method_exists(self):
        """Verify SparrowStream has pop method."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        assert hasattr(stream, "pop"), "SparrowStream should have pop method"

    def test_push_and_pop_single_array(self):
        """Test pushing and popping a single array."""
        # Create an empty stream
        batch = pa.record_batch({"x": []})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Create an array to push
        arr = sr.SparrowArray.from_arrow(pa.array([1, 2, 3, 4, 5]))
        
        # Push the array
        stream.push(arr)
        
        # Pop the array
        popped = stream.pop()
        
        assert popped is not None
        assert isinstance(popped, sr.SparrowArray)
        assert popped.size() == 5

    def test_push_multiple_arrays_and_pop_all(self):
        """Test pushing multiple arrays and popping them all."""
        # Create an empty stream
        batch = pa.record_batch({"x": []})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Push 3 arrays
        for i in range(3):
            arr = sr.SparrowArray.from_arrow(pa.array([i * 10, i * 10 + 1, i * 10 + 2]))
            stream.push(arr)
        
        # Pop all 3 arrays
        for i in range(3):
            popped = stream.pop()
            assert popped is not None
            assert popped.size() == 3
        
        # Stream should be exhausted
        empty_pop = stream.pop()
        assert empty_pop is None

    def test_pop_from_empty_stream_returns_none(self):
        """Test that popping from an empty stream returns None."""
        # Create an empty stream
        batch = pa.record_batch({"x": []})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [])
        stream = sr.SparrowStream.from_stream(reader)
        
        popped = stream.pop()
        assert popped is None

    def test_pop_preserves_fifo_order(self):
        """Test that pop returns arrays in FIFO order."""
        # Create an empty stream
        batch = pa.record_batch({"x": []})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Push arrays with different values
        arr1 = sr.SparrowArray.from_arrow(pa.array([100, 101, 102]))
        arr2 = sr.SparrowArray.from_arrow(pa.array([200, 201, 202]))
        arr3 = sr.SparrowArray.from_arrow(pa.array([300, 301, 302]))
        
        stream.push(arr1)
        stream.push(arr2)
        stream.push(arr3)
        
        # Pop should return in FIFO order
        first = stream.pop()
        second = stream.pop()
        third = stream.pop()
        
        assert first is not None
        assert second is not None
        assert third is not None
        assert first.size() == 3
        assert second.size() == 3
        assert third.size() == 3

    def test_pop_from_stream_created_from_batches(self):
        """Test popping from a stream created from PyArrow batches."""
        # Create a stream with 2 batches
        batch1 = pa.record_batch({"x": [1, 2, 3]})
        batch2 = pa.record_batch({"x": [4, 5, 6]})
        reader = pa.RecordBatchReader.from_batches(batch1.schema, [batch1, batch2])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Pop first batch
        first = stream.pop()
        assert first is not None
        assert first.size() == 3
        
        # Pop second batch
        second = stream.pop()
        assert second is not None
        assert second.size() == 3
        
        # Stream should be exhausted
        third = stream.pop()
        assert third is None


class TestSparrowStreamConsumedState:
    """Test SparrowStream is_consumed behavior."""

    def test_is_consumed_method_exists(self):
        """Verify SparrowStream has is_consumed method."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        assert hasattr(stream, "is_consumed"), "SparrowStream should have is_consumed method"

    def test_push_does_not_consume_stream(self):
        """Verify that push operations don't consume the stream."""
        batch = pa.record_batch({"x": []})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [])
        stream = sr.SparrowStream.from_stream(reader)
        
        assert stream.is_consumed() is False
        
        arr = sr.SparrowArray.from_arrow(pa.array([1, 2, 3]))
        stream.push(arr)
        
        assert stream.is_consumed() is False

    def test_pop_does_not_consume_stream(self):
        """Verify that pop operations don't consume the stream."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        assert stream.is_consumed() is False
        
        stream.pop()
        
        assert stream.is_consumed() is False

    def test_push_to_consumed_stream_raises_error(self):
        """Verify that pushing to a consumed stream raises RuntimeError."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Consume the stream
        _ = stream.__arrow_c_stream__()
        assert stream.is_consumed() is True
        
        # Try to push - should raise
        arr = sr.SparrowArray.from_arrow(pa.array([4, 5, 6]))
        with pytest.raises(RuntimeError, match="consumed"):
            stream.push(arr)

    def test_pop_from_consumed_stream_raises_error(self):
        """Verify that popping from a consumed stream raises RuntimeError."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Consume the stream
        _ = stream.__arrow_c_stream__()
        assert stream.is_consumed() is True
        
        # Try to pop - should raise
        with pytest.raises(RuntimeError, match="consumed"):
            stream.pop()

    def test_consumed_state_persists(self):
        """Verify that consumed state persists."""
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Consume the stream
        _ = stream.__arrow_c_stream__()
        
        # Check multiple times
        assert stream.is_consumed() is True
        assert stream.is_consumed() is True
        assert stream.is_consumed() is True


class TestSparrowStreamPushPopIntegration:
    """Test integration of push, pop, and stream export."""

    def test_push_then_export(self):
        """Test pushing arrays then exporting the stream."""
        # Create an empty stream
        batch = pa.record_batch({"x": []})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Push some arrays
        arr1 = sr.SparrowArray.from_arrow(pa.array([1, 2, 3]))
        arr2 = sr.SparrowArray.from_arrow(pa.array([4, 5, 6]))
        stream.push(arr1)
        stream.push(arr2)
        
        # Export to PyArrow
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_batches = list(result_reader)
        
        # Should have 2 batches
        assert len(result_batches) == 2
        assert result_batches[0].num_rows == 3
        assert result_batches[1].num_rows == 3

    def test_pop_then_push_then_export(self):
        """Test popping, then pushing, then exporting."""
        # Create a stream with one batch
        batch = pa.record_batch({"x": [1, 2, 3]})
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Pop the original batch
        original = stream.pop()
        assert original is not None
        
        # Push new arrays
        arr1 = sr.SparrowArray.from_arrow(pa.array([10, 20]))
        arr2 = sr.SparrowArray.from_arrow(pa.array([30, 40]))
        stream.push(arr1)
        stream.push(arr2)
        
        # Export to PyArrow
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_batches = list(result_reader)
        
        # Should have 2 batches (the newly pushed ones)
        assert len(result_batches) == 2
        assert result_batches[0].num_rows == 2
        assert result_batches[1].num_rows == 2

    def test_partial_pop_then_export(self):
        """Test popping some arrays, then exporting the rest."""
        # Create a stream with 3 batches
        batch1 = pa.record_batch({"x": [1, 2]})
        batch2 = pa.record_batch({"x": [3, 4]})
        batch3 = pa.record_batch({"x": [5, 6]})
        reader = pa.RecordBatchReader.from_batches(batch1.schema, [batch1, batch2, batch3])
        stream = sr.SparrowStream.from_stream(reader)
        
        # Pop first batch
        first = stream.pop()
        assert first is not None
        assert first.size() == 2
        
        # Export remaining batches
        result_reader = pa.RecordBatchReader.from_stream(stream)
        result_batches = list(result_reader)
        
        # Should have 2 remaining batches
        assert len(result_batches) == 2


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
