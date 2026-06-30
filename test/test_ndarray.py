from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from sparrow_helpers import SparrowArray

try:
    import test_sparrow_helper  # noqa: E402
except ImportError:
    import test_sparrow_helperd as test_sparrow_helper  # noqa: E402


@pytest.mark.parametrize(
    ("dtype", "values"),
    [
        (np.int8, [1, 2, 3, 4]),
        (np.int16, [10, 20, 30, 40]),
        (np.int32, [1, 2, 3, 4]),
        (np.int64, [10, 20, 30, 40]),
        (np.uint8, [5, 6, 7, 8]),
        (np.uint16, [100, 200, 300, 400]),
        (np.uint32, [5, 6, 7, 8]),
        (np.uint64, [1000, 2000, 3000, 4000]),
        (np.float32, [1.5, 2.5, 3.5, 4.5]),
        (np.float64, [9.5, 8.5, 7.5, 6.5]),
        (np.bool_, [True, False, True, True]),
    ],
)
def test_from_ndarray_roundtrip(dtype, values):
    source = np.array(values, dtype=dtype)

    sparrow_array = SparrowArray.from_ndarray(source)
    result = np.asarray(sparrow_array)

    assert result.dtype == source.dtype
    assert result.tolist() == source.tolist()


def test_to_numpy_zero_copy_view_for_non_nullable_numeric_arrays():
    source = np.array([1, 2, 3], dtype=np.int32)
    sparrow_array = SparrowArray.from_ndarray(source)

    exported = sparrow_array.to_numpy()

    assert exported is source
    assert exported.__array_interface__["data"][0] == source.__array_interface__["data"][0]
    assert np.shares_memory(exported, source)

    exported[0] = 99
    assert source.tolist() == [99, 2, 3]


def test_from_ndarray_zero_copy_import_reflects_source_mutation():
    source = np.array([1, 2, 3], dtype=np.int32)
    sparrow_array = SparrowArray.from_ndarray(source)
    exported = sparrow_array.to_numpy()

    assert exported is source
    assert _data_pointer(exported) == _data_pointer(source)
    assert np.shares_memory(exported, source)

    source[1] = 42

    roundtrip = np.asarray(sparrow_array)
    assert roundtrip is source
    assert _data_pointer(roundtrip) == _data_pointer(source)
    assert np.shares_memory(roundtrip, source)
    assert roundtrip.tolist() == [1, 42, 3]


def test_from_ndarray_readonly_source_exports_readonly_view():
    source = np.array([1, 2, 3], dtype=np.int32)
    source.flags.writeable = False

    sparrow_array = SparrowArray.from_ndarray(source)
    exported = sparrow_array.to_numpy()

    assert exported is source
    assert _data_pointer(exported) == _data_pointer(source)
    assert np.shares_memory(exported, source)
    assert not exported.flags.writeable
    with pytest.raises(ValueError, match="read-only"):
        exported[0] = 99


def test_to_numpy_copy_true_returns_independent_array():
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()

    copied = sparrow_array.to_numpy(copy=True)
    copied[0] = 77

    roundtrip = np.asarray(sparrow_array)
    assert copied.tolist() == [77, 2, 3, 4, 5]
    assert roundtrip.tolist() == [1, 2, 3, 4, 5]


# =============================================================================
# Zero-copy buffer identity tests
# =============================================================================


def _data_pointer(arr: np.ndarray) -> int:
    """Return the raw data pointer of a numpy array."""
    return arr.__array_interface__["data"][0]


@pytest.mark.parametrize(
    "dtype",
    [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.float32, np.float64],
)
def test_to_numpy_returns_same_object_for_ndarray_backed_array(dtype):
    """to_numpy() returns the exact same Python object for ndarray-backed SparrowArrays."""
    source = np.array([1, 2, 3], dtype=dtype)
    sparrow_array = SparrowArray.from_ndarray(source)

    exported = sparrow_array.to_numpy()

    # Same Python object — not just same buffer, literally the same ndarray
    assert exported is source
    assert _data_pointer(exported) == _data_pointer(source)
    assert np.shares_memory(exported, source)


@pytest.mark.parametrize(
    "dtype",
    [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.float32, np.float64],
)
def test_roundtrip_preserves_buffer_identity(dtype):
    """SparrowArray.from_ndarray → to_numpy returns the very same ndarray object."""
    source = np.array([1, 2, 3], dtype=dtype)
    sparrow_array = SparrowArray.from_ndarray(source)

    # np.asarray calls __array__ which delegates to to_numpy(copy=False)
    result = np.asarray(sparrow_array)

    assert result is source
    assert _data_pointer(result) == _data_pointer(source)
    assert np.shares_memory(result, source)


@pytest.mark.parametrize(
    "dtype",
    [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.float32, np.float64],
)
def test_from_ndarray_shares_memory_with_source(dtype):
    """from_ndarray() keeps the source ndarray alive as backing storage."""
    source = np.array([10, 20, 30], dtype=dtype)
    sparrow_array = SparrowArray.from_ndarray(source)

    # Mutate the source — SparrowArray sees it because it points to the same buffer
    source[0] = 99

    exported = sparrow_array.to_numpy()
    assert _data_pointer(exported) == _data_pointer(source)
    assert exported[0] == 99
    assert np.shares_memory(exported, source)


def test_bool_from_ndarray_is_not_zero_copy():
    """Bool arrays are copied because Sparrow stores them bit-packed."""
    source = np.array([True, False, True], dtype=np.bool_)
    sparrow_array = SparrowArray.from_ndarray(source)

    exported = sparrow_array.to_numpy()

    # Bool is copied internally — exported is not the same object
    assert exported is not source
    # But values roundtrip correctly
    assert exported.tolist() == source.tolist()


def test_bool_from_ndarray_mutation_does_not_propagate():
    """Mutating the source bool ndarray does not affect the SparrowArray."""
    source = np.array([True, False, True], dtype=np.bool_)
    sparrow_array = SparrowArray.from_ndarray(source)

    source[0] = False

    # SparrowArray has its own bit-packed copy — unaffected by source mutation
    assert np.asarray(sparrow_array).tolist() == [True, False, True]


def test_to_numpy_copy_true_has_different_buffer():
    """to_numpy(copy=True) allocates a fresh buffer unrelated to the source."""
    source = np.array([1, 2, 3], dtype=np.int32)
    sparrow_array = SparrowArray.from_ndarray(source)

    copied = sparrow_array.to_numpy(copy=True)

    assert copied is not source
    assert not np.shares_memory(copied, source)
    assert _data_pointer(copied) != _data_pointer(source)


def test_from_arrow_to_numpy_is_zero_copy_readonly_view():
    """to_numpy() on an Arrow-imported primitive array returns a readonly view."""
    arrow_array = pa.array([1, 2, 3], type=pa.int32())
    sparrow_array = SparrowArray.from_arrow(arrow_array)

    exported = sparrow_array.to_numpy()
    copied = sparrow_array.to_numpy(copy=True)

    assert exported.base is not None
    assert not exported.flags.owndata
    assert not exported.flags.writeable
    assert exported.tolist() == [1, 2, 3]
    assert copied.flags.writeable
    assert copied.flags.owndata
    assert not np.shares_memory(copied, exported)
    assert copied.__array_interface__["data"][0] != exported.__array_interface__["data"][0]


def test_readonly_source_still_returns_same_object():
    """Even a readonly source ndarray is returned as-is by to_numpy()."""
    source = np.array([1, 2, 3], dtype=np.float64)
    source.flags.writeable = False

    sparrow_array = SparrowArray.from_ndarray(source)
    exported = sparrow_array.to_numpy()

    assert exported is source
    assert _data_pointer(exported) == _data_pointer(source)
    assert np.shares_memory(exported, source)
    assert not exported.flags.writeable


def test_from_ndarray_then_to_numpy_preserves_writability():
    """A writable source stays writable through the roundtrip."""
    source = np.array([1, 2, 3], dtype=np.int32)
    assert source.flags.writeable

    sparrow_array = SparrowArray.from_ndarray(source)
    exported = sparrow_array.to_numpy()

    assert exported is source
    assert _data_pointer(exported) == _data_pointer(source)
    assert np.shares_memory(exported, source)
    assert exported.flags.writeable


def test_from_ndarray_rejects_multidimensional_arrays():
    source = np.arange(6, dtype=np.int32).reshape(2, 3)

    with pytest.raises(ValueError, match="1D ndarrays"):
        SparrowArray.from_ndarray(source)


def test_from_ndarray_rejects_non_contiguous_arrays():
    source = np.arange(8, dtype=np.int32)[::2]

    with pytest.raises(ValueError, match="contiguous 1D ndarray"):
        SparrowArray.from_ndarray(source)


def test_from_ndarray_rejects_unsupported_dtypes():
    source = np.array([1 + 2j, 3 + 4j], dtype=np.complex64)

    with pytest.raises(TypeError, match="Unsupported ndarray dtype"):
        SparrowArray.from_ndarray(source)


def test_nullable_integer_export_is_rejected():
    sparrow_array = test_sparrow_helper.create_test_array()

    with pytest.raises(TypeError, match="nullable integer arrays"):
        sparrow_array.to_numpy()


def test_nullable_bool_export_is_rejected():
    sparrow_array = test_sparrow_helper.create_nullable_bool_array()

    with pytest.raises(TypeError, match="nullable bool arrays"):
        sparrow_array.to_numpy()


def test_non_primitive_export_is_rejected():
    sparrow_array = test_sparrow_helper.create_string_array()

    with pytest.raises(TypeError, match="primitive 1D Sparrow arrays"):
        sparrow_array.to_numpy()


def test_from_arrow_numeric_export_is_readonly():
    sparrow_array = SparrowArray.from_arrow(pa.array([1, 2, 3], type=pa.int32()))

    exported = sparrow_array.to_numpy()

    assert exported.tolist() == [1, 2, 3]
    assert not exported.flags.writeable
    with pytest.raises(ValueError, match="read-only"):
        exported[0] = 99


def test_array_protocol_rejects_dtype_coercion():
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()

    with pytest.raises(TypeError, match="does not support dtype conversion"):
        sparrow_array.__array__(dtype=np.dtype(np.float64))


# =============================================================================
# NumPy operations on exported arrays
# =============================================================================


@pytest.mark.parametrize(
    ("dtype", "values"),
    [
        (np.int8, [1, 2, 3, 4]),
        (np.int16, [10, 20, 30, 40]),
        (np.int32, [1, 2, 3, 4]),
        (np.int64, [10, 20, 30, 40]),
        (np.uint8, [5, 6, 7, 8]),
        (np.uint16, [100, 200, 300, 400]),
        (np.uint32, [5, 6, 7, 8]),
        (np.uint64, [1000, 2000, 3000, 4000]),
        (np.float32, [1.5, 2.5, 3.5, 4.5]),
        (np.float64, [9.5, 8.5, 7.5, 6.5]),
    ],
)
def test_numpy_arithmetic_on_exported_view(dtype, values):
    """Arithmetic on an exported zero-copy view mutates the SparrowArray."""
    sparrow_array = SparrowArray.from_ndarray(np.array(values, dtype=dtype))
    exported = sparrow_array.to_numpy()

    exported += np.array([1] * len(values), dtype=dtype)

    expected = [v + 1 for v in values]
    assert np.asarray(sparrow_array).tolist() == expected


def test_numpy_in_place_multiplication_on_view():
    """In-place multiplication on exported view reflects in SparrowArray."""
    sparrow_array = SparrowArray.from_ndarray(np.array([2, 4, 6], dtype=np.int32))
    exported = sparrow_array.to_numpy()

    exported *= 3

    assert np.asarray(sparrow_array).tolist() == [6, 12, 18]


@pytest.mark.parametrize(
    ("helper_fn", "values", "expected_sum"),
    [
        (test_sparrow_helper.create_primitive_int32_array, [1, 2, 3, 4, 5], 15),
        (test_sparrow_helper.create_primitive_int64_array, [10, 20, 30, 40, 50], 150),
        (test_sparrow_helper.create_primitive_float32_array, [1.5, 2.5, 3.5, 4.5, 5.5], 17.5),
        (test_sparrow_helper.create_primitive_float64_array, [1.0, 2.0, 3.0, 4.0, 5.0], 15.0),
        (test_sparrow_helper.create_primitive_uint32_array, [5, 6, 7, 8, 9], 35),
    ],
)
def test_numpy_reductions_on_exported_array(helper_fn, values, expected_sum):
    """Reductions (sum, min, max) work on exported numpy arrays."""
    sparrow_array = helper_fn()
    exported = sparrow_array.to_numpy()

    assert exported.sum() == expected_sum
    assert exported.min() == min(values)
    assert exported.max() == max(values)


def test_numpy_mean_and_std_on_exported_array():
    """Mean and standard deviation work on exported arrays."""
    sparrow_array = test_sparrow_helper.create_primitive_float64_array()
    exported = sparrow_array.to_numpy()

    assert exported.mean() == 3.0
    assert np.isclose(exported.std(), np.std([1.0, 2.0, 3.0, 4.0, 5.0]))


@pytest.mark.parametrize(
    ("helper_fn", "values"),
    [
        (test_sparrow_helper.create_primitive_int32_array, [1, 2, 3, 4, 5]),
        (test_sparrow_helper.create_primitive_float64_array, [1.0, 2.0, 3.0, 4.0, 5.0]),
    ],
)
def test_numpy_ufunc_on_exported_array(helper_fn, values):
    """NumPy universal functions (sqrt, abs, add) work on exported arrays."""
    sparrow_array = helper_fn()
    exported = sparrow_array.to_numpy()

    abs_result = np.abs(exported)
    assert abs_result.tolist() == [abs(v) for v in values]

    if "float64" in helper_fn.__name__:
        sqrt_result = np.sqrt(exported)
        expected = np.sqrt(np.array(values, dtype=np.float64))
        assert np.allclose(sqrt_result, expected)


def test_numpy_slicing_on_exported_array():
    """Slicing the exported numpy array returns correct values."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    exported = sparrow_array.to_numpy()

    assert exported[1:4].tolist() == [2, 3, 4]
    assert exported[::2].tolist() == [1, 3, 5]
    assert exported[-1] == 5


def test_numpy_boolean_masking_on_exported_array():
    """Boolean masking works on exported numpy arrays."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    exported = sparrow_array.to_numpy()

    mask = exported > 2
    assert exported[mask].tolist() == [3, 4, 5]


def test_numpy_element_wise_comparison():
    """Element-wise comparison returns correct boolean array."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    exported = sparrow_array.to_numpy()

    result = exported == 3
    assert result.tolist() == [False, False, True, False, False]


def test_numpy_math_operations_on_copied_array_do_not_affect_source():
    """Math operations on a copied array don't mutate the original SparrowArray."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    copied = sparrow_array.to_numpy(copy=True)

    copied[0] = 99
    copied += 10

    assert copied.tolist() == [109, 12, 13, 14, 15]
    assert np.asarray(sparrow_array).tolist() == [1, 2, 3, 4, 5]


def test_numpy_broadcast_addition():
    """Broadcast addition of a scalar to an exported view works."""
    sparrow_array = test_sparrow_helper.create_primitive_float64_array()
    exported = sparrow_array.to_numpy()

    result = exported + 10.0

    assert result.tolist() == [11.0, 12.0, 13.0, 14.0, 15.0]
    assert np.asarray(sparrow_array).tolist() == [1.0, 2.0, 3.0, 4.0, 5.0]
