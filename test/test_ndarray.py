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


# =============================================================================
# ndarray_view_from_sparrow / ndarray_copy_from_sparrow helpers
# =============================================================================
# These C++ helpers always go through the Arrow buffer → memoryview →
# np.frombuffer path, bypassing the numpy_owner fast path in to_numpy().
# They exercise the zero-copy view logic independently.


def _get_view_fn_name(helper_fn):
    """Return a human-readable name for a helper function."""
    return helper_fn.__name__


@pytest.mark.parametrize(
    "helper_fn",
    [
        test_sparrow_helper.create_primitive_int32_array,
        test_sparrow_helper.create_primitive_int64_array,
        test_sparrow_helper.create_primitive_float32_array,
        test_sparrow_helper.create_primitive_float64_array,
        test_sparrow_helper.create_primitive_uint32_array,
    ],
    ids=_get_view_fn_name,
)
def test_ndarray_view_from_sparrow_returns_readonly_non_owning_view(helper_fn):
    """ndarray_view_from_sparrow returns a read-only, non-owning ndarray."""
    sparrow_array = helper_fn()
    view = test_sparrow_helper.ndarray_view_from_sparrow(sparrow_array)

    assert isinstance(view, np.ndarray)
    assert not view.flags.writeable
    assert not view.flags.owndata
    assert view.base is not None  # backed by a memoryview


@pytest.mark.parametrize(
    "helper_fn",
    [
        test_sparrow_helper.create_primitive_int32_array,
        test_sparrow_helper.create_primitive_int64_array,
        test_sparrow_helper.create_primitive_float32_array,
        test_sparrow_helper.create_primitive_float64_array,
        test_sparrow_helper.create_primitive_uint32_array,
    ],
    ids=_get_view_fn_name,
)
def test_ndarray_view_from_sparrow_contains_correct_data(helper_fn):
    """ndarray_view_from_sparrow returns correct values."""
    sparrow_array = helper_fn()
    view = test_sparrow_helper.ndarray_view_from_sparrow(sparrow_array)

    # Compare against the reference to_numpy() result
    expected = sparrow_array.to_numpy()
    assert view.tolist() == expected.tolist()


def test_ndarray_view_from_sparrow_readonly_write_raises():
    """Writing to the read-only ndarray_view raises ValueError."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    view = test_sparrow_helper.ndarray_view_from_sparrow(sparrow_array)

    with pytest.raises((ValueError, RuntimeError), match="(read-only|not writable)"):
        view[0] = 99


def test_ndarray_view_from_sparrow_rejects_nullable():
    """ndarray_view_from_sparrow rejects nullable arrays."""
    sparrow_array = test_sparrow_helper.create_test_array()  # nullable int32

    with pytest.raises(TypeError, match="nullable"):
        test_sparrow_helper.ndarray_view_from_sparrow(sparrow_array)


def test_ndarray_view_from_sparrow_rejects_string_arrays():
    """ndarray_view_from_sparrow rejects non-primitive arrays."""
    sparrow_array = test_sparrow_helper.create_string_array()

    with pytest.raises(TypeError, match="primitive"):
        test_sparrow_helper.ndarray_view_from_sparrow(sparrow_array)


def test_ndarray_copy_from_sparrow_returns_writable_owning_array():
    """ndarray_copy_from_sparrow returns a writable, owning ndarray."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    copied = test_sparrow_helper.ndarray_copy_from_sparrow(sparrow_array)

    assert isinstance(copied, np.ndarray)
    assert copied.flags.writeable
    assert copied.flags.owndata


def test_ndarray_copy_from_sparrow_contains_correct_data():
    """ndarray_copy_from_sparrow returns correct values."""
    sparrow_array = test_sparrow_helper.create_primitive_float64_array()
    copied = test_sparrow_helper.ndarray_copy_from_sparrow(sparrow_array)

    assert copied.tolist() == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_ndarray_copy_from_sparrow_is_independent():
    """Mutating the copy does not affect the original SparrowArray."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    copied = test_sparrow_helper.ndarray_copy_from_sparrow(sparrow_array)

    copied[0] = 999

    # Original is unchanged
    assert np.asarray(sparrow_array).tolist() == [1, 2, 3, 4, 5]
    assert copied.tolist() == [999, 2, 3, 4, 5]


def test_ndarray_copy_from_sparrow_rejects_nullable():
    """ndarray_copy_from_sparrow rejects nullable arrays."""
    sparrow_array = test_sparrow_helper.create_test_array()

    with pytest.raises(TypeError, match="nullable"):
        test_sparrow_helper.ndarray_copy_from_sparrow(sparrow_array)


def test_ndarray_view_and_copy_have_different_buffers():
    """ndarray_view and ndarray_copy have distinct data pointers."""
    sparrow_array = test_sparrow_helper.create_primitive_int32_array()
    view = test_sparrow_helper.ndarray_view_from_sparrow(sparrow_array)
    copied = test_sparrow_helper.ndarray_copy_from_sparrow(sparrow_array)

    assert not np.shares_memory(view, copied)
    assert _data_pointer(view) != _data_pointer(copied)


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


@pytest.mark.parametrize(
    ("helper_fn", "values"),
    [
        (test_sparrow_helper.create_primitive_int32_array, [1, 2, 3, 4, 5]),
        (test_sparrow_helper.create_primitive_float64_array, [1.0, 2.0, 3.0, 4.0, 5.0]),
    ],
)
def test_ufunc_directly_on_sparrow_array(helper_fn, values):
    """NumPy ufuncs work directly on SparrowArray via __array__ auto-conversion."""
    sparrow_array = helper_fn()

    # Unary ufunc without explicit to_numpy()
    abs_result = np.abs(sparrow_array)
    assert isinstance(abs_result, np.ndarray)
    assert abs_result.tolist() == [abs(v) for v in values]

    if "float64" in helper_fn.__name__:
        sqrt_result = np.sqrt(sparrow_array)
        expected = np.sqrt(np.array(values, dtype=np.float64))
        assert np.allclose(sqrt_result, expected)

    # Binary ufunc without explicit to_numpy()
    double_result = np.add(sparrow_array, sparrow_array)
    assert double_result.tolist() == [v + v for v in values]


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


# =============================================================================
# sparrow_ndarray_interop — pure-Python ndarray view/copy from Arrow capsules
# =============================================================================

from sparrow_ndarray_interop import ndarray_view, ndarray_copy  # noqa: E402


@pytest.mark.parametrize(
    ("arrow_dtype", "values", "np_dtype"),
    [
        (pa.int8(), [1, 2, 3], np.int8),
        (pa.int16(), [10, 20, 30], np.int16),
        (pa.int32(), [1, 2, 3], np.int32),
        (pa.int64(), [10, 20, 30], np.int64),
        (pa.uint8(), [5, 6, 7], np.uint8),
        (pa.uint16(), [100, 200, 300], np.uint16),
        (pa.uint32(), [5, 6, 7], np.uint32),
        (pa.uint64(), [1000, 2000, 3000], np.uint64),
        (pa.float32(), [1.5, 2.5, 3.5], np.float32),
        (pa.float64(), [9.5, 8.5, 7.5], np.float64),
    ],
)
def test_ndarray_view_from_pyarrow(arrow_dtype, values, np_dtype):
    """ndarray_view creates correct ndarray from a PyArrow array."""
    pa_array = pa.array(values, type=arrow_dtype)
    view = ndarray_view(pa_array)

    assert isinstance(view, np.ndarray)
    assert view.dtype == np_dtype
    assert view.tolist() == values


def test_ndarray_view_rejects_bool_arrays():
    """ndarray_view rejects bool arrays because Arrow stores them bit-packed."""
    pa_array = pa.array([True, False, True])
    with pytest.raises(TypeError, match="bool arrays"):
        ndarray_view(pa_array)


@pytest.mark.parametrize(
    ("arrow_dtype", "values", "np_dtype"),
    [
        (pa.int8(), [1, 2, 3], np.int8),
        (pa.int32(), [1, 2, 3], np.int32),
        (pa.float64(), [9.5, 8.5, 7.5], np.float64),
    ],
)
def test_ndarray_view_is_readonly(arrow_dtype, values, np_dtype):
    """ndarray_view returns a read-only, non-owning array."""
    pa_array = pa.array(values, type=arrow_dtype)
    view = ndarray_view(pa_array)

    assert not view.flags.writeable
    assert not view.flags.owndata


@pytest.mark.parametrize(
    ("create_sparrow", "values"),
    [
        (test_sparrow_helper.create_primitive_int32_array, [1, 2, 3, 4, 5]),
        (test_sparrow_helper.create_primitive_float64_array, [1.0, 2.0, 3.0, 4.0, 5.0]),
    ],
)
def test_ndarray_view_from_sparrow_array(create_sparrow, values):
    """ndarray_view works with SparrowArray objects (which implement __arrow_c_array__)."""
    sparrow_array = create_sparrow()
    view = ndarray_view(sparrow_array)

    assert isinstance(view, np.ndarray)
    assert view.tolist() == values


def test_ndarray_view_from_ndarray_roundtrip():
    """ndarray_view works end-to-end: ndarray → SparrowArray → view."""
    source = np.array([10, 20, 30, 40, 50], dtype=np.int32)
    sparrow_array = SparrowArray.from_ndarray(source)
    view = ndarray_view(sparrow_array)

    assert view.dtype == source.dtype
    assert view.tolist() == source.tolist()
    assert not view.flags.writeable


def test_ndarray_view_raises_on_non_arrow_objects():
    """ndarray_view raises TypeError on objects without __arrow_c_array__."""
    with pytest.raises(TypeError, match="__arrow_c_array__"):
        ndarray_view(np.array([1, 2, 3]))


def test_ndarray_view_rejects_nullable_arrays():
    """ndarray_view raises TypeError on nullable Arrow arrays."""
    nullable = pa.array([1, None, 3], type=pa.int32())
    with pytest.raises(TypeError, match="nullable"):
        ndarray_view(nullable)


def test_ndarray_view_rejects_string_arrays():
    """ndarray_view raises TypeError on non-primitive Arrow arrays."""
    strings = pa.array(["hello", "world"])
    with pytest.raises(TypeError, match="Unsupported Arrow format"):
        ndarray_view(strings)


def test_ndarray_copy_from_pyarrow_is_writable():
    """ndarray_copy returns a writable, owning array."""
    pa_array = pa.array([1, 2, 3], type=pa.int32())
    copied = ndarray_copy(pa_array)

    assert copied.flags.writeable
    assert copied.flags.owndata


def test_ndarray_copy_is_independent():
    """Mutating ndarray_copy result does not affect any view of the same data."""
    pa_array = pa.array([1, 2, 3], type=pa.int32())
    view = ndarray_view(pa_array)
    copied = ndarray_copy(pa_array)

    copied[0] = 999

    assert view.tolist() == [1, 2, 3]
    assert copied.tolist() == [999, 2, 3]


def test_ndarray_view_stays_alive_after_source_gc():
    """The ndarray_view keeps the Arrow capsules alive independently."""
    import gc

    pa_array = pa.array([1, 2, 3], type=pa.int32())
    view = ndarray_view(pa_array)

    # Delete the source and force GC — the view must still be valid
    del pa_array
    gc.collect()

    assert view.tolist() == [1, 2, 3]
