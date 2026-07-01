# sparrow-rockfinch

The Sparrow Rockfinch Interface - A C++ library for exchanging Apache Arrow data between C++ and Python using the Arrow C Data Interface via PyCapsules.

## Overview

`sparrow-rockfinch` provides a clean C++ API for:
- Exporting sparrow arrays to Python as PyCapsules (Arrow C Data Interface)
- Importing Arrow data from Python PyCapsules into sparrow arrays
- Importing/exporting primitive 1D NumPy ndarrays
- Zero-copy data exchange with Python libraries like Polars, PyArrow, and pandas
- A `SparrowArray` Python class that implements the Arrow PyCapsule Interface

## Features

- ✅ **Zero-copy data exchange** between C++ and Python
- ✅ **Arrow C Data Interface** compliant
- ✅ **PyCapsule-based** for safe memory management
- ✅ **NumPy ndarray interop** for primitive 1D arrays
- ✅ **Compatible with Polars, PyArrow, pandas** and other Arrow-based libraries
- ✅ **Bidirectional** data flow (C++ ↔ Python)
- ✅ **Type-safe** with proper ownership semantics
- ✅ **SparrowArray Python class** implementing `__arrow_c_array__` protocol

## Building

### Prerequisites

```bash
# Using conda (recommended)
conda env create -f environment-dev.yml
conda activate sparrow-rockfinch

# Or install manually
# - CMake >= 3.28
# - C++20 compiler
# - Python 3.x with development headers
# - sparrow library
```

### Build Instructions

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

### Build with Tests

```bash
mkdir build && cd build
cmake .. -DSPARROW_ROCKFINCH_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build .
ctest --output-on-failure
```

## Usage Example

### C++ Side: Creating a SparrowArray for Python

```cpp
#include <sparrow-rockfinch/pycapsule.hpp>
#include <sparrow-rockfinch/sparrow_array_python_class.hpp>
#include <sparrow/array.hpp>

// Create a sparrow array
sparrow::array my_array = /* ... */;

// Create a SparrowArray Python object that implements __arrow_c_array__
PyObject* sparrow_array = sparrow::pycapsule::create_sparrow_array_object(std::move(my_array));

// Return to Python - it can be used directly with Polars, PyArrow, etc.
```

### Python Side: Using SparrowArray

```python
from test_sparrow_helper import SparrowArray
import polars as pl
import pyarrow as pa

# Create SparrowArray from any Arrow-compatible object
pa_array = pa.array([1, 2, None, 4, 5], type=pa.int32())
sparrow_array = SparrowArray(pa_array)

# SparrowArray implements __arrow_c_array__, so it works with Polars
# Using Polars internal API for primitive arrays:
from polars._plr import PySeries
from polars._utils.wrap import wrap_s

ps = PySeries.from_arrow_c_array(sparrow_array)
series = wrap_s(ps)
print(series)  # shape: (5,), dtype: Int32

# Get array size
print(sparrow_array.size())  # 5
```

### Python Side: Exporting to C++

```python
import pyarrow as pa

# Any object implementing __arrow_c_array__ can be imported by sparrow
arrow_array = pa.array([1, 2, None, 4, 5])

# The SparrowArray constructor accepts any ArrowArrayExportable
sparrow_array = SparrowArray(arrow_array)
```

### Python Side: NumPy Interop

`SparrowArray` supports bidirectional zero-copy exchange with NumPy for all primitive
numeric dtypes. Bool arrays are handled as a special case (copied due to Sparrow's
bit-packed internal representation).

#### Supported NumPy dtypes

| Category | Dtypes |
|----------|--------|
| Signed integers | `int8`, `int16`, `int32`, `int64` |
| Unsigned integers | `uint8`, `uint16`, `uint32`, `uint64` |
| Floating point | `float32`, `float64` |
| Boolean | `bool` (copied, not zero-copy) |

#### `from_ndarray` — Import a NumPy array into Sparrow

```python
import numpy as np
import sparrow_rockfinch as sp

source = np.array([1, 2, 3, 4], dtype=np.int32)
sparrow_array = sp.SparrowArray.from_ndarray(source)

# The SparrowArray holds a reference to the original ndarray.
# Mutations to the source are visible through the SparrowArray:
source[0] = 42
print(np.asarray(sparrow_array))  # [42, 2, 3, 4]
```

`from_ndarray` is zero-copy for all numeric dtypes: the Arrow internal buffer points
directly to the NumPy data. Writable ndarrays produce writable Arrow buffers; read-only
ndarrays produce read-only Arrow buffers.

**Input requirements:**
- Must be a **1D** contiguous ndarray (C order, no slicing strides)
- Multidimensional and non-contiguous arrays raise `ValueError`
- Unsupported dtypes (complex, object, etc.) raise `TypeError`

#### `to_numpy` — Export a SparrowArray to NumPy

```python
# Zero-copy view (default) — returns the original ndarray for ndarray-backed arrays
sparrow_array = sp.SparrowArray.from_ndarray(np.array([1, 2, 3], dtype=np.float64))
view = sparrow_array.to_numpy()
assert view is source          # same Python object
assert np.shares_memory(view, source)

# In-place mutations on the view propagate back to the SparrowArray:
view[0] = 99
print(np.asarray(sparrow_array))  # [99., 2., 3.]

# copy=True allocates an independent writable copy:
copy = sparrow_array.to_numpy(copy=True)
assert not np.shares_memory(copy, source)
```

**Zero-copy path:**
- **ndarray-backed arrays**: returns the **exact same Python object** as the source (`is` identity)
- **Arrow-imported arrays** (via `from_arrow`): returns a **read-only memoryview** over the Arrow buffer

**Copy path:**
- `copy=True` always allocates a fresh writable array
- `bool` arrays are always copied (Sparrow stores them bit-packed)
- Nullable integer/bool arrays are **rejected** by `to_numpy()`

#### `__array__` protocol — NumPy integration via `np.asarray`

```python
sparrow_array = sp.SparrowArray.from_ndarray(np.array([1, 2, 3], dtype=np.int32))
result = np.asarray(sparrow_array)  # delegates to to_numpy(copy=False)
assert result is source
```

`SparrowArray` implements the `__array__` protocol so that `np.asarray()`,
`np.array()`, and other NumPy functions see it as a compatible array-like object.
Dtype coercion requests are rejected — `np.asarray(sparrow_array, dtype=np.float64)`
raises `TypeError`.

#### Read‑only vs writable views

```python
# Read-only source → read-only view
source = np.array([1, 2, 3], dtype=np.int32)
source.flags.writeable = False
sparrow_array = sp.SparrowArray.from_ndarray(source)

view = sparrow_array.to_numpy()
assert not view.flags.writeable
# view[0] = 99  # ValueError: assignment destination is read-only

# Arrow-imported arrays export as read-only memoryviews:
import pyarrow as pa
sparrow_array = sp.SparrowArray.from_arrow(pa.array([1, 2, 3], type=pa.int32()))
view = sparrow_array.to_numpy()
assert not view.flags.writeable
```

#### Limitations

- Only **1D contiguous** ndarrays are accepted by `from_ndarray()`
- Only **primitive types** are exportable via `to_numpy()` (no strings, lists, structs)
- `bool` is **always copied** because Sparrow stores it bit-packed
- **Nullable integer / bool** arrays are **rejected** by `to_numpy()`
- **Nullable float** arrays are exported as copies with `NaN` sentinels for null values
- `float16` / half-float arrays are not yet supported

### C++ Side: Importing from Python

```cpp
#include <sparrow-rockfinch/pycapsule.hpp>

// Receive capsules from Python (e.g., from __arrow_c_array__)
PyObject* schema_capsule = /* ... */;
PyObject* array_capsule = /* ... */;

// Import into sparrow array
sparrow::array imported_array = 
    sparrow::pycapsule::import_array_from_capsules(
        schema_capsule, array_capsule);

// Use the array
std::cout << "Array size: " << imported_array.size() << std::endl;
```

## SparrowArray Python Class

The `SparrowArray` class is a Python type implemented in C++ that:

- **Wraps a sparrow array** and exposes it to Python
- **Implements `__arrow_c_array__`** (ArrowArrayExportable protocol)
- **Accepts any ArrowArrayExportable** via `from_arrow()` (PyArrow, Polars, etc.)
- **Accepts primitive 1D NumPy ndarrays** via `from_ndarray()` (zero-copy)
- **Exports to NumPy** via `to_numpy()` / `__array__()` (see [NumPy Interop](#python-side-numpy-interop))
- **Provides a `size()` method** to get the number of elements

```python
import sparrow_rockfinch as sp
import numpy as np

# Create from Arrow-compatible objects
sparrow_array = sp.SparrowArray.from_arrow(pyarrow_array)

# Create from NumPy (zero-copy for numeric dtypes)
sparrow_array = sp.SparrowArray.from_ndarray(np.array([1, 2, 3], dtype=np.int32))

# Export to NumPy (zero-copy view when possible)
view = sparrow_array.to_numpy()

# NumPy array protocol integration
result = np.asarray(sparrow_array)

# Arrow PyCapsule interface
schema_capsule, array_capsule = sparrow_array.__arrow_c_array__()

# Get array size
n = sparrow_array.size()
```

## Testing

### C++ Unit Tests

```bash
cd build
./bin/Debug/test_sparrow_rockfinch_lib
```

### Integration Tests

Test bidirectional data exchange with Polars and PyArrow:

```bash
# Run integration tests (recommended)
cmake --build . --target run_polars_tests_direct

# Check dependencies first
cmake --build . --target check_polars_deps
```

See [test/README_POLARS_TESTS.md](test/README_POLARS_TESTS.md) for detailed documentation.

## CMake Targets

The project provides several convenient CMake targets for testing:

| Target | Description |
|--------|-------------|
| `run_tests` | Run all C++ unit tests |
| `run_tests_with_junit_report` | Run C++ tests with JUnit XML output |
| `run_polars_tests_direct` | Run integration tests (recommended) |
| `check_polars_deps` | Check Python dependencies (polars, pyarrow) |
| `test_library_load` | Debug library loading issues |

**Usage:**
```bash
cd build

# Run integration tests
cmake --build . --target run_polars_tests_direct

# Check dependencies first
cmake --build . --target check_polars_deps
```

## API Reference

### SparrowArray Python Class

```cpp
// Create a SparrowArray Python object from a sparrow::array
PyObject* create_sparrow_array_object(sparrow::array&& arr);

// Create a SparrowArray from PyCapsules
PyObject* create_sparrow_array_object_from_capsules(
    PyObject* schema_capsule, PyObject* array_capsule);

// Register SparrowArray type with a Python module
int register_sparrow_array_type(PyObject* module);

// Get the SparrowArray type object
PyTypeObject* get_sparrow_array_type();
```

### Export Functions

- `export_arrow_schema_pycapsule(array& arr)` - Export schema to PyCapsule
- `export_arrow_array_pycapsule(array& arr)` - Export array data to PyCapsule
- `export_array_to_capsules(array& arr)` - Export both schema and array (recommended)

### Import Functions

- `get_arrow_schema_pycapsule(PyObject* capsule)` - Get ArrowSchema pointer from capsule
- `get_arrow_array_pycapsule(PyObject* capsule)` - Get ArrowArray pointer from capsule
- `import_array_from_capsules(PyObject* schema, PyObject* array)` - Import complete array

### Memory Management

All capsules have destructors that properly clean up Arrow structures.

## Supported Data Types

The library supports all Arrow data types that sparrow supports:
- Integer types (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64)
- Floating point (Float32, Float64)
- Boolean
- String (UTF-8)
- And more...

All types support nullable values via the Arrow null bitmap.

## Integration with Python Libraries

### Polars

```python
from polars._plr import PySeries
from polars._utils.wrap import wrap_s

# SparrowArray implements __arrow_c_array__, use Polars internal API
sparrow_array = SparrowArray(some_arrow_array)
ps = PySeries.from_arrow_c_array(sparrow_array)
series = wrap_s(ps)
```

### PyArrow

```python
import pyarrow as pa

# Create SparrowArray from PyArrow
pa_array = pa.array([1, 2, 3])
sparrow_array = SparrowArray(pa_array)

# Export back to PyArrow
schema_capsule, array_capsule = sparrow_array.__arrow_c_array__()
```

### pandas (via PyArrow)

```python
import pandas as pd
import pyarrow as pa

series = pd.Series([1, 2, 3])
arrow_array = pa.Array.from_pandas(series)
sparrow_array = SparrowArray(arrow_array)
```

## License

See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please ensure:
- Code follows the existing style
- All tests pass (`ctest --output-on-failure`)
- New features include tests
- Documentation is updated

## Related Projects

- [sparrow](https://github.com/man-group/sparrow) - Modern C++ library for Apache Arrow
- [Apache Arrow](https://arrow.apache.org/) - Cross-language development platform
- [Polars](https://www.pola.rs/) - Fast DataFrame library
