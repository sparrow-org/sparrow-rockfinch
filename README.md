# sparrow-rockfinch

The Sparrow Rockfinch Interface - A C++ library for exchanging Apache Arrow data between C++ and Python using the Arrow C Data Interface via PyCapsules.

## Overview

`sparrow-rockfinch` provides a clean C++ API for:
- Exporting sparrow arrays to Python as PyCapsules (Arrow C Data Interface)
- Importing Arrow data from Python PyCapsules into sparrow arrays
- Zero-copy data exchange with Python libraries like Polars, PyArrow, and pandas
- A `SparrowArray` Python class that implements the Arrow PyCapsule Interface

## Features

- ✅ **Zero-copy data exchange** between C++ and Python
- ✅ **Arrow C Data Interface** compliant
- ✅ **PyCapsule-based** for safe memory management
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
- **Accepts any ArrowArrayExportable** in its constructor (PyArrow, Polars, etc.)
- **Provides a `size()` method** to get the number of elements

```python
# Constructor accepts any object with __arrow_c_array__
sparrow_array = SparrowArray(pyarrow_array)
sparrow_array = SparrowArray(another_sparrow_array)

# Implements ArrowArrayExportable protocol
schema_capsule, array_capsule = sparrow_array.__arrow_c_array__()

# Get array size
n = sparrow_array.size()
```

## Testing

### C++ Unit Tests

```bash
cd build
./bin/Debug/test_sparrow_ROCKFINCH_lib
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
