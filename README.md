# sparrow-pycapsule

The Sparrow PyCapsule Interface - A C++ library for exchanging Apache Arrow data between C++ and Python using the Arrow C Data Interface via PyCapsules.

## Overview

`sparrow-pycapsule` provides a clean C++ API for:
- Exporting sparrow arrays to Python as PyCapsules (Arrow C Data Interface)
- Importing Arrow data from Python PyCapsules into sparrow arrays
- Zero-copy data exchange with Python libraries like Polars, PyArrow, and pandas

## Features

- ✅ **Zero-copy data exchange** between C++ and Python
- ✅ **Arrow C Data Interface** compliant
- ✅ **PyCapsule-based** for safe memory management
- ✅ **Compatible with Polars, PyArrow, pandas** and other Arrow-based libraries
- ✅ **Bidirectional** data flow (C++ ↔ Python)
- ✅ **Type-safe** with proper ownership semantics

## Building

### Prerequisites

```bash
# Using conda (recommended)
conda env create -f environment-dev.yml
conda activate sparrow-pycapsule

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
cmake .. -DSPARROW_PYCAPSULE_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build .
ctest --output-on-failure
```

## Usage Example

### C++ Side: Exporting Data

```cpp
#include <sparrow-pycapsule/pycapsule.hpp>
#include <sparrow/array.hpp>

// Create a sparrow array
sparrow::array my_array = /* ... */;

// Export to PyCapsules for Python consumption
auto [schema_capsule, array_capsule] = 
    sparrow::pycapsule::export_array_to_capsules(my_array);

// Pass capsules to Python (via Python C API, pybind11, etc.)
```

### Python Side: Consuming C++ Data

```python
import polars as pl
import pyarrow as pa

# Receive capsules from C++
# schema_capsule, array_capsule = get_from_cpp()

# Import into PyArrow
arrow_array = pa.Array._import_from_c_capsule(schema_capsule, array_capsule)

# Convert to Polars
series = pl.from_arrow(arrow_array)

# Use in Polars DataFrame
df = pl.DataFrame({"my_column": series})
```

### Python Side: Exporting to C++

```python
import polars as pl

# Create Polars data
series = pl.Series([1, 2, None, 4, 5])

# Convert to Arrow and export as capsules
arrow_array = series.to_arrow()
schema_capsule, array_capsule = arrow_array.__arrow_c_array__()

# Pass to C++
```

### C++ Side: Importing from Python

```cpp
#include <sparrow-pycapsule/pycapsule.hpp>

// Receive capsules from Python
PyObject* schema_capsule = /* ... */;
PyObject* array_capsule = /* ... */;

// Import into sparrow array
sparrow::array imported_array = 
    sparrow::pycapsule::import_array_from_capsules(
        schema_capsule, array_capsule);

// Use the array
std::cout << "Array size: " << imported_array.size() << std::endl;
```

## Testing

### C++ Unit Tests

```bash
cd build
./bin/Debug/test_sparrow_pycapsule_lib
```

### Polars Integration Tests

Test bidirectional data exchange with Polars:

```bash

# Or with direct execution (better output)
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
| `run_polars_tests_direct` | Run Polars test directly (recommended, better output) |
| `check_polars_deps` | Check Python dependencies (polars, pyarrow) |

**Usage:**
```bash
cd build

# Run Polars integration tests
cmake --build . --target run_polars_tests_direct

# Check dependencies first
cmake --build . --target check_polars_deps
```

### Debugging Test Failures

If you encounter segmentation faults or other issues:

```bash
cd build

# Run minimal library loading test (step-by-step debugging)
cmake --build . --target test_library_load

# Check that libraries exist and dependencies are correct
cmake --build . --target check_polars_deps
```

## API Reference

### Export Functions

- `export_arrow_schema_pycapsule(array& arr)` - Export schema to PyCapsule
- `export_arrow_array_pycapsule(array& arr)` - Export array data to PyCapsule
- `export_array_to_capsules(array& arr)` - Export both schema and array (recommended)

### Import Functions

- `get_arrow_schema_pycapsule(PyObject* capsule)` - Get ArrowSchema pointer from capsule
- `get_arrow_array_pycapsule(PyObject* capsule)` - Get ArrowArray pointer from capsule
- `import_array_from_capsules(PyObject* schema, PyObject* array)` - Import complete array

### Memory Management

- `release_arrow_schema_pycapsule(PyObject* capsule)` - PyCapsule destructor for schema
- `release_arrow_array_pycapsule(PyObject* capsule)` - PyCapsule destructor for array

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
series = pl.Series([1, 2, 3])
arrow_array = series.to_arrow()
schema_capsule, array_capsule = arrow_array.__arrow_c_array__()
# Pass to C++
```

### PyArrow
```python
arrow_array = pa.array([1, 2, 3])
schema_capsule, array_capsule = arrow_array.__arrow_c_array__()
# Pass to C++
```

### pandas (via PyArrow)
```python
import pandas as pd
series = pd.Series([1, 2, 3])
arrow_array = pa.Array.from_pandas(series)
schema_capsule, array_capsule = arrow_array.__arrow_c_array__()
# Pass to C++
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
