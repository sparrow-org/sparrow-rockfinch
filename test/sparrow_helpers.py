"""
Python helper module for sparrow-rockfinch integration tests.

This module provides helper functions that wrap the C++ SparrowArray class,
making it easy to create test arrays and perform roundtrip operations.
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Any, Protocol, Tuple
import importlib.util


class ArrowArrayExportable(Protocol):
    """Protocol for objects implementing the Arrow PyCapsule Interface."""
    
    def __arrow_c_array__(
        self, requested_schema: Any = None
    ) -> Tuple[Any, Any]:
        """Export the array as Arrow PyCapsules.
        
        Returns
        -------
        Tuple[Any, Any]
            A tuple of (schema_capsule, array_capsule).
        """
        ...


class SparrowArrayType(ArrowArrayExportable, Protocol):
    """Type definition for SparrowArray from C++ extension."""
    
    def size(self) -> int:
        """Get the number of elements in the array."""
        ...
    
    @classmethod
    def from_arrow(cls, arrow_array: ArrowArrayExportable) -> "SparrowArrayType":
        """Create a SparrowArray from an Arrow-compatible object."""
        ...


def _get_module_name_from_path(file_path: Path) -> str:
    """Extract the module name from a .so/.pyd file path.
    
    Handles various naming patterns:
    - Linux: 'sparrow_rockfinchd.cpython-312-x86_64-linux-gnu.so' -> 'sparrow_rockfinchd'
    - Windows: 'sparrow_rockfinch.cp314-win_amd64.pyd' -> 'sparrow_rockfinch'
    - Simple: 'module.so' or 'module.pyd' -> 'module'
    """
    name = file_path.name
    # The module name is always the part before the first dot
    # This handles all patterns: name.cpython-..., name.cp314-..., name.so, name.pyd
    if '.' in name:
        name = name.split('.')[0]
    return name


def _load_module_from_path(module_name: str, file_path: Path):
    """Load a Python extension module from a file path.
    
    The module_name should match the PyInit_<name> function in the compiled module.
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    raise ImportError(f"Could not load {module_name} from {file_path}")


def _setup_modules() -> None:
    """Set up the sparrow_rockfinch and test_sparrow_helper modules."""
    # Load the main sparrow_rockfinch module
    sparrow_path = os.environ.get('SPARROW_MODULE_PATH')
    if sparrow_path:
        sparrow_file = Path(sparrow_path)
        if sparrow_file.exists():
            # Use actual module name from file (handles debug 'd' suffix)
            module_name = _get_module_name_from_path(sparrow_file)
            _load_module_from_path(module_name, sparrow_file)

    # Load the test helper module
    helper_path = os.environ.get('TEST_SPARROW_HELPER_LIB_PATH')
    if helper_path:
        helper_file = Path(helper_path)
        if helper_file.exists():
            # Use actual module name from file (handles debug 'd' suffix)
            module_name = _get_module_name_from_path(helper_file)
            _load_module_from_path(module_name, helper_file)
            return
    
    # Fallback: try to find modules in build directory
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
        "Could not find sparrow_rockfinch or test_sparrow_helper module. "
        "Build the project first or set SPARROW_MODULE_PATH and TEST_SPARROW_HELPER_LIB_PATH."
    )


# Set up modules
_setup_modules()

# Import from the sparrow_rockfinch module (try release first, then debug)
try:
    from sparrow_rockfinch import SparrowArray  # noqa: E402
except ImportError:
    from sparrow_rockfinchd import SparrowArray  # noqa: E402
