"""
Python helper module for sparrow-pycapsule integration tests.

This module provides helper functions that wrap the C++ SparrowArray class,
making it easy to create test arrays and perform roundtrip operations.
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Any, Protocol, Tuple

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


def _setup_module_path() -> None:
    """Add the build directory to Python path so we can import test_sparrow_helper."""
    # Check for environment variable first
    helper_path = os.environ.get('TEST_SPARROW_HELPER_PATH')
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
        "Could not find test_sparrow_helper module. "
        "Build the project first or set TEST_SPARROW_HELPER_PATH."
    )


# Set up module path and import the C++ module
_setup_module_path()

# Import the native Python extension module that provides SparrowArray
from test_sparrow_helper import SparrowArray  # noqa: E402
