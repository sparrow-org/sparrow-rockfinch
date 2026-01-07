"""
Sparrow Rockfinch - High-performance Arrow array library for Python.

This module provides the SparrowArray class which implements the Arrow PyCapsule
Interface for zero-copy data exchange with other Arrow-compatible libraries
like Polars and PyArrow.

Example
-------
>>> import pyarrow as pa
>>> from sparrow_rockfinch import SparrowArray
>>>
>>> # Create from PyArrow
>>> pa_array = pa.array([1, 2, None, 4, 5])
>>> sparrow_array = SparrowArray.from_arrow(pa_array)
>>>
>>> # Use with Polars (zero-copy)
>>> import polars as pl
>>> series = pl.from_arrow(sparrow_array)
"""

from __future__ import annotations

from typing import Any, Protocol, Tuple, TYPE_CHECKING

# Import the compiled extension module
# The actual module name matches the compiled .so file
try:
    from sparrow_rockfinch.sparrow_rockfinch import SparrowArray
except ImportError:
    # Fallback for development builds where module might be at top level
    try:
        from .sparrow_rockfinch import SparrowArray
    except ImportError:
        # Last resort: try importing from parent (editable installs)
        from sparrow_rockfinch import SparrowArray as _SparrowArray
        SparrowArray = _SparrowArray


class ArrowArrayExportable(Protocol):
    """Protocol for objects implementing the Arrow PyCapsule Interface.
    
    Any object implementing this protocol can be used to create a SparrowArray.
    This includes PyArrow arrays, Polars Series, and other Arrow-compatible types.
    """
    
    def __arrow_c_array__(
        self, requested_schema: Any = None
    ) -> Tuple[Any, Any]:
        """Export the array as Arrow PyCapsules.
        
        Parameters
        ----------
        requested_schema : object, optional
            The requested schema for the export (typically None).
        
        Returns
        -------
        Tuple[object, object]
            A tuple of (schema_capsule, array_capsule).
        """
        ...


__all__ = [
    "SparrowArray",
    "ArrowArrayExportable",
]

__version__ = "0.1.0"
