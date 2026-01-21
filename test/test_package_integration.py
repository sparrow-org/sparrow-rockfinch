#!/usr/bin/env python3
"""
Integration test for sparrow-rockfinch with Polars and PyArrow.

This file imports shared test classes from test_common to avoid duplication.
"""

import sys
import pytest

# Import shared test classes from test_common
from test_common import (
    TestPyArrowToSparrow,
    TestSparrowStreamWithPolars,
)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
