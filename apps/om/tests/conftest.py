"""Pytest configuration for apps/om tests.

Inserts apps/om/flows on sys.path so flow modules (pipeline_soil,
pipeline_heat, api_retry, ...) can be imported directly by their module
name, the same way they import each other at runtime.
"""

import sys
from pathlib import Path

FLOWS_DIR = Path(__file__).resolve().parents[1] / "flows"
if str(FLOWS_DIR) not in sys.path:
    sys.path.insert(0, str(FLOWS_DIR))
