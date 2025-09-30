#!/usr/bin/env python3
"""
Sequential Workflow Entry Point

This imports the sequential workflow from the tests/integration directory.
"""

import sys
from pathlib import Path

# Import the sequential workflow from tests
sys.path.insert(0, str(Path(__file__).parent / "tests" / "integration"))

from sequential_workflow import AnimeAssistantWorkflow

# Re-export for convenience
__all__ = ["AnimeAssistantWorkflow"]