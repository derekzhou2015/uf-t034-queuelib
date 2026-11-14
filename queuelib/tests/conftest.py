"""Pytest configuration shared by the whole test suite."""

from __future__ import annotations

from hypothesis import settings

# The default 200 ms deadline assumes fast, uncontended storage. On a busy
# Windows machine (antivirus scans, file indexer) the same property-test
# example can take anywhere from a few milliseconds to well over a second,
# depending on when a scan happens to touch the temporary files. That is
# environmental scheduling jitter rather than a code regression, so the
# deadline is disabled here; correctness of the queue operations is still
# fully asserted by every example.
settings.register_profile("queuelib", deadline=None)
settings.load_profile("queuelib")
