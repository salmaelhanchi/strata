"""Deterministic crash points for crash-resume tests.

``STRATA_CRASH_AT=after_manifest:12`` (read by ``Settings.crash_at``; a job arms it when it
starts) makes the process die, with no cleanup at all, the 12th time it passes the
``after_manifest`` point: the same as a power cut or ``kill -9`` at exactly that moment. Unset
(always, outside rehearsals), a crash point costs one comparison.

The points, in the order a record passes them:

    after_append    record bytes are on disk; its manifest line is not
    after_manifest  the manifest line is on disk; the part is not sealed
    after_seal      the part is in the store; its rows are not in the lake
    after_insert    the rows are in the lake; the run has not finished
"""

from __future__ import annotations

import os
from collections import Counter

POINTS = ("after_append", "after_manifest", "after_seal", "after_insert")
EXIT_CODE = 137  # what a shell reports for kill -9

_passed: Counter[str] = Counter()
_target: tuple[str, int] | None = None


def arm(spec: str | None) -> None:
    """Set the crash point a run dies at (``name`` or ``name:nth``); ``None`` disarms."""
    global _target
    _passed.clear()
    if not spec:
        _target = None
        return
    name, _, nth = spec.partition(":")
    if name not in POINTS:
        raise ValueError(f"STRATA_CRASH_AT={spec!r}: unknown point; use one of {', '.join(POINTS)}")
    _target = name, int(nth) if nth else 1


def crashpoint(name: str) -> None:
    if _target is None or _target[0] != name:
        return
    _passed[name] += 1
    if _passed[name] >= _target[1]:
        os._exit(EXIT_CODE)
