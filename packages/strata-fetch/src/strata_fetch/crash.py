"""Deterministic crash points for crash-resume tests (week 2, S12).

``STRATA_CRASH_AT=after_manifest:12`` makes the process die, with no cleanup at all, the 12th
time it passes the ``after_manifest`` point: the same as a power cut or ``kill -9`` at exactly
that moment. Unset (always, outside tests), a crash point costs one dictionary lookup.

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


def _target() -> tuple[str, int] | None:
    spec = os.environ.get("STRATA_CRASH_AT")
    if not spec:
        return None
    name, _, nth = spec.partition(":")
    if name not in POINTS:
        raise ValueError(f"STRATA_CRASH_AT={spec!r}: unknown point; use one of {', '.join(POINTS)}")
    return name, int(nth) if nth else 1


def crashpoint(name: str) -> None:
    target = _target()
    if target is None or target[0] != name:
        return
    _passed[name] += 1
    if _passed[name] >= target[1]:
        os._exit(EXIT_CODE)
