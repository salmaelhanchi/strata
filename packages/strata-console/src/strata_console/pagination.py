"""Cursor pagination: a page of items and an opaque cursor for the next one.

The cursor encodes the sort key of the last item returned, so a page is stable while rows are
added before it (new jobs, newly pulled pages), which offsets are not.
"""

from __future__ import annotations

import base64
import binascii
import json

from fastapi import HTTPException
from pydantic import BaseModel


class Page[T](BaseModel):
    items: list[T]
    next_cursor: str | None = None  # pass it back as ?cursor= for the next page


def encode(key: str) -> str:
    return base64.urlsafe_b64encode(json.dumps({"after": key}).encode()).decode().rstrip("=")


def decode(cursor: str | None) -> str | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        after = json.loads(base64.urlsafe_b64decode(padded))["after"]
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(400, "malformed cursor") from exc
    if not isinstance(after, str):
        raise HTTPException(400, "malformed cursor")
    return after


def page_of[T](items: list[T], limit: int, key: str | None) -> Page[T]:
    """``items`` fetched with ``limit + 1`` rows: the extra one only says there is more."""
    more = len(items) > limit
    return Page(items=items[:limit], next_cursor=encode(key) if more and key else None)
