"""Normalizers: pure functions from the text a page shows to values that can be compared.

Week 2, S16. Each function takes a string and returns a canonical value or ``None``; none of them
does I/O. They are tested with Hypothesis properties (idempotence, round trips) rather than a
handful of examples, and checked against publishers' own indexes (``strata normalize``).
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date

from pydantic import BaseModel, ConfigDict

MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ],
        start=1,
    )
}

# Statuses as each publisher spells them, lower-cased; values are the canonical form.
PEP_STATUSES = {
    s.lower(): s.lower().replace(" ", "_").replace("!", "")
    for s in (
        "Draft",
        "Active",
        "Accepted",
        "Provisional",
        "Deferred",
        "Rejected",
        "Withdrawn",
        "Final",
        "Superseded",
        "April Fool!",
    )
}
RFC_STATUSES = {
    s: s.replace(" ", "_")
    for s in (
        "internet standard",
        "proposed standard",
        "draft standard",
        "best current practice",
        "informational",
        "experimental",
        "historic",
        "unknown",
    )
}


def normalize_space(text: str) -> str:
    """NFC, every run of whitespace (non-breaking spaces included) as one space, trimmed."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text)).strip()


def title_key(text: str) -> str:
    """A title for comparison: spaces normalized, dashes unified, a final period dropped,
    case folded. ``"Robots  Exclusion Protocol."`` and ``"robots exclusion protocol"`` match."""
    t = normalize_space(text)
    t = re.sub(r"[\u2010-\u2015\u2212]", "-", t)  # dashes and minus signs
    return t.rstrip(". ").casefold()


class NormalizedDate(BaseModel):
    """A date with the precision the source gave: ``2007-03`` is March 2007, not March 1st."""

    model_config = ConfigDict(frozen=True)

    value: str  # ISO prefix: YYYY, YYYY-MM or YYYY-MM-DD
    precision: str  # year, month or day


_ISO = re.compile(r"^([0-9]{4})(?:-([0-9]{2})(?:-([0-9]{2}))?)?$")
_DAY_MON_YEAR = re.compile(r"^([0-9]{1,2})[\s-]+([a-z]+)\.?[\s-]+([0-9]{4})$")
_MON_YEAR = re.compile(r"^([a-z]+)\.?,?\s+([0-9]{4})$")


def _month(name: str) -> int | None:
    """A month from its name or a prefix of at least three letters (``oct``, ``sept``)."""
    if len(name) < 3:
        return None
    return next((i for m, i in MONTHS.items() if m.startswith(name)), None)


def _checked(y: int, m: int | None = None, d: int | None = None) -> NormalizedDate | None:
    try:
        if d is not None and m is not None:
            return NormalizedDate(value=date(y, m, d).isoformat(), precision="day")
        if m is not None:
            date(y, m, 1)
            return NormalizedDate(value=f"{y:04d}-{m:02d}", precision="month")
        return NormalizedDate(value=f"{y:04d}", precision="year")
    except ValueError:
        return None


def parse_date(text: str) -> NormalizedDate | None:
    """Dates as PEPs and RFCs write them: ``08-Oct-2013``, ``1 April 2009``, ``March 2007``,
    ``2013-10-08``, ``2007``. Anything else is ``None``, never a guess."""
    t = normalize_space(text).lower().rstrip(".")
    if m := _ISO.match(t):
        y, mo, d = m.groups()
        return _checked(int(y), int(mo) if mo else None, int(d) if d else None)
    if m := _DAY_MON_YEAR.match(t):
        mon = _month(m.group(2))
        return None if mon is None else _checked(int(m.group(3)), mon, int(m.group(1)))
    if m := _MON_YEAR.match(t):
        mon = _month(m.group(1))
        return None if mon is None else _checked(int(m.group(2)), mon)
    return None


def parse_pep_number(text: str) -> int | None:
    """``PEP 8``, ``pep-0008``, ``PEP-0008`` and URLs ending ``/pep-0008/`` -> 8."""
    m = re.search(r"\bpep[\s-]*0*([0-9]{1,4})\b", normalize_space(text), re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_rfc_number(text: str) -> int | None:
    """``RFC 9309``, ``rfc9309``, ``RFC-9309`` and URLs ending ``/rfc9309/`` -> 9309."""
    m = re.search(r"\brfc[\s-]*0*([0-9]{1,5})\b", normalize_space(text), re.IGNORECASE)
    return int(m.group(1)) if m else None


def normalize_status(text: str, vocabulary: dict[str, str]) -> str | None:
    """A status in its canonical form, or ``None`` if it isn't one the publisher uses."""
    return vocabulary.get(normalize_space(text).lower().rstrip("."))


_ANGLED = re.compile(r"<[^<>]*>")
_OBFUSCATED = re.compile(r"\S+\s+(?:at|\[at\]|\(at\))\s+\S+\.\S+", re.IGNORECASE)
_EMAIL = re.compile(r"\S+@\S+")


def strip_contacts(text: str) -> str:
    """Remove e-mail addresses, plain (``a@b.org``), angled (``<a@b.org>``) or obfuscated
    (``<larry at hastings.org>``), so names can be compared and stored without them."""
    t = _ANGLED.sub(" ", text)
    t = _EMAIL.sub(" ", t)
    t = _OBFUSCATED.sub(" ", t)
    t = re.sub(r"\(\s*\)", " ", t)
    return normalize_space(t).strip(" ,;")


def people(text: str) -> list[str]:
    """Names from an author line: contacts removed, split on commas and "and", trimmed."""
    cleaned = strip_contacts(text).rstrip(".")
    parts = re.split(r",\s*|\s+and\s+|\s*&\s*", cleaned)
    return [p.strip(" .") for p in parts if p.strip(" .")]


def person_key(name: str) -> str:
    """A name for comparison across sources that abbreviate differently: the last word, case
    folded. ``"Larry Hastings"`` and ``"L. Hastings"`` both give ``"hastings"``."""
    words = normalize_space(name).replace(".", " ").split()
    return words[-1].casefold() if words else ""


def normalize_version(text: str) -> str | None:
    """``3.12``, ``Python 3.12``, ``3.12.0`` -> the version string as written (dots kept)."""
    m = re.search(r"\b([0-9]+(?:\.[0-9]+){0,2})\b", normalize_space(text))
    return m.group(1) if m else None
