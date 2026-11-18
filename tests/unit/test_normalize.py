"""Normalizers (week 2, S16): properties over generated input, then the publishers' formats."""

from __future__ import annotations

from datetime import date

import pytest
from hypothesis import given
from hypothesis import strategies as st

from strata_core.normalize import (
    MONTHS,
    PEP_STATUSES,
    RFC_STATUSES,
    normalize_space,
    normalize_status,
    normalize_version,
    parse_date,
    parse_pep_number,
    parse_rfc_number,
    people,
    person_key,
    strip_contacts,
    title_key,
)

MONTH_NAMES = list(MONTHS)
dates = st.dates(min_value=date(1900, 1, 1), max_value=date(2099, 12, 31))


@given(st.text())
def test_space_normalization_is_idempotent_and_tidy(text: str) -> None:
    once = normalize_space(text)
    assert normalize_space(once) == once
    assert once == once.strip()
    assert "  " not in once and "\n" not in once and "\u00a0" not in once


@given(st.text())
def test_title_keys_are_idempotent(text: str) -> None:
    assert title_key(title_key(text)) == title_key(text)


def test_title_keys_ignore_spacing_case_dashes_and_a_final_period() -> None:
    assert title_key("Robots  Exclusion\u00a0Protocol.") == title_key("robots exclusion protocol")
    assert title_key("Self\u2013Describing") == title_key("self-describing")


@given(dates)
def test_dates_round_trip_through_every_spelling(d: date) -> None:
    month = MONTH_NAMES[d.month - 1]
    iso = d.isoformat()
    spellings = [
        iso,
        f"{d.day:02d}-{month[:3].title()}-{d.year}",  # PEP: 08-Oct-2013
        f"{d.day} {month.title()} {d.year}",  # RFC index: 1 April 2009
        f"  {d.day}\u00a0{month.upper()}  {d.year}. ",
    ]
    for text in spellings:
        got = parse_date(text)
        assert got is not None, text
        assert (got.value, got.precision) == (iso, "day")
    by_month = parse_date(f"{month.title()} {d.year}")  # RFC info page: March 2007
    assert by_month is not None
    assert (by_month.value, by_month.precision) == (iso[:7], "month")


@given(st.text(max_size=40))
def test_parse_date_never_guesses(text: str) -> None:
    got = parse_date(text)
    if got is not None:
        again = parse_date(got.value)  # whatever it returns is itself a valid, stable date
        assert again == got
        assert got.value[:4].isascii() and got.value[:4].isdigit()


@pytest.mark.parametrize(
    "text", ["31-Feb-2020", "2020-13", "Smarch 2020", "Ma 2020", "08/10/2013", "٢٠٢٠"]
)
def test_impossible_or_ambiguous_dates_are_none(text: str) -> None:
    assert parse_date(text) is None


@given(st.integers(min_value=1, max_value=9999))
def test_pep_numbers_from_urls_and_labels(n: int) -> None:
    assert parse_pep_number(f"https://peps.python.org/pep-{n:04d}/") == n
    assert parse_pep_number(f"PEP {n}") == n
    assert parse_pep_number(f"pep-{n:04d}") == n


@given(st.integers(min_value=1, max_value=99999))
def test_rfc_numbers_from_urls_and_labels(n: int) -> None:
    assert parse_rfc_number(f"https://www.rfc-editor.org/info/rfc{n}/") == n
    assert parse_rfc_number(f"RFC {n:04d}") == n
    assert parse_rfc_number(f"rfc{n}") == n


def test_numbers_are_none_when_absent() -> None:
    assert parse_pep_number("https://peps.python.org/topic/packaging/") is None
    assert parse_rfc_number("https://www.rfc-editor.org/search/") is None


def test_statuses_use_each_publishers_vocabulary() -> None:
    assert normalize_status("Final", PEP_STATUSES) == "final"
    assert normalize_status("April Fool!", PEP_STATUSES) == "april_fool"
    assert normalize_status("PROPOSED STANDARD", RFC_STATUSES) == "proposed_standard"
    assert normalize_status("Best Current Practice.", RFC_STATUSES) == "best_current_practice"
    assert normalize_status("Final", RFC_STATUSES) is None  # not an RFC status
    assert normalize_status("Fnal", PEP_STATUSES) is None


@given(st.sampled_from(sorted(PEP_STATUSES)), st.sampled_from(["", " ", "\u00a0"]))
def test_status_normalization_ignores_case_and_spacing(status: str, pad: str) -> None:
    assert normalize_status(pad + status.upper() + pad, PEP_STATUSES) == PEP_STATUSES[status]


def test_contacts_are_removed_in_every_spelling() -> None:
    line = "Larry Hastings <larry at hastings.org>, Guido van Rossum <guido@python.org>"
    assert "@" not in strip_contacts(line) and " at " not in strip_contacts(line)
    assert people(line) == ["Larry Hastings", "Guido van Rossum"]
    assert people("B. Feinstein, G. Matthews.") == ["B. Feinstein", "G. Matthews"]
    assert people("A. One and B. Two") == ["A. One", "B. Two"]


@given(
    st.from_regex(r"[A-Z][a-z]{1,8}", fullmatch=True),
    st.from_regex(r"[A-Z][a-z]{1,10}", fullmatch=True),
)
def test_full_and_abbreviated_names_share_a_key(first: str, last: str) -> None:
    assert person_key(f"{first} {last}") == person_key(f"{first[0]}. {last}") == last.casefold()


def test_versions() -> None:
    assert normalize_version("Python 3.12") == "3.12"
    assert normalize_version("3.12.0") == "3.12.0"
    assert normalize_version("none") is None
