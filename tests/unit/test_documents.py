"""Extractors, the publishers' indexes and their agreement."""

from __future__ import annotations

from pathlib import Path

from strata_core.models import Document
from strata_fetch.documents import (
    agreement,
    extract_pep,
    extract_rfc,
    extractor_for,
    html_of,
    labelled,
    load_oracles,
    page_text,
    parse_peps_json,
    parse_rfc_index,
    same_date,
)
from tests.fakecc import build_record
from tests.pages import pep_page, peps_json, rfc_index_entry, rfc_info_page, rfc_page

PEP_URL = "https://peps.python.org/pep-0457/"
RFC_URL = "https://www.rfc-editor.org/info/rfc4767/"


def test_page_text_breaks_blocks_and_skips_scripts() -> None:
    lines, headings = page_text("<h1>A <b>bold</b> title</h1><p>x<br>y</p><script>z</script>")
    assert headings == ["A bold title"]
    assert lines == ["A bold title", "x", "y"]
    assert labelled(["Author:", "Ada"], "Author") == "Ada"
    assert labelled(["Status: Final"], "status") == "Final"
    assert labelled(["Statuses: x"], "Status") is None


def test_pep_extractor_reads_the_header_fields() -> None:
    got = extract_pep(PEP_URL, pep_page(457, "Notation For Things", created="08-Oct-2013"))
    assert got.number == 457
    assert got.title == "Notation For Things"
    assert got.status == "final"  # not the "Status: Draft" in a script or the body
    assert got.doc_type == "standards track"
    assert (got.date, got.date_precision) == ("2013-10-08", "day")
    assert got.authors == ["Ada Example"]  # the obfuscated address is gone


def test_rfc_extractor_reads_the_unlabelled_status() -> None:
    got = extract_rfc(RFC_URL, rfc_page(4767, "The Example Exchange Protocol (EXP)"))
    assert got.number == 4767
    assert got.title == "The Example Exchange Protocol (EXP)"
    assert got.status == "experimental"
    assert (got.date, got.date_precision) == ("2007-03", "month")
    assert got.authors == ["A. Writer", "B. Editor"]


def test_rfc_extractor_reads_the_citation_tags_of_the_redesigned_page() -> None:
    got = extract_rfc(RFC_URL, rfc_info_page(4767, "Numbers Considered Useful"))
    assert got.title == "Numbers Considered Useful"  # not "BCP 99: Numbers ..." from the h1
    assert (got.date, got.date_precision) == ("2004-01-30", "day")  # not a reference's date
    assert got.authors == ["A. Writer", "B. Editor"]
    assert got.status == "best_current_practice"
    assert got.extractor == "rfc-info-html/2"


def test_dates_agree_at_the_precision_both_sides_have() -> None:
    assert same_date("2004-01-30", "2004-01")
    assert same_date("2009-04-01", "2009-04-01")
    assert not same_date("2004-02-01", "2004-01")
    assert not same_date(None, "2004-01")


def test_extractors_are_chosen_by_url_shape() -> None:
    assert extractor_for(PEP_URL) is extract_pep
    assert extractor_for("https://peps.python.org/pep-0008") is extract_pep
    assert extractor_for(RFC_URL) is extract_rfc
    assert extractor_for("https://peps.python.org/topic/packaging/") is None
    assert extractor_for("https://www.rfc-editor.org/rfc/rfc4767.html") is None


def test_html_of_decodes_the_declared_charset() -> None:
    body = "<h1>RFC 1: Café</h1>".encode("latin-1")
    record, _ = build_record(RFC_URL, body, content_type="text/html; charset=ISO-8859-1")
    assert "Café" in html_of(record)


def test_peps_json_is_normalized_like_the_pages() -> None:
    peps = parse_peps_json(
        peps_json({457: {"title": "Notation", "authors": "Ada Example, B. Two", "status": "Final"}})
    )
    e = peps[457]
    assert (e.status, e.doc_type, e.date) == ("final", "standards track", "2013-10-08")
    assert e.authors == ["Ada Example", "B. Two"]


def test_rfc_index_entries_span_lines_and_not_issued_ones_are_skipped() -> None:
    text = "\n\n".join(
        [
            "RFC INDEX\n-------------",
            rfc_index_entry(
                4767,
                "The Example Exchange Protocol (EXP)",
                "A. Writer, B. Editor",
                "March 2007",
                "EXPERIMENTAL",
            ),
            "0008 Not Issued.",
            rfc_index_entry(
                19, "An Early Note", "D. Early", "October 1969", "UNKNOWN", padded=False
            ),
            rfc_index_entry(
                5000,
                "A Title Mentioning June 1999 Inside",
                "C. Author",
                "1 April 2009",
                "INFORMATIONAL",
            ),
        ]
    )
    rfcs = parse_rfc_index(text)
    assert set(rfcs) == {19, 4767, 5000}
    assert rfcs[19].date == "1969-10"  # the current file doesn't zero-pad numbers below 1000
    assert (rfcs[4767].status, rfcs[4767].date) == ("experimental", "2007-03")
    assert rfcs[5000].date == "2009-04-01"  # the last date before "(Format", not the title's
    assert rfcs[4767].text and rfcs[4767].text.startswith("The Example Exchange Protocol (EXP).")


def doc(url: str, html: str) -> Document:
    from datetime import UTC, datetime

    extract = extractor_for(url)
    assert extract is not None
    got = extract(url, html)
    return Document(
        capture_id=url, job_id="j", url=url, extracted_at=datetime.now(UTC), **got.model_dump()
    )


def test_agreement_counts_each_field_and_shows_disagreements() -> None:
    peps = parse_peps_json(
        peps_json(
            {
                457: {"title": "Notation For Things"},
                458: {"title": "Another One", "status": "Draft"},
            }
        )
    )
    rfcs = parse_rfc_index(
        rfc_index_entry(
            4767,
            "The Example Exchange Protocol (EXP)",
            "A. Writer, B. Editor",
            "March 2007",
            "EXPERIMENTAL",
        )
    )
    docs = [
        doc(PEP_URL, pep_page(457, "Notation for things")),  # case differs: still agrees
        doc("https://peps.python.org/pep-0458/", pep_page(458, "Another One")),  # status differs
        doc(RFC_URL, rfc_page(4767, "The Example Exchange Protocol (EXP)")),
        doc("https://www.rfc-editor.org/info/rfc9999/", rfc_page(9999, "Not In The Index")),
    ]

    result = agreement(docs, peps, rfcs)

    assert result.documents == 4 and result.not_in_oracle == 1
    assert (result.fields["pep.title"].agreed, result.fields["pep.title"].compared) == (2, 2)
    assert (result.fields["pep.status"].agreed, result.fields["pep.status"].compared) == (1, 2)
    assert result.fields["rfc.title"].rate == 1.0
    assert "rfc.authors" not in result.fields  # the index lists names only as free text
    assert result.core_rate == 8 / 9  # title, status, date: 3 docs x 3 fields, one miss
    assert len(result.disagreements) == 1 and "pep-0458" in result.disagreements[0]


def test_load_oracles_from_a_folder(tmp_path: Path) -> None:
    assert load_oracles(tmp_path) == ({}, {})
    (tmp_path / "peps.json").write_bytes(peps_json({8: {"title": "Style Guide"}}))
    peps, rfcs = load_oracles(tmp_path)
    assert set(peps) == {8} and rfcs == {}
