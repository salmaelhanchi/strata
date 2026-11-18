"""Diffs and change rates from the index's payload digests: no page is fetched."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from strata_core.models import CaptureRef
from strata_fetch.cc.catalog import SiteCatalog, SiteSummary
from strata_services.changes import Change, change_rates, diff_catalogs


def catalog(crawl: str, pages: dict[str, str]) -> SiteCatalog:
    refs = [
        CaptureRef(
            crawl=crawl,
            url=url,
            warc_filename="crawl-data/x.warc.gz",
            warc_record_offset=0,
            warc_record_length=10,
            content_digest=digest,
            mime="text/html",
            status=200,
        )
        for url, digest in pages.items()
    ]
    return SiteCatalog(
        crawl=crawl,
        site="example.org",
        built_at=datetime(2026, 11, 18, tzinfo=UTC),
        summary=SiteSummary(urls=len(refs)),
        refs=refs,
    )


A = "https://example.org/"


def test_a_diff_sorts_every_url_into_one_kind() -> None:
    old = catalog("C1", {A + "a": "D1", A + "b": "D2", A + "c": "D3", A + "news/1": "N1"})
    new = catalog("C2", {A + "a": "D1", A + "b": "XX", A + "d": "D4", A + "news/1": "N2"})

    d = diff_catalogs(old, new, requests=3)

    assert d.counts == {Change.ADDED: 1, Change.REMOVED: 1, Change.CHANGED: 2, Change.SAME: 1}
    assert [(c.url, c.change) for c in d.changes] == [
        (A + "b", Change.CHANGED),
        (A + "c", Change.REMOVED),
        (A + "d", Change.ADDED),
        (A + "news/1", Change.CHANGED),
    ]
    assert d.changes[0].from_digest == "D2"
    assert d.changes[0].to_digest == "XX"
    assert d.shapes[0].shape == "/a"
    assert {s.shape: s.counts[Change.CHANGED] for s in d.shapes}["/news/N"] == 1
    assert (d.from_crawl, d.to_crawl, d.requests, d.page_fetches) == ("C1", "C2", 3, 0)


def test_a_capture_without_a_digest_never_counts_as_unchanged() -> None:
    old = catalog("C1", {A + "a": ""})
    new = catalog("C2", {A + "a": ""})
    old = old.model_copy(update={"refs": [old.refs[0].model_copy(update={"content_digest": None})]})
    new = new.model_copy(update={"refs": [new.refs[0].model_copy(update={"content_digest": None})]})
    assert diff_catalogs(old, new).counts[Change.CHANGED] == 1


def test_change_rates_compare_consecutive_crawls_per_shape() -> None:
    crawls = [
        catalog("C1", {A + "news/1": "a", A + "news/2": "b", A + "pep-1/": "p"}),
        catalog("C2", {A + "news/1": "c", A + "news/2": "b", A + "pep-1/": "p"}),
        catalog("C3", {A + "news/1": "d", A + "pep-1/": "p", A + "pep-2/": "q"}),
    ]

    rates = change_rates(crawls)

    by_shape = {s.shape: s for s in rates.shapes}
    assert (by_shape["/news/N"].compared, by_shape["/news/N"].changed) == (3, 2)
    assert (by_shape["/pep-N/"].compared, by_shape["/pep-N/"].changed) == (2, 0)
    assert by_shape["/pep-N/"].urls == 2
    assert rates.shapes[0].shape == "/news/N"  # most often changing first
    assert by_shape["/news/N"].rate == pytest.approx(2 / 3)
    assert rates.crawls == ["C1", "C2", "C3"]
    assert (rates.compared, rates.changed) == (5, 2)
