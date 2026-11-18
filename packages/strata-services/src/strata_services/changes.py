"""What changed on a site between crawls, from the URL index's payload digests alone.

Common Crawl records a SHA-1 of every capture's payload in its index, so two site catalogs are
enough to tell which URLs appeared, disappeared or changed: no page is fetched. Two caveats are
part of the answer, not footnotes: a crawl does not revisit every page, so "removed" means "not
captured in the later crawl"; and a page that embeds a timestamp or a token changes its digest on
every capture, so a high change rate can be noise. ``changes`` reports per URL shape so the noise
shows up as a shape, not as a site-wide number.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from enum import StrEnum
from itertools import pairwise

from pydantic import BaseModel, ConfigDict

from strata_core.models import CaptureRef
from strata_fetch.cc.attempts import IndexAttempt
from strata_fetch.cc.catalog import SiteCatalog, url_shape
from strata_fetch.surt import normalise_domain
from strata_services.profile import CatalogSource


class Change(StrEnum):
    ADDED = "added"  # captured in the later crawl only
    REMOVED = "removed"  # captured in the earlier crawl only
    CHANGED = "changed"  # captured in both, payload digest differs
    SAME = "same"  # captured in both, same payload digest


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class UrlChange(_Frozen):
    url: str
    change: Change
    from_digest: str | None = None
    to_digest: str | None = None


class ShapeDiff(_Frozen):
    shape: str
    counts: dict[Change, int]


class SiteDiff(_Frozen):
    site: str
    from_crawl: str
    to_crawl: str
    counts: dict[Change, int]
    shapes: list[ShapeDiff]  # largest first
    changes: list[UrlChange]  # every URL except the unchanged ones, in URL order
    requests: int  # index requests this diff spent; 0 when both catalogs were cached
    footer_reads: int = 0  # one-time shard-map reads for a crawl, shared and never charged
    page_fetches: int = 0  # always 0: digests come from the index


class ShapeRate(_Frozen):
    shape: str
    urls: int  # distinct URLs of this shape seen in any of the crawls
    compared: int  # (URL, consecutive crawl pair) cases captured in both crawls
    changed: int  # of those, how many had a different payload digest
    example: str

    @property
    def rate(self) -> float:
        return self.changed / self.compared if self.compared else 0.0


class ChangeRates(_Frozen):
    site: str
    crawls: list[str]  # oldest first
    shapes: list[ShapeRate]  # most often changing first
    compared: int
    changed: int
    requests: int
    footer_reads: int = 0


def diff_catalogs(
    old: SiteCatalog, new: SiteCatalog, *, requests: int = 0, footer_reads: int = 0
) -> SiteDiff:
    before = {r.url: r for r in old.refs}
    after = {r.url: r for r in new.refs}
    changes: list[UrlChange] = []
    counts: Counter[Change] = Counter()
    by_shape: dict[str, Counter[Change]] = {}
    for url in sorted(before.keys() | after.keys()):
        kind, a, b = _compare(before.get(url), after.get(url))
        counts[kind] += 1
        by_shape.setdefault(url_shape(url), Counter())[kind] += 1
        if kind is not Change.SAME:
            changes.append(UrlChange(url=url, change=kind, from_digest=a, to_digest=b))
    shapes = [
        ShapeDiff(shape=s, counts=_ordered(c))
        for s, c in sorted(by_shape.items(), key=lambda kv: (-kv[1].total(), kv[0]))
    ]
    return SiteDiff(
        site=new.site,
        from_crawl=old.crawl,
        to_crawl=new.crawl,
        counts=_ordered(counts),
        shapes=shapes,
        changes=changes,
        requests=requests,
        footer_reads=footer_reads,
    )


def _compare(a: CaptureRef | None, b: CaptureRef | None) -> tuple[Change, str | None, str | None]:
    if a is None and b is not None:
        return Change.ADDED, None, b.content_digest
    if b is None and a is not None:
        return Change.REMOVED, a.content_digest, None
    assert a is not None
    assert b is not None
    same = a.content_digest is not None and a.content_digest == b.content_digest
    return (Change.SAME if same else Change.CHANGED), a.content_digest, b.content_digest


def _ordered(counts: Counter[Change]) -> dict[Change, int]:
    return {kind: counts.get(kind, 0) for kind in Change}


def change_rates(
    catalogs: Sequence[SiteCatalog], *, requests: int = 0, footer_reads: int = 0
) -> ChangeRates:
    """Per URL shape, how often a URL captured in two consecutive crawls changed in between.
    ``catalogs`` are oldest first."""
    stats: dict[str, list[int]] = {}  # shape -> [compared, changed]
    urls: dict[str, set[str]] = {}
    for old, new in pairwise(catalogs):
        before = {r.url: r.content_digest for r in old.refs}
        for ref in new.refs:
            shape = url_shape(ref.url)
            urls.setdefault(shape, set()).add(ref.url)
            if ref.url not in before:
                continue
            row = stats.setdefault(shape, [0, 0])
            row[0] += 1
            row[1] += before[ref.url] != ref.content_digest
    for cat in catalogs[:1]:
        for ref in cat.refs:
            urls.setdefault(url_shape(ref.url), set()).add(ref.url)
    shapes = [
        ShapeRate(
            shape=shape,
            urls=len(members),
            compared=stats.get(shape, [0, 0])[0],
            changed=stats.get(shape, [0, 0])[1],
            example=min(members),
        )
        for shape, members in urls.items()
    ]
    shapes.sort(key=lambda s: (-s.rate, -s.compared, s.shape))
    return ChangeRates(
        site=catalogs[-1].site if catalogs else "",
        crawls=[c.crawl for c in catalogs],
        shapes=shapes,
        compared=sum(s.compared for s in shapes),
        changed=sum(s.changed for s in shapes),
        requests=requests,
        footer_reads=footer_reads,
    )


class ChangeService:
    def __init__(self, catalogs: CatalogSource) -> None:
        self._catalogs = catalogs

    async def _load(
        self, site: str, crawls: Sequence[str]
    ) -> tuple[list[SiteCatalog], dict[str, int]]:
        """The catalogs, and what loading them cost: requests charged, footer reads not."""
        loaded: list[SiteCatalog] = []
        sent: list[IndexAttempt] = []
        for crawl in crawls:
            catalog, attempts, _, _ = await self._catalogs.catalog(site, crawl)
            loaded.append(catalog)
            sent += attempts
        charged = sum(1 for a in sent if a.charged)
        return loaded, {"requests": charged, "footer_reads": len(sent) - charged}

    async def diff(self, site: str, from_crawl: str, to_crawl: str) -> SiteDiff:
        if from_crawl == to_crawl:
            raise ValueError("a diff needs two different crawls")
        (old, new), sent = await self._load(normalise_domain(site), [from_crawl, to_crawl])
        return diff_catalogs(old, new, **sent)

    async def rates(self, site: str, crawls: Sequence[str]) -> ChangeRates:
        """``crawls`` oldest first; at least two."""
        if len(crawls) < 2:
            raise ValueError("change rates need at least two crawls")
        loaded, sent = await self._load(normalise_domain(site), crawls)
        return change_rates(loaded, **sent)
