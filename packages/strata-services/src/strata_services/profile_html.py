"""A site profile as one self-contained HTML file: no script, no external request, prints well."""

from __future__ import annotations

from collections.abc import Iterable
from html import escape

from strata_services.profile import CrawlProfile, RobotsProfile, SiteProfile

_CSS = """
:root{--bg:#fbfaf7;--fg:#1d1d1b;--muted:#6b6a65;--line:#e4e1d8;--bar:#2f6f5e;--bar2:#c9b37e;
--warn:#a4452c;--card:#ffffff;font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
@media (prefers-color-scheme:dark){:root{--bg:#161614;--fg:#ecebe6;--muted:#9c9a92;
--line:#2d2c28;--bar:#5fae96;--bar2:#a8935f;--warn:#e07a5f;--card:#1e1e1b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg)}
main{max-width:980px;margin:0 auto;padding:40px 20px 64px}
h1{font-size:30px;margin:0 0 4px;letter-spacing:-.01em}h2{font-size:18px;margin:40px 0 12px}
h3{font-size:15px;margin:20px 0 8px}p.lead{color:var(--muted);margin:0 0 24px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
.tile{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.tile b{display:block;font-size:24px;font-variant-numeric:tabular-nums}
.tile span{color:var(--muted);font-size:13px}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted);font-weight:600}
td.n{text-align:right;white-space:nowrap}td.bar{width:34%}
.track{height:10px;border-radius:5px;background:var(--line);overflow:hidden;display:flex}
.fill{background:var(--bar);height:100%}.fill.alt{background:var(--bar2)}
code{font:13px/1.4 ui-monospace,SFMono-Regular,Consolas,monospace;word-break:break-all}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:24px}
.counts code{word-break:normal;overflow-wrap:anywhere}.counts td.bar{width:20%}
.muted{color:var(--muted)}.warn{color:var(--warn)}
.rules{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 16px;
max-height:320px;overflow:auto}
.legend{font-size:13px;color:var(--muted)}.legend i{display:inline-block;width:10px;height:10px;
border-radius:2px;margin:0 4px 0 10px;background:var(--bar)}.legend i.alt{background:var(--bar2)}
footer{margin-top:48px;font-size:13px;color:var(--muted)}
"""

_LANGUAGES = {
    "eng": "English",
    "deu": "German",
    "fra": "French",
    "spa": "Spanish",
    "ita": "Italian",
    "por": "Portuguese",
    "nld": "Dutch",
    "rus": "Russian",
    "jpn": "Japanese",
    "zho": "Chinese",
    "kor": "Korean",
    "ara": "Arabic",
    "pol": "Polish",
    "swe": "Swedish",
    "tur": "Turkish",
    "-": "not detected",
}


def _n(value: int) -> str:
    return f"{value:,}"


def _pct(part: int, whole: int) -> str:
    return "n/a" if not whole else f"{100 * part / whole:.1f}%"


def _bar(part: int, whole: int, *, alt: bool = False) -> str:
    width = 0.0 if not whole else 100 * part / whole
    cls = "fill alt" if alt else "fill"
    return f'<div class="track"><div class="{cls}" style="width:{width:.2f}%"></div></div>'


def _split_bar(allowed: int, total: int) -> str:
    """Allowed share of a shape (green), the rest (sand)."""
    a = 0.0 if not total else 100 * allowed / total
    return (
        f'<div class="track"><div class="fill" style="width:{a:.2f}%"></div>'
        f'<div class="fill alt" style="width:{100 - a:.2f}%"></div></div>'
    )


def _counts_table(title: str, counts: dict[str, int], total: int, label: str) -> str:
    rows = "".join(
        f"<tr><td><code>{escape(k)}</code></td><td class=n>{_n(v)}</td>"
        f"<td class=n>{_pct(v, total)}</td><td class=bar>{_bar(v, total)}</td></tr>"
        for k, v in counts.items()
    )
    return (
        f"<div><h3>{escape(title)}</h3><table class=counts><tr><th>{escape(label)}</th>"
        f"<th>count</th><th>share</th><th></th></tr>{rows}</table></div>"
    )


def _tiles(items: Iterable[tuple[str, str]]) -> str:
    return (
        '<div class="tiles">'
        + "".join(
            f'<div class="tile"><b>{escape(v)}</b><span>{escape(k)}</span></div>' for k, v in items
        )
        + "</div>"
    )


def _crawls(profile: SiteProfile) -> str:
    peak = max(c.urls for c in profile.crawls) or 1
    rows = "".join(
        f"<tr><td><code>{escape(c.crawl)}</code></td><td class=n>{_n(c.urls)}</td>"
        f"<td class=n>{_n(c.captures)}</td><td class=bar>{_bar(c.urls, peak)}</td>"
        f"<td class=muted>{'cache' if c.from_cache else f'{c.requests} req'}</td></tr>"
        for c in profile.crawls
    )
    return (
        "<h2>Pages per crawl</h2><table><tr><th>crawl</th><th>pages</th><th>captures</th>"
        f"<th></th><th>index cost</th></tr>{rows}</table>"
        '<p class="legend">Pages: distinct URLs answered 200 with text/html. Captures: every row'
        " the URL index holds for the site, redirects and errors included.</p>"
    )


def _shapes(c: CrawlProfile) -> str:
    known = all(s.allowed is not None for s in c.shapes)
    rows = "".join(
        f"<tr><td><code>{escape(s.shape)}</code><br><span class=muted>{escape(s.example)}"
        f"</span></td><td class=n>{_n(s.urls)}</td><td class=n>{_pct(s.urls, c.urls)}</td>"
        + (
            f"<td class=n>{_pct(s.allowed or 0, s.urls)}</td>"
            f"<td class=bar>{_split_bar(s.allowed or 0, s.urls)}</td>"
            if known
            else f"<td class=bar>{_bar(s.urls, c.urls)}</td>"
        )
        + "</tr>"
        for s in c.shapes
    )
    head = "<th>allowed</th><th></th>" if known else "<th></th>"
    legend = (
        '<p class="legend"><i></i>allowed by robots.txt<i class="alt"></i>disallowed or'
        " undecided. Digit runs show as N; two path segments at most.</p>"
        if known
        else '<p class="legend">Digit runs show as N; two path segments at most.</p>'
    )
    return (
        f"<h2>URL shapes in {escape(c.crawl)}</h2><table><tr><th>shape and an example</th>"
        f"<th>pages</th><th>share</th>{head}</tr>{rows}</table>{legend}"
    )


def _robots(r: RobotsProfile) -> str:
    verdicts = ", ".join(f"{escape(k)} {_n(v)}" for k, v in r.verdicts.items()) or "no pages"
    lines = [
        f"<h3>{escape(r.host)}</h3>",
        f"<p><b>{escape(r.basis)}</b>"
        + (f" · <code>{escape(r.robots_url)}</code>" if r.robots_url else "")
        + (f" · archived HTTP {r.archived_status}" if r.archived_status is not None else "")
        + f" · verdicts: {verdicts}</p>",
    ]
    if r.error:
        lines.append(f'<p class="warn">{escape(r.error)}</p>')
    if r.crawl_delay_s is not None:
        lines.append(f"<p>Crawl-delay for Strata: {r.crawl_delay_s:g} s</p>")
    if r.content_usage:
        lines.append("<p>" + "<br>".join(f"<code>{escape(u)}</code>" for u in r.content_usage))
    if r.group is not None:
        rules = "".join(
            f"<code>{escape(kind.capitalize())}: {escape(path) or '(empty)'}</code><br>"
            for kind, path in r.group.rules
        )
        agents = ", ".join(escape(a) for a in r.group.agents)
        lines.append(
            f"<p class=muted>Group that applies (User-agent: {agents}), one of {r.groups}:</p>"
            f'<div class="rules">{rules or "<span class=muted>no rules: all allowed</span>"}</div>'
        )
    if r.sitemaps:
        lines.append(
            "<p class=muted>Sitemaps:</p><p>"
            + "<br>".join(f"<code>{escape(s)}</code>" for s in r.sitemaps[:10])
            + "</p>"
        )
    if r.capture_id:
        lines.append(f"<p class=muted>robots.txt capture <code>{escape(r.capture_id)}</code></p>")
    return "".join(lines)


def render(profile: SiteProfile) -> str:
    c = profile.latest
    languages = {_LANGUAGES.get(k, k): v for k, v in c.by_language.items()}
    depth = {f"{k} segment{'s' if k != 1 else ''}": v for k, v in c.depth.items()}
    span = ("captured", "n/a")
    if c.first_capture and c.last_capture:
        days = (c.last_capture.date() - c.first_capture.date()).days + 1
        span = (
            f"captured {c.first_capture:%Y-%m-%d} to {c.last_capture:%m-%d}",
            f"{days} day{'s' if days != 1 else ''}",
        )
    body = [
        f"<h1>{escape(profile.site)}</h1>",
        f'<p class="lead">Site profile from Common Crawl {escape(c.crawl)}, built from the URL'
        " index and the crawl's own robots.txt captures. No page of the site was fetched.</p>",
        _tiles(
            [
                ("pages (200, text/html)", _n(c.urls)),
                ("captures in the index", _n(c.captures)),
                ("hosts", _n(len(c.hosts))),
                span,
                ("requests spent on this site", _n(profile.requests)),
            ]
        ),
        _crawls(profile),
        _shapes(c),
        '<h2>What the crawl holds</h2><div class="grid2">',
        _counts_table("HTTP status (all captures)", c.by_status, c.captures, "status"),
        _counts_table("MIME type (all captures)", c.by_mime, c.captures, "type"),
        _counts_table("Language (pages)", languages, c.urls, "language"),
        _counts_table("Hosts (pages)", c.hosts, c.urls, "host"),
        _counts_table("Path depth (pages)", depth, c.urls, "depth"),
        "</div>",
        "<h2>robots.txt</h2>",
        "".join(_robots(r) for r in profile.robots) or "<p class=muted>No host profiled.</p>",
        f"<footer>Built {profile.built_at:%Y-%m-%d %H:%M} UTC by Strata"
        f" {escape(profile.strata_version)}"
        f" · {_n(c.query_string_urls)} page(s) with a query string are counted above"
        " · Verdicts follow RFC 9309 for the product token “strata”.</footer>",
    ]
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{escape(profile.site)} · Strata profile</title><style>{_CSS}</style></head>"
        f"<body><main>{''.join(body)}</main></body></html>\n"
    )
