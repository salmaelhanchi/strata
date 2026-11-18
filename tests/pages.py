"""Pages and index entries shaped like the two Gate 1 sites, for extractor and agreement tests.

The markup follows what the live pages showed on 2026-11-14 (peps.python.org/pep-NNNN/ and
www.rfc-editor.org/info/rfcN/): the labels, their order and the way values are wrapped. The
documents themselves are invented, so no published text is copied here.
"""

from __future__ import annotations

import json


def pep_page(
    number: int,
    title: str,
    *,
    authors: str = "Ada Example &lt;ada&#32;at&#32;example.org&gt;",
    status: str = "Final",
    kind: str = "Standards Track",
    created: str = "08-Oct-2013",
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>PEP {number} &#8211; {title} | peps.python.org</title>
<script>var x = "Status: Draft";</script></head>
<body><nav>Python Enhancement Proposals</nav>
<article><section id="pep-page-section">
<header><ul class="breadcrumbs">
<li><a href="/">PEP Index</a> &raquo; </li><li>PEP {number}</li>
</ul></header>
<h1 class="page-title">PEP {number} &#8211; {title}</h1>
<dl class="rfc2822 field-list simple">
<dt class="field-odd">Author<span class="colon">:</span></dt>
<dd class="field-odd">{authors}</dd>
<dt class="field-even">Discussions-To<span class="colon">:</span></dt>
<dd class="field-even">
<a class="reference external" href="https://discuss.example.org/">Discourse thread</a></dd>
<dt class="field-odd">Status<span class="colon">:</span></dt>
<dd class="field-odd"><abbr title="Accepted and implementation complete">{status}</abbr></dd>
<dt class="field-even">Type<span class="colon">:</span></dt>
<dd class="field-even"><abbr title="Normative PEP">{kind}</abbr></dd>
<dt class="field-odd">Created<span class="colon">:</span></dt>
<dd class="field-odd">{created}</dd>
<dt class="field-even">Post-History<span class="colon">:</span></dt>
<dd class="field-even">09-Oct-2013</dd>
</dl>
<section id="abstract"><h2>Abstract</h2><p>Status: this paragraph is not the header.</p></section>
</section></article></body></html>"""


def rfc_page(
    number: int,
    title: str,
    *,
    status: str = "Experimental",
    published: str = "March 2007",
    authors: str = "A. Writer, B. Editor",
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>RFC {number}: {title}</title></head>
<body><header><a href="/">RFC Editor</a><nav><a href="/search/">Search</a></nav></header>
<main><div class="infoblock">
<h1>RFC {number}: {title}</h1>
<p>{status}</p>
<ul>
<li>Document Type: RFC - {status}</li>
<li>Date published: {published}</li>
<li>Authors: {authors}.</li>
<li>Stream: IETF</li>
<li>Source: example (sec)</li>
</ul>
<h2>About this RFC</h2>
<p>This RFC is Informational? No: this is the about section, not the header.</p>
</div></main></body></html>"""


def peps_json(entries: dict[int, dict[str, str]]) -> bytes:
    return json.dumps(
        {
            str(n): {
                "number": n,
                "title": e["title"],
                "authors": e.get("authors", "Ada Example"),
                "status": e.get("status", "Final"),
                "type": e.get("type", "Standards Track"),
                "created": e.get("created", "08-Oct-2013"),
                "url": f"https://peps.python.org/pep-{n:04d}/",
            }
            for n, e in entries.items()
        }
    ).encode()


def rfc_index_entry(n: int, title: str, authors: str, when: str, status: str) -> str:
    """An entry wrapped at 72 columns, continuation lines indented five spaces, as the file
    does."""
    words = (
        f"{n:04d} {title}. {authors}. {when}. (Format: TXT, HTML) (Status: {status})"
        f" (DOI: 10.17487/RFC{n:04d})"
    ).split()
    lines, line = [], ""
    for w in words:
        if line and len(line) + 1 + len(w) > 72:
            lines.append(line)
            line = "     " + w
        else:
            line = f"{line} {w}" if line else w
    lines.append(line)
    return "\n".join(lines)
