"""SURT helpers for range scans over Common Crawl's URL index, which is sorted by SURT key.

Common Crawl builds ``url_surtkey`` with the Internet Archive's ``surt`` library. A host's key
reverses its labels, after the library drops a leading ``www.`` (or ``www2.`` and so on):

    https://peps.python.org/pep-0008/      -> org,python,peps)/pep-0008
    https://www.rfc-editor.org/info/rfc9309 -> org,rfc-editor)/info/rfc9309
    https://a.www.example.org/x            -> org,example,www,a)/x   (only a *leading* www goes)

So ``www.example.org`` and ``example.org`` share the key ``org,example)``. Every key for a domain
and its subdomains starts with ``org,example`` followed by ``)`` (the domain itself) or ``,`` (a
subdomain). In ASCII ``)`` < ``*`` < ``+`` < ``,`` < ``-``, so the half-open range
``['org,example)', 'org,example-')`` holds exactly those keys and lets Parquet row-group
statistics skip everything else. A range can't separate ``www.example.org`` from ``example.org``;
callers that want one host keep its rows with ``on_site`` (or ``url_host_name`` in SQL).

Week 1 kept the ``www`` and looked up ``www.rfc-editor.org`` in ``org,rfc-editor,www)``, where no
key can be. The tests agreed, because the fake built its keys with the same function;
``tests/unit/test_surt.py`` now pins these helpers to outputs of the real library.
"""

from __future__ import annotations

import contextlib
import re
from urllib.parse import urlsplit

_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
# The surt library's rule (IAURLCanonicalizer._RE_WWWDIGITS, matched at the start of the host).
_LEADING_WWW = re.compile(r"^www[0-9]*\.")  # ASCII digits, as in the library's bytes regex


def normalise_domain(domain: str) -> str:
    """Lower-case ASCII form of a host or domain; international names become punycode (IDNA).

    The surt library keys international hosts by their punycode form (``bücher.example`` ->
    ``example,xn--bcher-kva)``), so the ASCII form is the one key ranges are built from.
    """
    d = domain.strip().lower().rstrip(".")
    d = d.removeprefix("*.")
    try:
        d = d.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError(f"not a registrable domain name: {domain!r}") from exc
    labels = d.split(".")
    if len(labels) < 2 or not all(_LABEL.match(label) for label in labels):
        raise ValueError(f"not a registrable domain name: {domain!r}")
    return d


def host_forms(site: str) -> tuple[str, ...]:
    """The forms a host can take in ``url_host_name``: punycode, and Unicode when different.

    Common Crawl keeps raw non-ASCII characters in URLs, so the column may hold either form;
    a filter that accepts both can't miss the host.
    """
    ascii_form = normalise_domain(site)
    try:
        unicode_form = ascii_form.encode("ascii").decode("idna")
    except UnicodeError:
        return (ascii_form,)
    return (ascii_form,) if unicode_form == ascii_form else (ascii_form, unicode_form)


def _massage(host: str) -> str:
    """Drop a leading ``www.``/``wwwN.`` as the surt library does, keeping at least two labels.

    The library would also turn ``www.org`` into ``org``; a range over that would span every
    .org site, so such a host keeps its ``www`` here and its keys are simply not found.
    """
    stripped = _LEADING_WWW.sub("", host, count=1)
    return stripped if "." in stripped else host


def surt_prefix(domain: str) -> str:
    return ",".join(reversed(_massage(normalise_domain(domain)).split(".")))


def surt_range(domain: str) -> tuple[str, str]:
    """Half-open [lo, hi) covering the domain (or host) and all of its subdomains.

    For a ``www`` host the range is its parent domain's: Common Crawl keys both the same way.
    """
    prefix = surt_prefix(domain)
    return prefix + ")", prefix + "-"


def host_surt(host: str) -> str:
    """SURT key prefix for a host, as Common Crawl writes it.

    ``peps.python.org`` -> ``org,python,peps)``; ``www.python.org`` -> ``org,python)``.
    """
    return ",".join(reversed(_massage(host.lower().rstrip(".")).split("."))) + ")"


def on_site(url: str, site: str) -> bool:
    """Whether ``url``'s host is ``site`` or one of its subdomains (``site`` already normalised).

    Either side may be an international name in Unicode or punycode.
    """
    host = (urlsplit(url).hostname or "").rstrip(".")
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return False
    return host == site or host.endswith("." + site)


_DEFAULT_PORTS = {"http": 80, "https": 443}


def url_key(url: str) -> str:
    """A URL's SURT key as Common Crawl writes it, for exact-key lookups such as robots.txt:
    the host key (``www`` dropped, punycode), a non-default port, then the lower-cased path
    with repeated and trailing slashes removed, and the query with its parameters sorted.

    ``https://www.Example.org:443/ROBOTS.TXT`` -> ``org,example)/robots.txt`` and
    ``https://example.org/a//b/`` -> ``org,example)/a/b``: both checked against the surt library
    (``tests/unit/test_surt.py`` pins its outputs). Not replicated: the library's removal of
    session ids (``PHPSESSID=...``) and its percent-unescaping (``%7E`` -> ``~``), which a
    robots.txt URL never needs.
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").rstrip(".")
    with contextlib.suppress(UnicodeError):
        host = host.encode("idna").decode("ascii")
    key = host_surt(host)
    if parts.port is not None and parts.port != _DEFAULT_PORTS.get(parts.scheme):
        key = key[:-1] + f":{parts.port})"
    path = re.sub(r"/{2,}", "/", parts.path or "/").lower()
    key += path.rstrip("/") or "/"
    if parts.query:
        key += "?" + "&".join(sorted(parts.query.lower().split("&")))
    return key
