"""SURT helpers for range scans over Common Crawl's URL index, which is sorted by SURT key.

A host's SURT reverses its labels: ``www.example.org`` -> ``org,example,www)``. Every key for a
registered domain and its subdomains therefore starts with ``org,example`` followed by ``)``
(the domain itself) or ``,`` (a subdomain). In ASCII ``)`` < ``*`` < ``+`` < ``,`` < ``-``, so the
half-open range ``['org,example)', 'org,example-')`` holds exactly those keys and lets Parquet
row-group statistics skip everything else.
"""

from __future__ import annotations

import re

_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def normalise_domain(domain: str) -> str:
    d = domain.strip().lower().rstrip(".")
    d = d.removeprefix("*.")
    labels = d.split(".")
    if len(labels) < 2 or not all(_LABEL.match(label) for label in labels):
        raise ValueError(f"not a registrable domain name: {domain!r}")
    return d


def surt_prefix(domain: str) -> str:
    return ",".join(reversed(normalise_domain(domain).split(".")))


def surt_range(domain: str) -> tuple[str, str]:
    """Half-open [lo, hi) covering the domain and all of its subdomains."""
    prefix = surt_prefix(domain)
    return prefix + ")", prefix + "-"


def host_surt(host: str) -> str:
    """SURT key prefix for a host, e.g. ``www.example.org`` -> ``org,example,www)``."""
    return ",".join(reversed(host.lower().rstrip(".").split("."))) + ")"
