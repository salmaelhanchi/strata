from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from strata_fetch.surt import (
    host_forms,
    host_surt,
    normalise_domain,
    on_site,
    surt_prefix,
    surt_range,
    url_key,
)

label = st.from_regex(r"[a-z0-9]([a-z0-9-]{0,8}[a-z0-9])?", fullmatch=True)


def in_range(key: str, rng: tuple[str, str]) -> bool:
    return rng[0] <= key < rng[1]


def test_known_examples() -> None:
    assert surt_prefix("www.Example.org.") == "org,example"
    assert surt_range("example.org") == ("org,example)", "org,example-")
    assert host_surt("www.example.org") == "org,example)"
    assert normalise_domain("*.example.org") == "example.org"


# Common Crawl's url_surtkey comes from the Internet Archive's `surt` library. These pairs are
# its real output (surt 0.3.1, run 2026-11-08), so our keys and the fake's can't drift from it.
SURT_LIBRARY = [
    ("https://www.rfc-editor.org/info/rfc9309", "org,rfc-editor)/info/rfc9309"),
    ("https://rfc-editor.org/info/rfc9309", "org,rfc-editor)/info/rfc9309"),
    ("https://www2.example.org/a", "org,example)/a"),
    ("https://peps.python.org/pep-0008/", "org,python,peps)/pep-0008"),
    ("https://www.python.org/about/", "org,python)/about"),
    ("https://a.www.example.org/x", "org,example,www,a)/x"),
]


@pytest.mark.parametrize(("url", "key"), SURT_LIBRARY)
def test_host_keys_match_the_surt_library(url: str, key: str) -> None:
    host, _, _ = url.removeprefix("https://").partition("/")
    assert key.startswith(host_surt(host))
    site = host
    assert surt_range(site)[0] <= key < surt_range(site)[1]  # a lookup for the host finds it


def test_a_www_host_shares_its_parent_domains_range() -> None:
    assert surt_range("www.rfc-editor.org") == surt_range("rfc-editor.org")
    assert surt_range("www2.example.org") == surt_range("example.org")
    assert surt_range("www.org") == ("org,www)", "org,www-")  # never the whole .org TLD


def test_on_site_keeps_the_host_and_its_subdomains_only() -> None:
    assert on_site("https://www.rfc-editor.org/info/rfc1", "www.rfc-editor.org")
    assert on_site("https://a.www.rfc-editor.org/", "www.rfc-editor.org")
    assert not on_site("https://rfc-editor.org/info/rfc1", "www.rfc-editor.org")
    assert not on_site("https://datatracker.rfc-editor.org/", "www.rfc-editor.org")
    assert not on_site("https://evilwww.rfc-editor.org/", "www.rfc-editor.org")
    assert on_site("https://www.python.org/about/", "python.org")


@pytest.mark.parametrize("bad", ["localhost", "", "exa mple.org", "-bad.org", "a..org"])
def test_rejects_non_domains(bad: str) -> None:
    with pytest.raises(ValueError):
        normalise_domain(bad)


@given(sld=label, tld=label, subs=st.lists(label, max_size=3), path=st.text(max_size=20))
def test_domain_and_every_subdomain_fall_inside_the_range(
    sld: str, tld: str, subs: list[str], path: str
) -> None:
    domain = f"{sld}.{tld}"
    host = ".".join([*subs, domain])
    assert in_range(host_surt(host) + "/" + path, surt_range(domain))


@given(sld=label, tld=label, suffix=st.from_regex(r"[a-z0-9-]{1,6}", fullmatch=True))
def test_lookalike_domains_fall_outside_the_range(sld: str, tld: str, suffix: str) -> None:
    domain = f"{sld}.{tld}"
    lookalike = f"{sld}{suffix}.{tld}"  # e.g. example-shop.org, examples.org
    assert not in_range(host_surt(lookalike) + "/", surt_range(domain))


www_label = st.from_regex(r"www[0-9]{0,2}", fullmatch=True)


@given(sld=label, tld=label, www=www_label, path=st.text(max_size=20))
def test_a_www_hosts_keys_fall_inside_its_own_range(
    sld: str, tld: str, www: str, path: str
) -> None:
    host = f"{www}.{sld}.{tld}"
    assert in_range(host_surt(host) + "/" + path, surt_range(host))


# url_key against the surt library's own output (surt 0.3.1, run 2026-11-14).
URL_KEYS = [
    ("https://www.Example.org:443/ROBOTS.TXT", "org,example)/robots.txt"),
    ("https://bücher.example/Path?b=2&a=1", "example,xn--bcher-kva)/path?a=1&b=2"),
    ("https://xn--bcher-kva.example/x", "example,xn--bcher-kva)/x"),
    ("https://münchen.de/robots.txt", "de,xn--mnchen-3ya)/robots.txt"),
    ("https://example.org:8080/a", "org,example:8080)/a"),
    ("http://example.org:80/a", "org,example)/a"),
    ("https://www.example.org", "org,example)/"),
    ("https://example.org//", "org,example)/"),
    ("http://WWW3.Example.org/A/B/?Z=1&y=2", "org,example)/a/b?y=2&z=1"),
    ("https://peps.python.org/pep-0008/", "org,python,peps)/pep-0008"),
    ("https://www.rfc-editor.org/info/rfc9309/", "org,rfc-editor)/info/rfc9309"),
    ("https://example.org/a//b/", "org,example)/a/b"),
    ("https://example.org/a/?x=1", "org,example)/a?x=1"),
    ("https://example.org/?x=1", "org,example)/?x=1"),
    ("https://example.org/a?", "org,example)/a"),
    ("https://example.org/a#frag", "org,example)/a"),
    ("https://example.org/a;b=1", "org,example)/a;b=1"),
    ("https://example.org/a?b=&a", "org,example)/a?a&b="),
    ("https://Example.ORG./x", "org,example)/x"),
    ("https://user:pw@example.org/x", "org,example)/x"),
]


@pytest.mark.parametrize(("url", "key"), URL_KEYS)
def test_url_key_matches_the_surt_library(url: str, key: str) -> None:
    assert url_key(url) == key


def test_international_names_are_keyed_and_matched_in_punycode() -> None:
    assert normalise_domain("Bücher.Example") == "xn--bcher-kva.example"
    assert host_forms("bücher.example") == ("xn--bcher-kva.example", "bücher.example")
    assert host_forms("xn--bcher-kva.example") == ("xn--bcher-kva.example", "bücher.example")
    assert host_forms("peps.python.org") == ("peps.python.org",)
    assert surt_range("bücher.example") == surt_range("xn--bcher-kva.example")
    site = normalise_domain("bücher.example")
    # Common Crawl keeps raw Unicode in URLs; either spelling is the same site.
    assert on_site("https://bücher.example/a", site)
    assert on_site("https://xn--bcher-kva.example/a", site)
    assert on_site("https://shop.bücher.example/a", site)
    assert not on_site("https://bücherei.example/a", site)


@given(
    sld=label,
    tld=label,
    www=st.sampled_from(["", "www.", "www2."]),
    path=st.lists(st.from_regex(r"[a-z0-9._~-]{1,6}", fullmatch=True), max_size=3),
    slash=st.booleans(),
)
def test_url_key_starts_with_the_host_key_and_falls_inside_the_range(
    sld: str, tld: str, www: str, path: list[str], slash: bool
) -> None:
    url = f"https://{www}{sld}.{tld}/" + "/".join(path) + ("/" if slash and path else "")
    key = url_key(url)
    assert key.startswith(host_surt(f"{www}{sld}.{tld}"))
    assert in_range(key, surt_range(f"{sld}.{tld}"))
    assert not key.endswith("/") or key.endswith(")/")  # only the root keeps its slash
