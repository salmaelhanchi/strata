from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from strata_fetch.surt import host_surt, normalise_domain, surt_prefix, surt_range

label = st.from_regex(r"[a-z0-9]([a-z0-9-]{0,8}[a-z0-9])?", fullmatch=True)


def in_range(key: str, rng: tuple[str, str]) -> bool:
    return rng[0] <= key < rng[1]


def test_known_examples() -> None:
    assert surt_prefix("www.Example.org.") == "org,example,www"
    assert surt_range("example.org") == ("org,example)", "org,example-")
    assert host_surt("www.example.org") == "org,example,www)"
    assert normalise_domain("*.example.org") == "example.org"


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
