from __future__ import annotations

from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from strata_core.ids import capture_id, new_job_id
from strata_core.models import CaptureRef, Outcome, Source

refs = st.builds(
    CaptureRef,
    crawl=st.from_regex(r"CC-MAIN-20[0-9]{2}-[0-9]{2}", fullmatch=True),
    url=st.from_regex(r"https://[a-z]{1,12}\.org/[a-z0-9/]{0,20}", fullmatch=True),
    url_surtkey=st.none() | st.text(min_size=1, max_size=40),
    fetch_time=st.none() | st.datetimes(timezones=st.just(UTC)),
    warc_filename=st.from_regex(r"crawl-data/[A-Za-z0-9/._-]{1,60}\.warc\.gz", fullmatch=True),
    warc_record_offset=st.integers(min_value=0, max_value=2**40),
    warc_record_length=st.integers(min_value=1, max_value=2**24),
    content_digest=st.none() | st.from_regex(r"[A-Z2-7]{32}", fullmatch=True),
    mime=st.none() | st.just("text/html"),
    status=st.none() | st.integers(min_value=100, max_value=599),
)


@given(refs)
def test_capture_ref_json_round_trip(ref: CaptureRef) -> None:
    assert CaptureRef.model_validate_json(ref.model_dump_json()) == ref


@given(refs)
def test_range_header_covers_exactly_the_record(ref: CaptureRef) -> None:
    first, last = ref.range_header().removeprefix("bytes=").split("-")
    assert int(first) == ref.warc_record_offset
    assert int(last) - int(first) + 1 == ref.warc_record_length


def test_capture_ref_rejects_empty_record() -> None:
    with pytest.raises(ValidationError):
        CaptureRef(
            crawl="c", url="u", warc_filename="f", warc_record_offset=0, warc_record_length=0
        )


def test_capture_id_is_deterministic_and_location_sensitive() -> None:
    a = capture_id(Source.COMMON_CRAWL, "CC-MAIN-2026-17", "crawl-data/x.warc.gz", 100)
    assert a == capture_id(Source.COMMON_CRAWL, "CC-MAIN-2026-17", "crawl-data/x.warc.gz", 100)
    assert a != capture_id(Source.COMMON_CRAWL, "CC-MAIN-2026-17", "crawl-data/x.warc.gz", 101)
    assert a != capture_id(Source.COMMON_CRAWL, "CC-MAIN-2026-21", "crawl-data/x.warc.gz", 100)
    assert a != capture_id(Source.WAYBACK, "CC-MAIN-2026-17", "crawl-data/x.warc.gz", 100)
    assert len(a) == 64


def test_capture_id_rejects_negative_offset() -> None:
    with pytest.raises(ValueError, match="offset"):
        capture_id(Source.COMMON_CRAWL, None, "f", -1)


def test_job_id_is_time_sortable() -> None:
    early = new_job_id(datetime(2026, 11, 7, 9, 0, tzinfo=UTC))
    late = new_job_id(datetime(2026, 11, 7, 10, 0, tzinfo=UTC))
    assert early.startswith("job_20261107T090000Z_")
    assert early < late


def test_outcome_names_match_the_protobuf_enum() -> None:
    # Week 3 maps these mechanically to OUTCOME_<NAME>; renaming one is a breaking change.
    assert [o.name for o in Outcome] == [
        "OK",
        "DEFINITIVE_EMPTY",
        "RETRYABLE_ERROR",
        "RATE_LIMITED",
        "BLOCKED",
    ]
