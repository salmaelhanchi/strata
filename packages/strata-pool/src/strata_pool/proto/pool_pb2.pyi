from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Outcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTCOME_UNSPECIFIED: _ClassVar[Outcome]
    OUTCOME_OK: _ClassVar[Outcome]
    OUTCOME_DEFINITIVE_EMPTY: _ClassVar[Outcome]
    OUTCOME_RETRYABLE_ERROR: _ClassVar[Outcome]
    OUTCOME_RATE_LIMITED: _ClassVar[Outcome]
    OUTCOME_BLOCKED: _ClassVar[Outcome]
OUTCOME_UNSPECIFIED: Outcome
OUTCOME_OK: Outcome
OUTCOME_DEFINITIVE_EMPTY: Outcome
OUTCOME_RETRYABLE_ERROR: Outcome
OUTCOME_RATE_LIMITED: Outcome
OUTCOME_BLOCKED: Outcome

class WorkerMessage(_message.Message):
    __slots__ = ("hello", "result")
    HELLO_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    hello: Hello
    result: Result
    def __init__(self, hello: _Optional[_Union[Hello, _Mapping]] = ..., result: _Optional[_Union[Result, _Mapping]] = ...) -> None: ...

class SchedulerMessage(_message.Message):
    __slots__ = ("lease", "ack")
    LEASE_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    lease: Lease
    ack: Ack
    def __init__(self, lease: _Optional[_Union[Lease, _Mapping]] = ..., ack: _Optional[_Union[Ack, _Mapping]] = ...) -> None: ...

class Hello(_message.Message):
    __slots__ = ("worker_id", "slots")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    slots: int
    def __init__(self, worker_id: _Optional[str] = ..., slots: _Optional[int] = ...) -> None: ...

class PageRef(_message.Message):
    __slots__ = ("crawl", "url", "url_surtkey", "fetch_time", "warc_filename", "warc_record_offset", "warc_record_length", "content_digest", "mime", "status")
    CRAWL_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    URL_SURTKEY_FIELD_NUMBER: _ClassVar[int]
    FETCH_TIME_FIELD_NUMBER: _ClassVar[int]
    WARC_FILENAME_FIELD_NUMBER: _ClassVar[int]
    WARC_RECORD_OFFSET_FIELD_NUMBER: _ClassVar[int]
    WARC_RECORD_LENGTH_FIELD_NUMBER: _ClassVar[int]
    CONTENT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    MIME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    crawl: str
    url: str
    url_surtkey: str
    fetch_time: str
    warc_filename: str
    warc_record_offset: int
    warc_record_length: int
    content_digest: str
    mime: str
    status: int
    def __init__(self, crawl: _Optional[str] = ..., url: _Optional[str] = ..., url_surtkey: _Optional[str] = ..., fetch_time: _Optional[str] = ..., warc_filename: _Optional[str] = ..., warc_record_offset: _Optional[int] = ..., warc_record_length: _Optional[int] = ..., content_digest: _Optional[str] = ..., mime: _Optional[str] = ..., status: _Optional[int] = ...) -> None: ...

class Lease(_message.Message):
    __slots__ = ("lease_id", "job_id", "page", "attempt", "ttl_s", "data_base")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    TTL_S_FIELD_NUMBER: _ClassVar[int]
    DATA_BASE_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    job_id: str
    page: PageRef
    attempt: int
    ttl_s: float
    data_base: str
    def __init__(self, lease_id: _Optional[str] = ..., job_id: _Optional[str] = ..., page: _Optional[_Union[PageRef, _Mapping]] = ..., attempt: _Optional[int] = ..., ttl_s: _Optional[float] = ..., data_base: _Optional[str] = ...) -> None: ...

class Stored(_message.Message):
    __slots__ = ("raw_uri", "raw_offset", "raw_length", "raw_sha256", "http_status")
    RAW_URI_FIELD_NUMBER: _ClassVar[int]
    RAW_OFFSET_FIELD_NUMBER: _ClassVar[int]
    RAW_LENGTH_FIELD_NUMBER: _ClassVar[int]
    RAW_SHA256_FIELD_NUMBER: _ClassVar[int]
    HTTP_STATUS_FIELD_NUMBER: _ClassVar[int]
    raw_uri: str
    raw_offset: int
    raw_length: int
    raw_sha256: str
    http_status: int
    def __init__(self, raw_uri: _Optional[str] = ..., raw_offset: _Optional[int] = ..., raw_length: _Optional[int] = ..., raw_sha256: _Optional[str] = ..., http_status: _Optional[int] = ...) -> None: ...

class Result(_message.Message):
    __slots__ = ("lease_id", "outcome", "http_status", "retry_after_s", "bytes", "error", "started_at", "elapsed_ms", "stored")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    HTTP_STATUS_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_S_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_MS_FIELD_NUMBER: _ClassVar[int]
    STORED_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    outcome: Outcome
    http_status: int
    retry_after_s: float
    bytes: int
    error: str
    started_at: str
    elapsed_ms: int
    stored: Stored
    def __init__(self, lease_id: _Optional[str] = ..., outcome: _Optional[_Union[Outcome, str]] = ..., http_status: _Optional[int] = ..., retry_after_s: _Optional[float] = ..., bytes: _Optional[int] = ..., error: _Optional[str] = ..., started_at: _Optional[str] = ..., elapsed_ms: _Optional[int] = ..., stored: _Optional[_Union[Stored, _Mapping]] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("lease_id", "accepted")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    accepted: bool
    def __init__(self, lease_id: _Optional[str] = ..., accepted: _Optional[bool] = ...) -> None: ...
