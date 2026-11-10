from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class CampaignRequest(_message.Message):
    __slots__ = ("election", "candidate_id")
    ELECTION_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    election: str
    candidate_id: str
    def __init__(self, election: _Optional[str] = ..., candidate_id: _Optional[str] = ...) -> None: ...

class CampaignReply(_message.Message):
    __slots__ = ("is_leader", "leader_id", "term", "lease_remaining_ms")
    IS_LEADER_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEASE_REMAINING_MS_FIELD_NUMBER: _ClassVar[int]
    is_leader: bool
    leader_id: str
    term: int
    lease_remaining_ms: int
    def __init__(self, is_leader: _Optional[bool] = ..., leader_id: _Optional[str] = ..., term: _Optional[int] = ..., lease_remaining_ms: _Optional[int] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("election", "candidate_id", "term")
    ELECTION_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    election: str
    candidate_id: str
    term: int
    def __init__(self, election: _Optional[str] = ..., candidate_id: _Optional[str] = ..., term: _Optional[int] = ...) -> None: ...

class HeartbeatReply(_message.Message):
    __slots__ = ("still_leader", "leader_id", "term", "lease_remaining_ms")
    STILL_LEADER_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEASE_REMAINING_MS_FIELD_NUMBER: _ClassVar[int]
    still_leader: bool
    leader_id: str
    term: int
    lease_remaining_ms: int
    def __init__(self, still_leader: _Optional[bool] = ..., leader_id: _Optional[str] = ..., term: _Optional[int] = ..., lease_remaining_ms: _Optional[int] = ...) -> None: ...

class GetLeaderRequest(_message.Message):
    __slots__ = ("election",)
    ELECTION_FIELD_NUMBER: _ClassVar[int]
    election: str
    def __init__(self, election: _Optional[str] = ...) -> None: ...

class GetLeaderReply(_message.Message):
    __slots__ = ("has_leader", "leader_id", "term", "lease_remaining_ms")
    HAS_LEADER_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEASE_REMAINING_MS_FIELD_NUMBER: _ClassVar[int]
    has_leader: bool
    leader_id: str
    term: int
    lease_remaining_ms: int
    def __init__(self, has_leader: _Optional[bool] = ..., leader_id: _Optional[str] = ..., term: _Optional[int] = ..., lease_remaining_ms: _Optional[int] = ...) -> None: ...
