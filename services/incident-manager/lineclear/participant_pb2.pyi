from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Vote(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VOTE_UNSPECIFIED: _ClassVar[Vote]
    VOTE_YES: _ClassVar[Vote]
    VOTE_NO: _ClassVar[Vote]

class TxnState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TXN_STATE_UNSPECIFIED: _ClassVar[TxnState]
    TXN_STATE_PREPARED: _ClassVar[TxnState]
    TXN_STATE_COMMITTED: _ClassVar[TxnState]
    TXN_STATE_ABORTED: _ClassVar[TxnState]
VOTE_UNSPECIFIED: Vote
VOTE_YES: Vote
VOTE_NO: Vote
TXN_STATE_UNSPECIFIED: TxnState
TXN_STATE_PREPARED: TxnState
TXN_STATE_COMMITTED: TxnState
TXN_STATE_ABORTED: TxnState

class DiversionPlan(_message.Message):
    __slots__ = ("incident_id", "train_number", "blocked_section", "loop_line")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    TRAIN_NUMBER_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_SECTION_FIELD_NUMBER: _ClassVar[int]
    LOOP_LINE_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    train_number: str
    blocked_section: str
    loop_line: str
    def __init__(self, incident_id: _Optional[str] = ..., train_number: _Optional[str] = ..., blocked_section: _Optional[str] = ..., loop_line: _Optional[str] = ...) -> None: ...

class PrepareRequest(_message.Message):
    __slots__ = ("txn_id", "term", "coordinator_id", "plan")
    TXN_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    COORDINATOR_ID_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    txn_id: str
    term: int
    coordinator_id: str
    plan: DiversionPlan
    def __init__(self, txn_id: _Optional[str] = ..., term: _Optional[int] = ..., coordinator_id: _Optional[str] = ..., plan: _Optional[_Union[DiversionPlan, _Mapping]] = ...) -> None: ...

class PrepareReply(_message.Message):
    __slots__ = ("vote", "reason")
    VOTE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    vote: Vote
    reason: str
    def __init__(self, vote: _Optional[_Union[Vote, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class DecisionRequest(_message.Message):
    __slots__ = ("txn_id", "term", "coordinator_id")
    TXN_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    COORDINATOR_ID_FIELD_NUMBER: _ClassVar[int]
    txn_id: str
    term: int
    coordinator_id: str
    def __init__(self, txn_id: _Optional[str] = ..., term: _Optional[int] = ..., coordinator_id: _Optional[str] = ...) -> None: ...

class DecisionReply(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: TxnState
    def __init__(self, state: _Optional[_Union[TxnState, str]] = ...) -> None: ...

class GetDecisionRequest(_message.Message):
    __slots__ = ("txn_id",)
    TXN_ID_FIELD_NUMBER: _ClassVar[int]
    txn_id: str
    def __init__(self, txn_id: _Optional[str] = ...) -> None: ...

class GetDecisionReply(_message.Message):
    __slots__ = ("state", "vote", "term")
    STATE_FIELD_NUMBER: _ClassVar[int]
    VOTE_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    state: TxnState
    vote: Vote
    term: int
    def __init__(self, state: _Optional[_Union[TxnState, str]] = ..., vote: _Optional[_Union[Vote, str]] = ..., term: _Optional[int] = ...) -> None: ...
