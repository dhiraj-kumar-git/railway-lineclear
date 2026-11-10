from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListSegmentsRequest(_message.Message):
    __slots__ = ("held_only",)
    HELD_ONLY_FIELD_NUMBER: _ClassVar[int]
    held_only: bool
    def __init__(self, held_only: _Optional[bool] = ...) -> None: ...

class Segment(_message.Message):
    __slots__ = ("code", "kind", "hold", "hold_state", "txn_id", "train_number")
    CODE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    HOLD_FIELD_NUMBER: _ClassVar[int]
    HOLD_STATE_FIELD_NUMBER: _ClassVar[int]
    TXN_ID_FIELD_NUMBER: _ClassVar[int]
    TRAIN_NUMBER_FIELD_NUMBER: _ClassVar[int]
    code: str
    kind: str
    hold: str
    hold_state: str
    txn_id: str
    train_number: str
    def __init__(self, code: _Optional[str] = ..., kind: _Optional[str] = ..., hold: _Optional[str] = ..., hold_state: _Optional[str] = ..., txn_id: _Optional[str] = ..., train_number: _Optional[str] = ...) -> None: ...

class ListSegmentsReply(_message.Message):
    __slots__ = ("segments", "highest_term")
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    HIGHEST_TERM_FIELD_NUMBER: _ClassVar[int]
    segments: _containers.RepeatedCompositeFieldContainer[Segment]
    highest_term: int
    def __init__(self, segments: _Optional[_Iterable[_Union[Segment, _Mapping]]] = ..., highest_term: _Optional[int] = ...) -> None: ...

class ReleaseAllHoldsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ReleaseAllHoldsReply(_message.Message):
    __slots__ = ("released_holds", "aborted_transactions")
    RELEASED_HOLDS_FIELD_NUMBER: _ClassVar[int]
    ABORTED_TRANSACTIONS_FIELD_NUMBER: _ClassVar[int]
    released_holds: int
    aborted_transactions: int
    def __init__(self, released_holds: _Optional[int] = ..., aborted_transactions: _Optional[int] = ...) -> None: ...
