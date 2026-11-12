"""The staff API key: actions that change the line need the X-Staff-Key header.

The key comes from Secrets Manager through External Secrets, as STAFF_API_KEY. It is compared in
constant time, so the time an answer takes says nothing about how much of a guess was right.
"""

import hmac
import os

from fastapi import Header, HTTPException


def require_staff(x_staff_key: str | None = Header(default=None)) -> None:
    expected = os.environ.get("STAFF_API_KEY", "")
    if not expected or not x_staff_key or not hmac.compare_digest(x_staff_key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="A valid X-Staff-Key header is required")
