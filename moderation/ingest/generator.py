"""
Source of truth for how a chat message should look on the wire.

Every other part of StreamGuard assumes these six fields.
"""

import time
import uuid
from typing import Optional


def make_message(user_id: str,
                 text: str,
                 stream_id: str = "stream_1",
                 client_meta: Optional[dict] = None) -> dict:
    """Build a single chat message in our canonical format."""
    return {
        # A unique id per message. Used later to make sure we never
        # record the same decision twice.
        "msg_id": str(uuid.uuid4()),
        "stream_id": stream_id,
        "user_id": user_id,
        "text": text,
        "ts": time.time(),
        # Anything the client tells us about itself, e.g. how old the account is.
        "client_meta": client_meta or {},
    }
