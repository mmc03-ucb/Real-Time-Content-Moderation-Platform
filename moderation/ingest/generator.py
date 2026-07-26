"""
Source of truth for how a message should be formatted
"""

import uuid
from datetime import datetime, timezone

def make_message(user_id: str, text:str) -> dict:
    # Build a single chat message in our canonical wire format
    return {
        "message_id": str(uuid.uuid4()), # uuid is gives an unique id that is converted to string so that it is JSON serializable
        "user_id": user_id,
        "text": text,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }