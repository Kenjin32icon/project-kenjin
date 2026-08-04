"""
Shared-secret auth. Local-only (127.0.0.1) traffic doesn't strictly need
this, but building it in from day one means the Render deployment later
needs zero route changes - just an env var - instead of a security
retrofit under time pressure.
"""
import os
from fastapi import Header, HTTPException


def verify_api_key(x_api_key: str = Header(default="")) -> None:
    expected = os.environ.get("ORCH_API_KEY", "")
    if not expected:
        # Fail loud in any environment where the key was never set, rather
        # than silently accepting all requests - an empty expected key is
        # almost certainly a missing .env, not an intentional "no auth" mode.
        raise HTTPException(status_code=500, detail="ORCH_API_KEY is not configured on the server.")
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key header.")
