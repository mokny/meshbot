"""
FastAPI HTTP API for the MeshBot.
by Till Vennefrohne - https://github.com/mokny/meshbot

This API is intentionally simple:
- It authenticates requests with one of the configured tokens.
- It delegates actual work to the MeshBot instance (send messages, read stats/history).
- Responses are formatted with meta fields for easier client usage.

Auth
----
Send a token as either:
- Authorization: Bearer <token>
- X-Api-Token: <token>

History endpoints
----------------
They support:
- limit (1..1000)
- order (asc|desc)
- sort_by (id|ts)
- before_id / after_id (paging cursors)
- direction (rx|tx)

The response includes:
- items: list of messages
- meta: conversation details + paging hints (next_before_id / next_after_id)
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List

from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field


def _check_token(tokens: List[str], authorization: Optional[str], x_api_token: Optional[str]) -> None:
    """
    Validate API auth.

    If no tokens are configured, the API is effectively disabled (401).
    """
    if not tokens:
        raise HTTPException(status_code=401, detail="No API tokens configured")

    tok = None

    # Prefer standard Authorization header
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            tok = a[7:].strip()

    # Alternative header for convenience (e.g. curl)
    if not tok and x_api_token:
        tok = x_api_token.strip()

    if not tok or tok not in tokens:
        raise HTTPException(status_code=401, detail="Unauthorized")


class SendChannelReq(BaseModel):
    """POST body for sending a channel message."""
    channel: int = Field(..., ge=0)
    text: str
    node: Optional[str] = None
    destination_id: str = "^all"  # broadcast by default


class SendDMReq(BaseModel):
    """POST body for sending a DM."""
    user_id: str
    text: str
    node: Optional[str] = None


def create_app(bot: Any) -> FastAPI:
    """
    Build and return the FastAPI application.

    'bot' is the MeshBot instance created by main.py.
    """
    app = FastAPI(title="Meshtastic Bot API", version="3.0")

    @app.get("/health")
    def health() -> Dict[str, Any]:
        """Simple health check (no auth)."""
        return {"ok": True}

    @app.get("/stats")
    def stats(
        authorization: Optional[str] = Header(default=None),
        x_api_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """Return bot and DB statistics."""
        _check_token(bot.cfg.api.tokens, authorization, x_api_token)
        return {"data": bot.get_stats()}

    @app.get("/user/{node_id}")
    def user(
        node_id: str,
        name_limit: int = Query(50, ge=1, le=500),
        name_order: str = Query("desc", pattern="^(asc|desc)$"),
        authorization: Optional[str] = Header(default=None),
        x_api_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """Return station info and name history for a given node_id."""
        _check_token(bot.cfg.api.tokens, authorization, x_api_token)
        info = bot.get_user_info(node_id=node_id, name_limit=int(name_limit), name_order=name_order)
        if not info:
            raise HTTPException(status_code=404, detail="Not found")
        return {"data": info}

    def _normalize(order: str, sort_by: str, direction: Optional[str], limit: int) -> Dict[str, Any]:
        """Normalize and validate query params for history endpoints."""
        ord_norm = (order or "desc").lower().strip()
        if ord_norm not in ("asc", "desc"):
            ord_norm = "desc"
        sort_norm = (sort_by or "id").lower().strip()
        if sort_norm not in ("id", "ts"):
            sort_norm = "id"
        dir_norm = (direction or "").lower().strip() or None
        if dir_norm not in (None, "rx", "tx"):
            dir_norm = None
        lim = max(1, min(int(limit), 1000))
        return {"order": ord_norm, "sort_by": sort_norm, "direction": dir_norm, "limit": lim}

    @app.get("/history/channel/{channel}")
    def history_channel(
        channel: int,
        limit: int = Query(100, ge=1, le=1000),
        order: str = Query("desc", pattern="^(asc|desc)$"),
        sort_by: str = Query("id", pattern="^(id|ts)$"),
        before_id: Optional[int] = None,
        after_id: Optional[int] = None,
        direction: Optional[str] = Query(None, pattern="^(rx|tx)$"),
        authorization: Optional[str] = Header(default=None),
        x_api_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """Return history for a channel conversation."""
        _check_token(bot.cfg.api.tokens, authorization, x_api_token)
        n = _normalize(order, sort_by, direction, limit)

        items = bot.get_history_channel(
            channel=int(channel),
            limit=n["limit"],
            order=n["order"],
            sort_by=n["sort_by"],
            before_id=before_id,
            after_id=after_id,
            direction=n["direction"],
        )

        # History responses include a meta object with paging cursors.
        # - When order=desc, use next_before_id to fetch older messages.
        # - When order=asc, use next_after_id to fetch newer messages.
        next_before = None
        next_after = None
        if items:
            ids = [int(it["id"]) for it in items if "id" in it]
            if n["order"] == "desc":
                next_before = min(ids)
            else:
                next_after = max(ids)

        return {
            "items": items,
            "meta": {
                "conversation": {"type": "channel", "channel": int(channel)},
                "limit": n["limit"],
                "order": n["order"],
                "sort_by": n["sort_by"],
                "direction": n["direction"],
                "before_id": before_id,
                "after_id": after_id,
                "next_before_id": next_before,
                "next_after_id": next_after,
            },
        }

    @app.get("/history/dm/{peer_id}")
    def history_dm(
        peer_id: str,
        limit: int = Query(100, ge=1, le=1000),
        order: str = Query("desc", pattern="^(asc|desc)$"),
        sort_by: str = Query("id", pattern="^(id|ts)$"),
        before_id: Optional[int] = None,
        after_id: Optional[int] = None,
        direction: Optional[str] = Query(None, pattern="^(rx|tx)$"),
        authorization: Optional[str] = Header(default=None),
        x_api_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """Return history for a DM conversation."""
        _check_token(bot.cfg.api.tokens, authorization, x_api_token)
        n = _normalize(order, sort_by, direction, limit)

        items = bot.get_history_dm(
            peer=str(peer_id),
            limit=n["limit"],
            order=n["order"],
            sort_by=n["sort_by"],
            before_id=before_id,
            after_id=after_id,
            direction=n["direction"],
        )

        next_before = None
        next_after = None
        if items:
            ids = [int(it["id"]) for it in items if "id" in it]
            if n["order"] == "desc":
                next_before = min(ids)
            else:
                next_after = max(ids)

        return {
            "items": items,
            "meta": {
                "conversation": {"type": "dm", "peer_id": str(peer_id)},
                "limit": n["limit"],
                "order": n["order"],
                "sort_by": n["sort_by"],
                "direction": n["direction"],
                "before_id": before_id,
                "after_id": after_id,
                "next_before_id": next_before,
                "next_after_id": next_after,
            },
        }

    @app.post("/send/channel")
    def send_channel(
        req: SendChannelReq,
        authorization: Optional[str] = Header(default=None),
        x_api_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """Send a message to a channel (broadcast by default)."""
        _check_token(bot.cfg.api.tokens, authorization, x_api_token)
        ok = bot.api_send_channel(channel=req.channel, text=req.text, node=req.node, destination_id=req.destination_id)
        return {"ok": ok}

    @app.post("/send/dm")
    def send_dm(
        req: SendDMReq,
        authorization: Optional[str] = Header(default=None),
        x_api_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """Send a DM to a user."""
        _check_token(bot.cfg.api.tokens, authorization, x_api_token)
        ok = bot.api_send_dm(user_id=req.user_id, text=req.text, node=req.node)
        return {"ok": ok}

    return app
