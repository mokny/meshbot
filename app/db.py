"""
SQLite storage layer for the MeshBot.
by Till Vennefrohne - https://github.com/mokny/meshbot

Tables
------
stations
    One row per heard node_id. Used for /stats and /user.
names
    Name history (short/long) per node_id. Allows detecting renames over time.
messages
    RX and TX message history for each conversation (channel or DM).

Thread safety
-------------
The bot loop thread and the API thread both access the database.
SQLite itself is not fully thread-safe without care, so we use:
- check_same_thread=False (allow use from multiple threads)
- a single connection with a global lock (threading.Lock)
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple


class BotDB:
    """Small SQLite helper used by MeshBot and the HTTP API."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()

        # A single connection is sufficient here. We protect it with a lock.
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init()

    def _init(self) -> None:
        """Create tables and indexes if they do not exist yet."""
        with self._conn:
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS stations (
                    node_id TEXT PRIMARY KEY,
                    first_seen INTEGER NOT NULL,
                    last_seen INTEGER NOT NULL
                )"""
            )
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS names (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    seen_at INTEGER NOT NULL,
                    short TEXT,
                    long TEXT
                )"""
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_names_node_id ON names(node_id, id DESC)")

            # messages keeps the *last N* per conversation (channel or DM)
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    conversation_type TEXT NOT NULL, -- 'channel' or 'dm'
                    channel INTEGER,
                    channel_name TEXT,
                    peer_id TEXT, -- DM peer (if conversation_type='dm')
                    user_id TEXT, -- sender/recipient node_id (e.g. '!abcdef01')
                    name_short TEXT,
                    name_long TEXT,
                    node_key TEXT, -- which TCP node connection received/sent it (host:port)
                    direction TEXT NOT NULL, -- 'rx'/'tx'
                    message TEXT NOT NULL
                )"""
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_type, channel, peer_id, id DESC)"
            )

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._lock:
            self._conn.close()

    # ---- stations / names -------------------------------------------------

    def touch_station(self, node_id: str, ts: int) -> None:
        """
        Upsert station presence.

        If the station does not exist, insert it and set first_seen=last_seen=ts.
        If it exists, update last_seen to ts.
        """
        if not node_id:
            return
        with self._lock, self._conn:
            row = self._conn.execute("SELECT node_id FROM stations WHERE node_id=?", (node_id,)).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO stations(node_id, first_seen, last_seen) VALUES(?,?,?)",
                    (node_id, int(ts), int(ts)),
                )
            else:
                self._conn.execute("UPDATE stations SET last_seen=? WHERE node_id=?", (int(ts), node_id))

    def record_name(self, node_id: str, ts: int, short: Optional[str], long: Optional[str]) -> None:
        """
        Record a name-history entry if it changed.

        We only insert a new row if the newest row differs from (short,long).
        This keeps the history meaningful and avoids spam inserts.
        """
        if not node_id:
            return
        short_s = short.strip() if isinstance(short, str) and short.strip() else None
        long_s = long.strip() if isinstance(long, str) and long.strip() else None
        if not short_s and not long_s:
            return
        with self._lock, self._conn:
            last = self._conn.execute(
                "SELECT short,long FROM names WHERE node_id=? ORDER BY id DESC LIMIT 1", (node_id,)
            ).fetchone()
            if last is not None:
                if (last["short"] or None) == (short_s or None) and (last["long"] or None) == (long_s or None):
                    return
            self._conn.execute(
                "INSERT INTO names(node_id, seen_at, short, long) VALUES(?,?,?,?)",
                (node_id, int(ts), short_s, long_s),
            )

    def get_latest_name(self, node_id: str) -> Tuple[Optional[str], Optional[str]]:
        """Return latest (short,long) for a node, or (None,None)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT short,long FROM names WHERE node_id=? ORDER BY id DESC LIMIT 1", (node_id,)
            ).fetchone()
            if not row:
                return None, None
            return row["short"], row["long"]

    def get_name_history(
        self, node_id: str, limit: int = 50, order: str = "desc"
    ) -> List[Tuple[int, Optional[str], Optional[str]]]:
        """
        Return name history rows.

        order:
          - desc (newest first)
          - asc  (oldest first)
        """
        ord_norm = (order or "desc").strip().lower()
        if ord_norm not in ("asc", "desc"):
            ord_norm = "desc"
        with self._lock:
            rows = self._conn.execute(
                f"SELECT seen_at, short, long FROM names WHERE node_id=? ORDER BY id {ord_norm.upper()} LIMIT ?",
                (node_id, int(limit)),
            ).fetchall()
            return [(int(r["seen_at"]), r["short"], r["long"]) for r in rows]

    def get_station_summary(self, node_id: str) -> Tuple[Optional[int], Optional[int], int]:
        """Return (first_seen, last_seen, number_of_name_rows) or (None,None,0) if not present."""
        with self._lock:
            row = self._conn.execute(
                "SELECT first_seen,last_seen FROM stations WHERE node_id=?", (node_id,)
            ).fetchone()
            if row is None:
                return None, None, 0
            cnt = self._conn.execute("SELECT COUNT(*) AS c FROM names WHERE node_id=?", (node_id,)).fetchone()
            return int(row["first_seen"]), int(row["last_seen"]), int(cnt["c"]) if cnt else 0

    def count_stations(self) -> int:
        """Count known stations (db users)."""
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS c FROM stations").fetchone()
            return int(row["c"]) if row else 0

    def count_name_rows(self) -> int:
        """Count name history rows."""
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS c FROM names").fetchone()
            return int(row["c"]) if row else 0

    # ---- messages ---------------------------------------------------------

    def add_message(
        self,
        ts: int,
        conversation_type: str,
        channel: Optional[int],
        channel_name: Optional[str],
        peer_id: Optional[str],
        user_id: Optional[str],
        name_short: Optional[str],
        name_long: Optional[str],
        node_key: Optional[str],
        direction: str,
        message: str,
        keep: int,
    ) -> None:
        """
        Insert a message (RX or TX) and prune per-conversation history.

        Retention:
            If keep > 0, we keep only the newest 'keep' rows for the same conversation:
              conversation is identified by (conversation_type, channel, peer_id).
        """
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO messages(ts, conversation_type, channel, channel_name, peer_id, user_id,
                                          name_short, name_long, node_key, direction, message)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    int(ts),
                    conversation_type,
                    int(channel) if channel is not None else None,
                    channel_name,
                    peer_id,
                    user_id,
                    name_short,
                    name_long,
                    node_key,
                    direction,
                    message,
                ),
            )
            if keep and keep > 0:
                # Delete everything except the newest 'keep' rows of that same conversation.
                self._conn.execute(
                    """DELETE FROM messages
                       WHERE id NOT IN (
                           SELECT id FROM messages
                           WHERE conversation_type=? AND
                                 ( (channel IS ?) OR (channel = ?) ) AND
                                 ( (peer_id IS ?) OR (peer_id = ?) )
                           ORDER BY id DESC
                           LIMIT ?
                       )
                       AND conversation_type=? AND
                           ( (channel IS ?) OR (channel = ?) ) AND
                           ( (peer_id IS ?) OR (peer_id = ?) )
                    """,
                    (
                        conversation_type,
                        channel,
                        int(channel) if channel is not None else None,
                        peer_id,
                        peer_id,
                        int(keep),
                        conversation_type,
                        channel,
                        int(channel) if channel is not None else None,
                        peer_id,
                        peer_id,
                    ),
                )

    def get_messages(
        self,
        conversation_type: str,
        channel: Optional[int],
        peer_id: Optional[str],
        limit: int = 100,
        order: str = "desc",
        sort_by: str = "id",
        before_id: Optional[int] = None,
        after_id: Optional[int] = None,
        direction_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch message history for one conversation.

        This powers the history API endpoints.

        Parameters:
          - conversation_type: 'channel' or 'dm'
          - channel: channel index for channels, else None
          - peer_id: DM peer for DMs, else None
          - limit: max number of rows
          - order: 'asc' or 'desc'
          - sort_by: 'id' or 'ts'
          - before_id / after_id: cursor paging
          - direction_filter: 'rx' or 'tx' (optional)
        """
        ord_norm = (order or "desc").strip().lower()
        if ord_norm not in ("asc", "desc"):
            ord_norm = "desc"
        sort_norm = (sort_by or "id").strip().lower()
        if sort_norm not in ("id", "ts"):
            sort_norm = "id"

        q = """SELECT id, ts, conversation_type, channel, channel_name, peer_id, user_id, name_short, name_long,
                       node_key, direction, message
               FROM messages
               WHERE conversation_type=? AND
                     ( (channel IS ?) OR (channel = ?) ) AND
                     ( (peer_id IS ?) OR (peer_id = ?) )
            """
        params: List[Any] = [
            conversation_type,
            channel,
            int(channel) if channel is not None else None,
            peer_id,
            peer_id,
        ]

        if direction_filter in ("rx", "tx"):
            q += " AND direction = ?"
            params.append(direction_filter)

        if before_id is not None:
            q += " AND id < ?"
            params.append(int(before_id))
        if after_id is not None:
            q += " AND id > ?"
            params.append(int(after_id))

        # Stable ordering: if sorting by ts, include id as tiebreaker.
        if sort_norm == "ts":
            q += f" ORDER BY ts {ord_norm.upper()}, id {ord_norm.upper()} LIMIT ?"
        else:
            q += f" ORDER BY id {ord_norm.upper()} LIMIT ?"
        params.append(int(limit))

        with self._lock:
            rows = self._conn.execute(q, tuple(params)).fetchall()
            return [dict(r) for r in rows]
