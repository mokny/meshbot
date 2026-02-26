"""
MeshBot logic.
by Till Vennefrohne - https://github.com/mokny/meshbot

High-level overview
-------------------
- NodeClient: maintains a TCP connection to one Meshtastic node and can send text messages.
- MeshBot:
  - subscribes to Meshtastic pubsub RX events
  - persists stations / names / message history to SQLite
  - replies to commands in the same conversation (channel vs DM)
  - optional: forwards incoming text to a webhook (config only)
  - optional: sends heartbeat messages (disabled by default)
  - optional: sends scheduled messages at fixed times

Important message rules
-----------------------
- All outgoing messages are limited to MAX_LEN = 190 characters.
- If a message exceeds MAX_LEN, it is split into numbered parts "1/N ...".
- If multiple parts are sent, there is a delay of MULTIPART_DELAY_SECONDS (2s) between parts.

Notes about Meshtastic data model
--------------------------------
Meshtastic delivers packets via pubsub topics, often as Python dicts.
Text messages are typically decoded with:
    packet["decoded"]["portnum"] == "TEXT_MESSAGE_APP"
and the text is in:
    packet["decoded"]["text"]

We also see:
- packet["fromId"] (string like "!bbfbc373")
- packet["from"] (numeric node id)
- packet["to"] / packet["toId"]
- packet["decoded"]["channel"] or packet["channel"]
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

from zoneinfo import ZoneInfo

import requests
from pubsub import pub
from meshtastic.tcp_interface import TCPInterface

from .config import AppCfg
from .db import BotDB
from .plugins import PluginManager

# Botversion
BOTVERSION = "2.01"

# TRACE is a custom log level more verbose than DEBUG.
TRACE = 5

# Meshtastic networks often have small message limits; we enforce 190 characters.
MAX_LEN = 190

# Delay between multipart message parts (seconds)
MULTIPART_DELAY_SECONDS = 2

# Meshtastic uses 0xFFFFFFFF for broadcast ("to everyone")
BROADCAST_TO_NUM = 0xFFFFFFFF


def install_trace_level() -> None:
    """
    Install a custom TRACE logging level.

    Usage:
        log.setLevel(TRACE)  # if configured
        log.trace("...")     # most verbose logging
    """
    if not hasattr(logging, "TRACE"):
        logging.addLevelName(TRACE, "TRACE")

        def trace(self: logging.Logger, msg: str, *args: Any, **kwargs: Any) -> None:
            if self.isEnabledFor(TRACE):
                self._log(TRACE, msg, args, **kwargs)

        logging.Logger.trace = trace  # type: ignore[attr-defined]


def json_default(o: Any) -> Any:
    """JSON serializer fallback for bytes and other non-serializable objects."""
    if isinstance(o, (bytes, bytearray, memoryview)):
        return {"__bytes_hex__": bytes(o).hex()}
    return str(o)


def make_jsonable(obj: Any) -> Any:
    """
    Convert an arbitrary object into something JSON-serializable.

    This avoids crashes when TRACE logging or webhook payloads include 'bytes' fields.
    """
    if isinstance(obj, dict):
        return {str(k): make_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_jsonable(v) for v in obj]
    if isinstance(obj, tuple):
        return [make_jsonable(v) for v in obj]
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return {"__bytes_hex__": bytes(obj).hex()}
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


def _chunk_text(text: str, max_len: int = MAX_LEN) -> List[str]:
    """
    Split a long message into parts that fit MAX_LEN.

    Parts are prefixed with "i/N " for better readability on the receiving device.
    """
    t = (text or "").strip()
    if len(t) <= max_len:
        return [t]

    raw_parts: List[str] = []
    i = 0
    while i < len(t):
        raw_parts.append(t[i : i + max_len])
        i += max_len

    total = len(raw_parts)
    parts: List[str] = []
    for idx, part in enumerate(raw_parts, start=1):
        prefix = f"{idx}/{total} "
        allowed = max_len - len(prefix)
        parts.append(prefix + part[:allowed])
    return parts


def _norm_name_val(v: Any) -> Optional[str]:
    """
    Normalize a short/long name value.

    Meshtastic metadata can come in different shapes and sometimes values are not strings.
    We convert to string and strip whitespace; empty strings become None.
    """
    if v is None:
        return None
    try:
        s = str(v).strip()
    except Exception:
        return None
    return s if s else None


def _extract_names_from_user_dict(user: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract (short,long) from a "user" dict with many possible key variants.
    """
    if not isinstance(user, dict):
        return None, None

    # Common Meshtastic python keys
    short = (
        user.get("shortName") or user.get("shortname") or user.get("short_name")
        or user.get("nameShort") or user.get("name_short") or user.get("name_short_name")
        or user.get("short")
    )
    long = (
        user.get("longName") or user.get("longname") or user.get("long_name")
        or user.get("nameLong") or user.get("name_long") or user.get("nameLongName")
        or user.get("long")
    )
    return _norm_name_val(short), _norm_name_val(long)


def _extract_names_from_packet(packet: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Try to extract short/long names from the packet.

    Note: many packets (especially TEXT_MESSAGE_APP) do NOT include user data.
    This function is best-effort and intentionally tolerant to different firmware/library versions.
    """
    decoded = packet.get("decoded", {}) or {}

    # Most common location (some firmwares)
    short, long = _extract_names_from_user_dict(decoded.get("user"))

    # Alternative locations (rare, but seen in some payload variants)
    if not short and not long:
        short2, long2 = _extract_names_from_user_dict(packet.get("user"))
        short = short or short2
        long = long or long2

    if not short and not long:
        # Sometimes "decoded" contains a nested "sender" or similar dict
        short2, long2 = _extract_names_from_user_dict(decoded.get("sender"))
        short = short or short2
        long = long or long2

    return short, long


def _extract_names_from_interface(interface: Any, from_id: str, from_num: Optional[int] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Fallback: read names from interface.nodes cache.

    Depending on Meshtastic python version, the keys inside interface.nodes can vary:
    - "!bbfbc373" (fromId string)
    - "bbfbc373"  (sometimes without the leading '!')
    - numeric node id (int) (rare)

    Therefore we try a set of candidate keys.
    """
    try:
        nodes = getattr(interface, "nodes", None)
        if not isinstance(nodes, dict):
            return None, None

        keys = []
        fid = str(from_id or "").strip()
        if fid:
            keys.append(fid)
            if fid.startswith("!"):
                keys.append(fid[1:])
            else:
                keys.append("!" + fid)

        if isinstance(from_num, int) and from_num >= 0:
            # Some caches might use numeric ids or hex strings
            keys.append(from_num)
            keys.append(f"{from_num:08x}")
            keys.append(f"!{from_num:08x}")

        for k in keys:
            if k in nodes:
                info = nodes.get(k) or {}
                user = info.get("user") or info.get("userInfo") or {}
                s, l = _extract_names_from_user_dict(user)
                if s or l:
                    return s, l
        return None, None
    except Exception:
        return None, None



def _derive_from_num(packet: Dict[str, Any]) -> Optional[int]:
    """
    Best-effort extraction of numeric 'from' value from a packet.

    Returns None if not present or not parseable.
    """
    try:
        frm = packet.get("from")
        if frm is None:
            return None
        if isinstance(frm, str):
            return int(frm, 0)
        return int(frm)
    except Exception:
        return None


def _derive_from_id(packet: Dict[str, Any]) -> str:
    """
    Derive a stable node_id string (e.g. '!bbfbc373').

    Some packets contain fromId directly.
    If not, we derive it from numeric 'from' as hex.
    """
    fid = str(packet.get("fromId", "") or "").strip()
    if fid:
        return fid
    try:
        frm = packet.get("from")
        frm_i = int(frm, 0) if isinstance(frm, str) else int(frm)
        return f"!{frm_i:08x}"
    except Exception:
        return ""


def _packet_ts(packet: Dict[str, Any]) -> int:
    """
    Extract a timestamp from packet metadata if available.

    - Some packets include rxTime (epoch seconds).
    - If not present, use current time.
    """
    for k in ("rxTime", "rx_time", "rxTimeSec"):
        v = packet.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return int(v)
    return int(time.time())


def _channel_index(packet: Dict[str, Any]) -> int:
    """Extract channel index (int) from packet."""
    decoded = packet.get("decoded", {}) or {}
    ch = decoded.get("channel", packet.get("channel", 0))
    try:
        if isinstance(ch, str):
            return int(ch, 0)
        return int(ch)
    except Exception:
        return 0


def _is_text_packet(packet: Dict[str, Any]) -> bool:
    """Return True if packet looks like a text message."""
    decoded = packet.get("decoded", {}) or {}
    if decoded.get("portnum") == "TEXT_MESSAGE_APP":
        return True
    t = decoded.get("text")
    return isinstance(t, str) and bool(t.strip())


def _get_text(packet: Dict[str, Any]) -> str:
    """Extract decoded text message."""
    decoded = packet.get("decoded", {}) or {}
    t = decoded.get("text")
    return t.strip() if isinstance(t, str) else ""


def _is_broadcast(packet: Dict[str, Any]) -> bool:
    """
    Determine whether the incoming packet was broadcast.

    If broadcast:
      - Treat the conversation as a channel conversation.
      - Replies should go to '^all' in that same channel.
    If not broadcast:
      - Treat as DM and reply to from_id.
    """
    to_num = packet.get("to")
    try:
        to_int = int(to_num, 0) if isinstance(to_num, str) else int(to_num)
        if to_int == BROADCAST_TO_NUM:
            return True
    except Exception:
        pass

    to_id = packet.get("toId")
    if to_id is None:
        return True
    return str(to_id).lower() in ("!ffffffff", "ffffffff", "0xffffffff", "^all")


def _reply_dest(packet: Dict[str, Any], from_id: str) -> str:
    """Return destinationId for replies: '^all' for channel messages, else sender ID for DMs."""
    return "^all" if _is_broadcast(packet) else from_id


def _hops(packet: Dict[str, Any]) -> int:
    """
    Estimate hops from hopStart and hopLimit if present.

    hops = hopStart - hopLimit
    """
    hop_start = packet.get("hopStart")
    hop_limit = packet.get("hopLimit")
    try:
        if hop_start is None or hop_limit is None:
            return 0
        hs = int(hop_start)
        hl = int(hop_limit)
        return max(0, hs - hl)
    except Exception:
        return 0


def _via(packet: Dict[str, Any]) -> str:
    """Return 'MQTT' if packet indicates it came via MQTT, else 'LoRa'."""
    v = packet.get("viaMqtt", False)
    try:
        return "MQTT" if bool(v) else "LoRa"
    except Exception:
        return "LoRa"


@dataclass
class Incoming:
    """
    A normalized representation of an incoming Meshtastic packet.

    We store fields that are frequently needed by the bot loop.
    """
    packet: Dict[str, Any]
    node_key: str
    iface: Any
    ts: int
    from_id: str
    short: Optional[str]
    long: Optional[str]


class NodeClient:
    """
    Maintains a resilient TCP connection to a Meshtastic node.

    - Connects using meshtastic.tcp_interface.TCPInterface(hostname, portNumber).
    - Reconnects on errors with exponential backoff.
    - send_text() enforces message splitting and delay.
    """

    def __init__(self, host: str, port: int, name: str, on_iface: Any, on_tx: Any, log: logging.Logger) -> None:
        self.host = host
        self.port = port
        self.name = name or f"{host}:{port}"
        self.key = f"{host}:{port}"

        self.iface: Optional[TCPInterface] = None
        self.connected = threading.Event()
        self.stop_event = threading.Event()

        # Callback called once a TCPInterface is created (so the bot can map iface -> node_key).
        self._on_iface = on_iface

        # Callback called for every sent message part (so the bot can store TX history).
        self._on_tx = on_tx

        self.log = log

    def connect_loop(self) -> None:
        """
        Background loop that keeps the TCP interface alive.

        This method runs forever (until stop_event is set):
        - Try to connect to the node.
        - On success, register the interface with the bot and keep the connection open.
        - On error, close the interface, wait with backoff, and retry.
        """
        backoff = 2
        while not self.stop_event.is_set():
            try:
                # NOTE: meshtastic uses the parameter name portNumber (not 'port')
                self.iface = TCPInterface(hostname=self.host, portNumber=self.port)

                # Let MeshBot know that this interface belongs to this node_key
                self._on_iface(self.iface, self.key)

                self.connected.set()
                self.log.info("[%s] connected", self.key)
                backoff = 2

                # Keep the connection alive; Meshtastic has its own reader thread.
                while not self.stop_event.is_set():
                    time.sleep(1)
                    _ = getattr(self.iface, "nodes", None)  # touch nodes to detect interface issues
            except Exception as e:
                # Connection failed or got dropped: clear flag and retry.
                self.connected.clear()
                self.log.warning("[%s] connect error: %r", self.key, e)
                try:
                    if self.iface:
                        self.iface.close()
                except Exception:
                    pass
                self.iface = None
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def close(self) -> None:
        """Stop this client and close its TCP interface."""
        self.stop_event.set()
        self.connected.clear()
        try:
            if self.iface:
                self.iface.close()
        except Exception:
            pass
        self.iface = None

    def send_text(self, text: str, destination_id: str, channel_index: int) -> bool:
        """
        Send a text message via Meshtastic.

        Important:
        - Enforces MAX_LEN by splitting into numbered parts.
        - If multiple parts are needed, waits MULTIPART_DELAY_SECONDS between parts.
        - Uses TCPInterface.sendText(text, destinationId=..., channelIndex=...).
        """
        if not self.iface or not self.connected.is_set():
            return False

        parts = _chunk_text(text, MAX_LEN)
        ok_all = True

        for i, part in enumerate(parts):
            try:
                self.log.debug("[%s] TX dest=%s ch=%s text=%r", self.key, destination_id, channel_index, part)

                # TRACE prints full payload as JSON (safe conversion for bytes)
                if self.log.isEnabledFor(TRACE):
                    self.log.trace(
                        "[%s] TX payload=%s",
                        self.key,
                        json.dumps({"dest": destination_id, "ch": channel_index, "text": part},
                                   ensure_ascii=False, default=json_default),
                    )

                # This is the actual Meshtastic send
                self.iface.sendText(part, destinationId=destination_id, channelIndex=channel_index)

                # Notify MeshBot for DB storage of TX messages
                try:
                    self._on_tx(self.key, part, destination_id, channel_index)
                except Exception:
                    pass

                # Delay between parts
                if len(parts) > 1 and i < (len(parts) - 1):
                    time.sleep(MULTIPART_DELAY_SECONDS)
            except Exception as e:
                self.log.error("[%s] send error: %r", self.key, e)
                ok_all = False

        return ok_all


class MeshBot:
    """
    Main bot controller.

    Thread model:
    - Meshtastic RX callback runs in pubsub context and enqueues Incoming events.
    - A bot-loop thread dequeues events, stores them, and replies.
    - A scheduler thread ticks once per second and sends scheduled messages.
    - The FastAPI server runs in another thread (started in main.py).

    Conversation behaviour:
    - If incoming message was broadcast -> reply to '^all' in the same channel.
    - If incoming message was DM -> reply to sender as DM.
    """

    def __init__(self, cfg: AppCfg, config_path: str, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.log = logger

        # One NodeClient per configured node
        self.clients: Dict[str, NodeClient] = {}
        self.node_name_by_key: Dict[str, str] = {}

        # Map TCPInterface objects to node keys (host:port)
        self._iface_to_key: Dict[int, str] = {}
        self._iface_lock = threading.Lock()

        # Incoming message queue; filled by _on_receive, drained by _loop
        self._q: "queue.Queue[Incoming]" = queue.Queue()
        self._stop = threading.Event()

        # Heartbeat state
        self._last_hb = 0.0

        # Scheduler state: ensure one send per minute per schedule item
        self._sched_last_sent: Dict[str, int] = {}

        # Persistent storage
        self.db = BotDB(self.cfg.db.path)

        # Plugin manager (loads *.py from cfg.plugins.path)
        self.plugins = PluginManager(bot=self, path=self.cfg.plugins.path, enabled=self.cfg.plugins.enabled, log=self.log)

        # Stats
        self.start_time = time.time()
        self.rx_messages = 0
        self.tx_messages = 0

    def start(self) -> None:
        """
        Start the bot.

        1) Subscribe to Meshtastic pubsub receive topics.
        2) Start connection threads for all configured nodes.
        3) Start the bot processing loop thread.
        4) Start the scheduler loop thread.
        """
        # Meshtastic emits receive events on these topics (varies by version)
        pub.subscribe(self._on_receive, "meshtastic.receive")
        pub.subscribe(self._on_receive, "meshtastic.receive.data")

        # Version info
        self.log.info("Version: %s by Till Vennefrohne https://github.com/mokny/meshbot", BOTVERSION)

        # Load plugins from disk (alphabetical order)
        self.plugins.load_all()

        # Start NodeClient connection threads
        for n in self.cfg.nodes:
            c = NodeClient(n.host, n.port, n.name, self._register_interface, self._on_tx, self.log)
            self.clients[c.key] = c
            self.node_name_by_key[c.key] = n.name or c.key
            threading.Thread(target=c.connect_loop, name=f"connect-{c.key}", daemon=True).start()

        self.log.info("Schedules loaded: %d item(s)", len(self.cfg.schedules.items or []))
        self.log.info("Command blocks loaded: %s | DM blocks: %s",
                      self.cfg.bot.command_blocks, self.cfg.bot.command_blocks_dm)

        if self.cfg.bot.commands_enabled:
            self.log.info("Bot-Commands enabled.")
        else:                
            self.log.info("Bot-Commands disabled in config.toml. The bot will not react to any command.")

        # Start worker threads
        threading.Thread(target=self._loop, name="bot-loop", daemon=True).start()
        threading.Thread(target=self._scheduler_loop, name="scheduler-loop", daemon=True).start()

        # Notify plugins that the bot is running
        self.plugins.on_start()

    def stop(self) -> None:
        """Stop threads, close DB, and close all TCP connections."""
        self._stop.set()
        try:
            self.plugins.on_stop()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass
        for c in self.clients.values():
            c.close()

    # ----- mapping interface->node_key -----

    def _register_interface(self, iface: Any, node_key: str) -> None:
        """Register TCPInterface -> node_key mapping."""
        with self._iface_lock:
            self._iface_to_key[id(iface)] = node_key

    def _node_key_for_interface(self, iface: Any) -> str:
        """
        Resolve node_key for a received packet.

        Prefer the mapping from _register_interface(); fallback to reading hostname/port from iface.
        """
        with self._iface_lock:
            k = self._iface_to_key.get(id(iface))
        if k:
            return k
        host = getattr(iface, "hostname", None) or getattr(iface, "host", None) or "unknown"
        port = getattr(iface, "portNumber", None) or getattr(iface, "port", None) or 4403
        return f"{host}:{int(port)}"

    # ----- channel name -----

    def channel_name(self, channel_index: int) -> str:
        """Return a friendly channel name (from config) or a fallback like 'ch7'."""
        n = (self.cfg.bot.channel_names or {}).get(int(channel_index))
        return str(n) if n else f"ch{int(channel_index)}"

    # ----- client selection -----

    def get_client(self, node: Optional[str]) -> Optional[NodeClient]:
        """
        Select a connected NodeClient.

        node can match:
        - configured node name
        - node key "host:port"
        If not provided, returns the first connected client.
        """
        if node:
            for k, name in self.node_name_by_key.items():
                if node == name or node == k:
                    c = self.clients.get(k)
                    if c and c.connected.is_set():
                        return c
            c2 = self.clients.get(node)
            if c2 and c2.connected.is_set():
                return c2
        return next((c for c in self.clients.values() if c.connected.is_set()), None)

    # ----- TX callback (store TX history) -----

    def _on_tx(self, node_key: str, part: str, destination_id: str, channel_index: int) -> None:
        """
        Called after each message part is sent.

        We store TX parts in the messages table so history includes bot responses too.
        """
        self.tx_messages += 1
        conv_type, ch, peer = self._conversation_for_outgoing(destination_id, channel_index)
        self.db.add_message(
            ts=int(time.time()),
            conversation_type=conv_type,
            channel=ch,
            channel_name=self.channel_name(ch or channel_index) if conv_type == "channel" else "DM",
            peer_id=peer,
            user_id=destination_id if destination_id else None,
            name_short=None,
            name_long=None,
            node_key=node_key,
            direction="tx",
            message=part,
            keep=int(self.cfg.db.keep_per_conversation),
        )

    # ----- conversation normalization (channel vs DM) -----

    def _conversation_for_incoming(self, packet: Dict[str, Any], from_id: str, channel_index: int) -> Tuple[str, Optional[int], Optional[str]]:
        """Return normalized (conversation_type, channel, peer_id) for an incoming packet."""
        if _is_broadcast(packet):
            return ("channel", int(channel_index), None)
        return ("dm", None, from_id)

    def _conversation_for_outgoing(self, destination_id: str, channel_index: int) -> Tuple[str, Optional[int], Optional[str]]:
        """Return normalized (conversation_type, channel, peer_id) for an outgoing send."""
        if destination_id and str(destination_id).lower() in ("^all", "!ffffffff", "ffffffff", "0xffffffff"):
            return ("channel", int(channel_index), None)
        return ("dm", None, destination_id)

    # ----- API hooks -----

    def api_send_channel(self, channel: int, text: str, node: Optional[str] = None, destination_id: str = "^all") -> bool:
        """Send a channel/broadcast message through a selected node connection."""
        client = self.get_client(node)
        if not client:
            return False
        return client.send_text(text, destination_id=destination_id, channel_index=int(channel))

    def api_send_dm(self, user_id: str, text: str, node: Optional[str] = None) -> bool:
        """Send a DM through a selected node connection."""
        client = self.get_client(node)
        if not client:
            return False
        return client.send_text(text, destination_id=str(user_id), channel_index=0)

    def get_history_channel(self, channel: int, limit: int, order: str, sort_by: str,
                            before_id: Optional[int], after_id: Optional[int], direction: Optional[str]) -> List[Dict[str, Any]]:
        """Return DB history for one channel conversation."""
        return self.db.get_messages(
            conversation_type="channel",
            channel=int(channel),
            peer_id=None,
            limit=limit,
            order=order,
            sort_by=sort_by,
            before_id=before_id,
            after_id=after_id,
            direction_filter=direction,
        )

    def get_history_dm(self, peer: str, limit: int, order: str, sort_by: str,
                       before_id: Optional[int], after_id: Optional[int], direction: Optional[str]) -> List[Dict[str, Any]]:
        """Return DB history for one DM conversation."""
        return self.db.get_messages(
            conversation_type="dm",
            channel=None,
            peer_id=str(peer),
            limit=limit,
            order=order,
            sort_by=sort_by,
            before_id=before_id,
            after_id=after_id,
            direction_filter=direction,
        )

    # ----- stats / user info -----

    def _fmt_uptime(self, seconds: int) -> str:
        """Format uptime into a human-readable string (e.g. '1d 2h 3m 4s')."""
        if seconds < 0:
            seconds = 0
        days, rem = divmod(int(seconds), 86400)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)
        parts: List[str] = []
        if days:
            parts.append(f"{days}d")
        if hours or days:
            parts.append(f"{hours}h")
        if minutes or hours or days:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    def get_stats(self) -> Dict[str, Any]:
        """Return bot+DB statistics used by /stats and GET /stats."""
        stations = self.db.count_stations()
        name_rows = self.db.count_name_rows()
        uptime_s = int(time.time() - self.start_time)
        return {
            "db_users": stations,
            "db_name_rows": name_rows,
            "uptime_seconds": uptime_s,
            "uptime_human": self._fmt_uptime(uptime_s),
            "rx_messages": self.rx_messages,
            "tx_messages": self.tx_messages,
        }

    def get_user_info(self, node_id: str, name_limit: int = 50, name_order: str = "desc") -> Optional[Dict[str, Any]]:
        """
        Return station info + name history for /user and GET /user/{node_id}.
        """
        first_seen, last_seen, name_count = self.db.get_station_summary(node_id)
        if first_seen is None:
            return None
        hist = self.db.get_name_history(node_id, limit=int(name_limit), order=name_order)
        return {
            "node_id": node_id,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "name_entries": name_count,
            "name_history": [{"seen_at": t, "short": s, "long": l} for (t, s, l) in hist],
        }

    # ----- command detection + blocking -----

    def _matched_command_key(self, text: str) -> Optional[str]:
        """
        Determine which command is invoked by the given text.

        Returns:
          - the lowercased trigger for built-in commands
          - the lowercased trigger for a custom command
          - None if not a command
        """
        t = (text or "").strip()
        if not t:
            return None
        low = t.lower()

        builtins = ["/help", "/ping", "/user", "/stats"]
        for b in builtins:
            if low.startswith(b):
                return b

        # Custom commands: match by exact trigger prefix
        for c in self.cfg.commands.list:
            trig = (c.trigger or "").strip()
            if trig and t.startswith(trig):
                return trig.strip().lower()

        return None

    def _is_blocked(self, cmd_key: str, channel_index: int, is_dm: bool) -> bool:
        """
        Evaluate command blocks.

        - Per-channel blocking: config [bot.command_blocks] {"/cmd"=[7,"Trusted",...]}
        - DM blocking: use "dm" in the list for a command
        """
        key = (cmd_key or "").strip().lower()
        if not key:
            return False

        # DM blocking
        if is_dm and key in (self.cfg.bot.command_blocks_dm or []):
            self.log.info("Command blocked (DM): %s", key)
            return True

        # Channel blocking
        blocked = (self.cfg.bot.command_blocks or {}).get(key, [])
        if (not is_dm) and blocked and (int(channel_index) in set(int(x) for x in blocked)):
            self.log.info("Command blocked (channel): %s in ch=%s (blocked=%s)", key, channel_index, blocked)
            return True

        return False

    # ----- webhook (config only) -----

    def _post_webhook(self, packet: Dict[str, Any], node_key: str, channel_index: int, text: str) -> None:
        """
        Forward a received text message to the configured webhook URL.

        NOTE:
        - Webhook is configured only via config.toml. There are no chat commands.
        - Payload includes channelName for easier parsing on the receiver side.
        """
        if not self.cfg.webhook.url:
            return

        host, port = (node_key.split(":") + ["4403"])[:2]
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "node": {"host": host, "port": int(port), "name": self.node_name_by_key.get(node_key, node_key)},
            "fromId": packet.get("fromId") or _derive_from_id(packet),
            "toId": packet.get("toId"),
            "channel": int(channel_index),
            "channelName": self.channel_name(int(channel_index)),
            "text": text,
            "raw": make_jsonable(packet),
        }

        try:
            requests.post(
                self.cfg.webhook.url,
                json=payload,
                timeout=self.cfg.webhook.timeout_seconds,
                headers={"User-Agent": "meshtastic-bot-suite/3.0"},
            ).raise_for_status()
        except Exception as e:
            self.log.warning("[webhook] error: %r", e)

    # ----- receive callback -----

    def _on_receive(self, packet: Dict[str, Any], interface: Any) -> None:
        """
        Meshtastic pubsub callback for received packets.

        We do minimal work here:
        - normalize from_id and timestamp
        - record station + (optional) name into DB for *all* packets
        - if it's a text message, enqueue for the main bot loop
        """
        node_key = self._node_key_for_interface(interface)
        ts = _packet_ts(packet)
        from_id = _derive_from_id(packet)

        # Record station + name history for ANY packet (not only text)
        short, long = _extract_names_from_packet(packet)
        if (not short) and (not long) and from_id:
            s2, l2 = _extract_names_from_interface(interface, from_id, _derive_from_num(packet))
            short = short or s2
            long = long or l2

        if from_id:
            try:
                self.db.touch_station(from_id, ts)
                if short is not None or long is not None:
                    self.db.record_name(from_id, ts, short, long)
            except Exception as e:
                # Do not crash on DB errors; log at DEBUG.
                self.log.debug("DB station/name record failed for %s: %r", from_id, e)

        # Plugins receive every packet (text and non-text)
        try:
            self.plugins.on_packet(packet, interface, node_key)
        except Exception:
            pass

        # Only enqueue text packets for command processing
        if not _is_text_packet(packet):
            return

        if self.log.isEnabledFor(TRACE):
            self.log.trace("[%s] RX packet=%s", node_key,
                           json.dumps(make_jsonable(packet), ensure_ascii=False, default=json_default))

        self._q.put(Incoming(packet=packet, node_key=node_key, iface=interface,
                             ts=ts, from_id=from_id, short=short, long=long))

    # ----- scheduler loop -----

    def _scheduler_loop(self) -> None:
        """Tick scheduler once per second."""
        self.log.info("Scheduler loop running.")
        while not self._stop.is_set():
            try:
                self._scheduled_tick()
            except Exception as e:
                self.log.warning("Scheduler loop error: %r", e)
            time.sleep(1)

    def _scheduled_tick(self) -> None:
        """
        Run one scheduler tick.

        Scheduler design:
        - Ticks once per second.
        - Compares current local HH:MM to each schedule item.
        - Uses a per-minute guard so an item fires only once per minute.
        """
        sched = self.cfg.schedules
        if not sched or not getattr(sched, "enabled", True):
            return

        items = getattr(sched, "items", []) or []
        if not items:
            return

        tz_name = getattr(sched, "timezone", "Europe/Berlin") or "Europe/Berlin"
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = None

        import datetime as _dt
        dt = _dt.datetime.now(tz) if tz else _dt.datetime.now()
        weekday = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][dt.weekday()]
        now_hhmm = dt.strftime("%H:%M")
        minute_bucket = int(dt.timestamp()) // 60

        for it in items:
            t = str(getattr(it, "time", "")).strip()
            if not t or t != now_hhmm:
                continue

            days = [str(d).strip().lower()[:3] for d in (getattr(it, "days", []) or [])]
            if days and weekday not in days:
                continue

            ch = int(getattr(it, "channel", 0))
            dest = str(getattr(it, "destination_id", "^all") or "^all")
            msg = str(getattr(it, "text", "") or "").strip()
            node_sel = str(getattr(it, "node", "") or "")
            if not msg:
                continue

            # Key that uniquely identifies this schedule item in the current minute.
            key = f"{t}|{weekday}|{ch}|{dest}|{node_sel}|{hash(msg)}"
            if self._sched_last_sent.get(key) == minute_bucket:
                continue

            client = self.get_client(node_sel or None)
            if not client:
                continue

            client.send_text(msg, destination_id=dest, channel_index=ch)
            self._sched_last_sent[key] = minute_bucket
            self.log.info("Scheduled message sent: time=%s ch=%s dest=%s", t, ch, dest)

    # ----- heartbeat -----

    def _heartbeat_tick(self) -> None:
        """
        Heartbeat sender (disabled by default).

        If enabled:
        - Every interval_seconds, send a heartbeat message.
        - mode='broadcast': send to ^all on heartbeat.channel
        - mode='dm': send DMs to heartbeat.targets
        """
        hb = self.cfg.heartbeat
        if not hb or not getattr(hb, "enabled", False):
            return

        interval = int(getattr(hb, "interval_seconds", 300) or 300)
        if interval < 1:
            interval = 1

        if (time.time() - self._last_hb) < interval:
            return

        msg = str(getattr(hb, "message", "") or "").strip() or "❤️ heartbeat"
        mode = str(getattr(hb, "mode", "broadcast") or "broadcast").lower()
        targets = list(getattr(hb, "targets", []) or [])
        hb_channel = int(getattr(hb, "channel", 0) or 0)

        for client in self.clients.values():
            if not client.connected.is_set():
                continue

            if mode == "dm" and targets:
                for t in targets:
                    client.send_text(msg, destination_id=str(t), channel_index=0)
            else:
                client.send_text(msg, destination_id="^all", channel_index=hb_channel)

        self._last_hb = time.time()


    def send_reply(self, text: str, destination_id: str, channel_index: int, node_hint: Optional[str] = None) -> bool:
        """Send a reply using the standard message-splitting rules.

        Plugins should use this method (or api_send_channel/api_send_dm) instead of calling
        Meshtastic `sendText()` directly.
        """
        client = self.clients.get(node_hint or "") or self.get_client(None)
        if not client:
            return False
        ok = client.send_text(text, destination_id=destination_id, channel_index=int(channel_index))
        try:
            self.plugins.on_reply(text, {"destination_id": destination_id, "channel": int(channel_index), "node_key": getattr(client, "key", None)})
        except Exception:
            pass
        return ok

    # ----- help & custom commands -----

    def _make_help(self) -> str:
        """Generate /help output."""
        lines = [
            "⚠️ Help (v"+BOTVERSION+")",
            "/ping",
            "/user",
            "/user <!idxxx>",
            "/stats",
        ]
        if self.cfg.commands.list:
            for c in self.cfg.commands.list:
                lines.append(f"{c.trigger}")
        return "\n".join(lines)

    def _handle_custom_command(self, text: str, from_id: str, channel_index: int, node_key: str) -> Optional[str]:
        """
        Try to match and render a custom command.

        Response supports simple .format() placeholders:
            {text}, {fromId}, {channel}, {channelName}, {node}
        """
        for c in self.cfg.commands.list:
            trig = c.trigger.strip()
            if trig and text.startswith(trig):
                return c.response.format(
                    text=text,
                    fromId=from_id,
                    channel=channel_index,
                    channelName=self.channel_name(channel_index),
                    node=self.node_name_by_key.get(node_key, node_key),
                )
        return None

    # ----- main bot loop -----

    def _loop(self) -> None:
        """
        Main bot loop.

        - Sends heartbeat (if enabled)
        - Dequeues incoming messages
        - Stores RX history in SQLite
        - Forwards to webhook (if configured)
        - Processes commands and sends replies (channel/DM)
        """
        self.log.info("Bot loop running.")

        # Defensive check: if this triggers, the container is running a stale bot.py
        if not hasattr(self, "_loop"):
            raise RuntimeError("MeshBot is missing _loop(); rebuild the image without cache or ensure /app/app/bot.py is updated")

        while not self._stop.is_set():
            # Heartbeat tick is quick; errors are ignored
            try:
                self._heartbeat_tick()
            except Exception:
                pass

            # Plugins periodic hook
            try:
                self.plugins.on_tick(int(time.time()))
            except Exception:
                pass

            # Wait for next incoming message (timeout so loop can exit)
            try:
                inc = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            packet = inc.packet
            node_key = inc.node_key
            text = _get_text(packet)
            if not text:
                continue

            channel_index = _channel_index(packet)
            from_id = inc.from_id
            is_dm = not _is_broadcast(packet)

            self.rx_messages += 1

            # Persist RX message (for history API) for both channel and DM conversations
            conv_type, ch, peer = self._conversation_for_incoming(packet, from_id, channel_index)
            self.db.add_message(
                ts=int(inc.ts),
                conversation_type=conv_type,
                channel=ch,
                channel_name=self.channel_name(ch or channel_index) if conv_type == "channel" else "DM",
                peer_id=peer,
                user_id=from_id or None,
                name_short=inc.short,
                name_long=inc.long,
                node_key=node_key,
                direction="rx",
                message=text,
                keep=int(self.cfg.db.keep_per_conversation),
            )

            self.log.debug("[%s] RX from=%s ch=%s text=%r", node_key, from_id, channel_index, text)

            # Forward to webhook (if configured)
            self._post_webhook(packet, node_key, channel_index, text)

            # Plugins receive text events (after DB + webhook)
            try:
                self.plugins.on_text(text, {
                    "from_id": from_id,
                    "channel": channel_index,
                    "channel_name": self.channel_name(channel_index),
                    "is_dm": is_dm,
                    "node_key": node_key,
                    "short": inc.short,
                    "long": inc.long,
                    "packet": packet,
                    "reply_dest": _reply_dest(packet, from_id),
                })
            except Exception:
                pass

            # If bot.channels is configured, only react to commands in those channels.
            # DMs are always allowed.
            if (not is_dm) and self.cfg.bot.channels and (channel_index not in set(self.cfg.bot.channels)):
                continue

            # Detect command key (built-in or custom)
            cmd_key = self._matched_command_key(text)

            # Apply command blocking rules
            if cmd_key and self._is_blocked(cmd_key, channel_index, is_dm):
                continue
            # Plugins can handle commands before the bot.
            if cmd_key:
                meta = {
                    "from_id": from_id,
                    "channel": channel_index,
                    "channel_name": self.channel_name(channel_index),
                    "is_dm": is_dm,
                    "node_key": node_key,
                    "short": inc.short,
                    "long": inc.long,
                    "packet": packet,
                    "reply_dest": _reply_dest(packet, from_id),
                }
                try:
                    if self.plugins.on_command(cmd_key, text, meta):
                        continue
                except Exception:
                    pass


            # Strict mode: ignore all non-commands
            if self.cfg.bot.strict and (cmd_key is None):
                continue

            # Choose the node connection that received the message (fallback to any connected client)
            client = self.clients.get(node_key) or self.get_client(None)
            if not client:
                continue

            # Reply destination: '^all' if channel message, else from_id for DMs
            dest = _reply_dest(packet, from_id)

            lower = text.lower()
            
            if self.cfg.bot.commands_enabled:
                # ----- built-in commands -----
    
                if lower.startswith("/help"):
                    client.send_text(self._make_help(), destination_id=dest, channel_index=channel_index)
                    continue
    
                if lower.startswith("/ping"):
                    # Resolve short name (packet -> interface.nodes -> DB fallback)
                    short = (inc.short or "").strip()
                    if not short and from_id:
                        s_db, _ = self.db.get_latest_name(from_id)
                        short = (s_db or "").strip()
                    if not short:
                        short = from_id or "unknown"
    
                    reply = f"Pong {short} | hops={_hops(packet)} | via={_via(packet)}"
                    client.send_text(reply, destination_id=dest, channel_index=channel_index)
                    continue
    
                if lower.startswith("/stats"):
                    s = self.get_stats()
                    out = "\n".join([
                        f"Uptime: {s.get('uptime_human')}",
                        f"DB users: {s.get('db_users', 0)} (name rows: {s.get('db_name_rows', 0)})",
                        f"RX msgs: {s.get('rx_messages', 0)} | TX msgs: {s.get('tx_messages', 0)}",
                    ])
                    client.send_text(out, destination_id=dest, channel_index=channel_index)
                    continue
    
                if lower.startswith("/user"):
                    # /user with no arg -> show info about sender
                    parts = text.split()
                    target = parts[1].strip() if len(parts) >= 2 else from_id
    
                    info = self.get_user_info(target, name_limit=20, name_order="desc")
                    if not info:
                        client.send_text(f"No data for {target}", destination_id=dest, channel_index=channel_index)
                        continue
    
                    def fmt_ts(tsv: int) -> str:
                        # We intentionally use UTC in responses for consistent interpretation.
                        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(tsv)))
    
                    hist = info.get("name_history", [])
                    lines = [
                        f"ID: {target}",
                        f"First: {fmt_ts(info['first_seen'])}",
                        f"Names: {max(0, int(info.get('name_entries', 0)))}",
                        "History:",
                    ]
                    if not hist:
                        lines.append("- (no names recorded yet)")
                    else:
                        for h in hist:
                            lines.append(
                                f"- {fmt_ts(h['seen_at'])} | ({h.get('short') or '-'}) {h.get('long') or '-'}"
                            )
    
                    client.send_text("\n".join(lines), destination_id=dest, channel_index=channel_index)
                    continue
    
                # ----- custom commands -----
    
                resp = self._handle_custom_command(text, from_id, channel_index, node_key)
                if resp:
                    client.send_text(resp, destination_id=dest, channel_index=channel_index)
