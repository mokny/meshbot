"""
Configuration loader/saver for the MeshBot.
by Till Vennefrohne - https://github.com/mokny/meshbot

Design goals
-----------
1) Everything is configured via a persistent TOML file (mounted into /config).
2) The loader is defensive: missing keys and wrong types should not crash the bot.
3) Channels can be referenced by either:
   - numeric index (e.g. 7)
   - friendly name (e.g. "Trusted") if configured in [bot].channel_names

Important sections in config.toml
--------------------------------
[nodes].list
    One or more Meshtastic nodes to connect to via WiFi/TCP.
[bot]
    Bot behaviour, allow-lists, channel naming, command blocking.
[commands].list
    Custom commands defined by the user (trigger -> response template).
[api]
    HTTP API host/port and authentication tokens.
[db]
    SQLite file path and how many messages per conversation to keep.
[webhook]
    Optional URL that receives every incoming text message (config only).
[heartbeat]
    Optional periodic message sender (default OFF).
[schedules]
    Fixed-time scheduled messages (timezone aware).
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

import tomli_w


# ---------- basic safe converters (defensive parsing) ----------

def _as_str(v: Any, default: str = "") -> str:
    """Convert any value to string (or default if None)."""
    if v is None:
        return default
    return str(v)


def _int(v: Any, default: int = 0) -> int:
    """Convert to int. Strings may be base-10 or base-16 (0x...)."""
    try:
        if isinstance(v, str):
            return int(v.strip(), 0)
        return int(v)
    except Exception:
        return default


def _bool(v: Any, default: bool = False) -> bool:
    """Convert to bool with common string/int representations."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off"):
            return False
    if isinstance(v, (int, float)):
        return bool(v)
    return default


def _str_list(v: Any) -> List[str]:
    """Convert list-like config values to a list[str]."""
    if not v:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    return []


def _int_str_dict(v: Any) -> Dict[int, str]:
    """Parse mapping {channel_index: channel_name}."""
    out: Dict[int, str] = {}
    if not v or not isinstance(v, dict):
        return out
    for k, val in v.items():
        try:
            ki = int(str(k), 0)
        except Exception:
            continue
        out[int(ki)] = str(val)
    return out


# ---------- helpers for channel lists and command blocking ----------

def _resolve_channels(raw_list: Any, channel_names: Dict[int, str]) -> List[int]:
    """
    Resolve a mixed list of channel indices and channel names into indices.

    Example:
        channel_names = {7: "Trusted"}
        raw_list = [7, "Trusted"]
        -> [7]
    """
    if not raw_list or not isinstance(raw_list, list):
        return []
    inv: Dict[str, int] = {str(v).strip().lower(): int(k) for k, v in (channel_names or {}).items()}
    out: List[int] = []
    for x in raw_list:
        if isinstance(x, (int, float)):
            out.append(int(x))
            continue
        if isinstance(x, str):
            s = x.strip()
            if not s:
                continue
            # numeric string?
            if re.fullmatch(r"0x[0-9a-fA-F]+|\d+", s):
                try:
                    out.append(int(s, 0))
                except Exception:
                    continue
            else:
                idx = inv.get(s.lower())
                if idx is not None:
                    out.append(int(idx))
    # de-dup preserve order
    seen = set()
    dedup: List[int] = []
    for n in out:
        if n not in seen:
            seen.add(n)
            dedup.append(n)
    return dedup


def _parse_command_blocks(raw_blocks: Any, channel_names: Dict[int, str]) -> Tuple[Dict[str, List[int]], List[str]]:
    """
    Parse [bot.command_blocks] allowing:

    - channel indices (numbers)
    - channel names (strings matching bot.channel_names values)
    - the string "dm" to block the command in DMs

    Returns:
      (channel_blocks, dm_blocks)
    """
    blocks: Dict[str, List[int]] = {}
    dm_blocks: List[str] = []
    if not raw_blocks or not isinstance(raw_blocks, dict):
        return blocks, dm_blocks

    inv: Dict[str, int] = {str(v).strip().lower(): int(k) for k, v in (channel_names or {}).items()}

    for cmd, v in raw_blocks.items():
        key = str(cmd).strip().lower()
        if not key:
            continue
        chs: List[int] = []
        if isinstance(v, list):
            for x in v:
                if isinstance(x, str):
                    s = x.strip()
                    if not s:
                        continue
                    if s.lower() == "dm":
                        if key not in dm_blocks:
                            dm_blocks.append(key)
                        continue
                    if re.fullmatch(r"0x[0-9a-fA-F]+|\d+", s):
                        try:
                            chs.append(int(s, 0))
                        except Exception:
                            continue
                    else:
                        idx = inv.get(s.lower())
                        if idx is not None:
                            chs.append(int(idx))
                elif isinstance(x, (int, float)):
                    chs.append(int(x))

        # de-dup
        seen = set()
        dedup: List[int] = []
        for n in chs:
            if n not in seen:
                seen.add(n)
                dedup.append(n)
        if dedup:
            blocks[key] = dedup

    return blocks, dm_blocks


# ---------- config dataclasses (typed config model) ----------

@dataclass
class NodeCfg:
    """One Meshtastic node reachable via WiFi TCP (hostname:portNumber)."""
    host: str
    port: int = 4403
    name: str = ""


@dataclass
class WebhookCfg:
    """Webhook forwarding settings (config only)."""
    url: str = ""
    timeout_seconds: int = 5


@dataclass
class HeartbeatCfg:
    """
    Heartbeat settings.

    Heartbeat sends a repeating message at a fixed interval.
    It is disabled by default (enabled=false).
    """
    enabled: bool = False
    interval_seconds: int = 300
    message: str = "❤️ heartbeat"
    # broadcast|dm
    mode: str = "broadcast"
    # DM recipients (only used if mode == "dm")
    targets: List[str] = field(default_factory=list)
    # Broadcast channel index (only used if mode == "broadcast")
    channel: int = 0


@dataclass
class ScheduleItemCfg:
    """One scheduled message item (fixed HH:MM, optional weekdays)."""
    time: str  # HH:MM
    channel: int = 0
    destination_id: str = "^all"
    text: str = ""
    days: List[str] = field(default_factory=list)  # ["mon",..] empty => daily
    node: str = ""  # optional node selector (name or host:port)


@dataclass
class SchedulesCfg:
    """Scheduler settings."""
    enabled: bool = True
    timezone: str = "Europe/Berlin"
    items: List[ScheduleItemCfg] = field(default_factory=list)


@dataclass
class LoggingCfg:
    """Logging settings. TRACE is the most verbose level."""
    level: str = "INFO"  # ERROR|WARNING|INFO|DEBUG|TRACE


@dataclass
class ApiCfg:
    """HTTP API settings."""
    listen_host: str = "0.0.0.0"
    listen_port: int = 8080
    tokens: List[str] = field(default_factory=list)


@dataclass
class PluginsCfg:
    """Plugin loader settings."""
    enabled: bool = True
    path: str = "/plugins"

@dataclass
class DbCfg:

    """SQLite DB settings."""
    path: str = "/config/bot.db"
    keep_per_conversation: int = 10000


@dataclass
class CommandCfg:
    """One custom command: trigger -> response template."""
    trigger: str
    response: str


@dataclass
class CommandsCfg:
    """Container for custom command list."""
    list: List[CommandCfg] = field(default_factory=list)


@dataclass
class BotCfg:
    """Bot settings: naming, channel allow-lists, and command blocking."""
    name: str = "meshbot"
    # If set, bot will only respond to commands in these channel indices (channel chat). DMs still work.
    channels: List[int] = field(default_factory=list)
    # Optional mapping of channel index -> display name
    channel_names: Dict[int, str] = field(default_factory=dict)
    # strict = true -> only react to commands (ignore other text)
    strict: bool = False
    commands_enabled: bool = False
    # command blocks
    command_blocks: Dict[str, List[int]] = field(default_factory=dict)
    command_blocks_dm: List[str] = field(default_factory=list)


@dataclass
class AppCfg:
    """Full app config."""
    nodes: List[NodeCfg] = field(default_factory=list)
    bot: BotCfg = field(default_factory=BotCfg)
    webhook: WebhookCfg = field(default_factory=WebhookCfg)
    heartbeat: HeartbeatCfg = field(default_factory=HeartbeatCfg)
    schedules: SchedulesCfg = field(default_factory=SchedulesCfg)
    logging: LoggingCfg = field(default_factory=LoggingCfg)
    api: ApiCfg = field(default_factory=ApiCfg)
    plugins: PluginsCfg = field(default_factory=PluginsCfg)
    db: DbCfg = field(default_factory=DbCfg)
    commands: CommandsCfg = field(default_factory=CommandsCfg)


# ---------- load/save functions ----------

def load_config(path: str) -> AppCfg:
    """
    Load TOML config from path.

    If the file does not exist, returns a config with default values.
    """
    p = pathlib.Path(path)
    raw: Dict[str, Any] = {}
    if p.exists():
        raw = tomllib.loads(p.read_text(encoding="utf-8"))

    # nodes
    nodes_container: Any = raw.get("nodes", {}) or {}
    nodes_raw = nodes_container.get("list", []) if isinstance(nodes_container, dict) else nodes_container
    nodes: List[NodeCfg] = []
    if isinstance(nodes_raw, list):
        for n in nodes_raw:
            if not isinstance(n, dict):
                continue
            host = _as_str(n.get("host")).strip()
            if not host:
                continue
            nodes.append(NodeCfg(host=host, port=_int(n.get("port"), 4403), name=_as_str(n.get("name"), "")))

    # bot
    bot_raw = raw.get("bot", {}) or {}
    channel_names = _int_str_dict(bot_raw.get("channel_names"))
    blocks_map, dm_blocks = _parse_command_blocks(bot_raw.get("command_blocks"), channel_names)
    bot = BotCfg(
        name=_as_str(bot_raw.get("name"), "meshbot"),
        channels=_resolve_channels(bot_raw.get("channels"), channel_names),
        channel_names=channel_names,
        strict=_bool(bot_raw.get("strict"), False),
        commands_enabled=_bool(bot_raw.get("commands_enabled"), False),
        command_blocks=blocks_map,
        command_blocks_dm=dm_blocks,
    )

    # webhook (config-only; no chat commands)
    webhook_raw = raw.get("webhook", {}) or {}
    webhook = WebhookCfg(
        url=_as_str(webhook_raw.get("url"), ""),
        timeout_seconds=_int(webhook_raw.get("timeout_seconds"), 5),
    )

    # heartbeat (default off)
    hb_raw = raw.get("heartbeat", {}) or {}
    heartbeat = HeartbeatCfg(
        enabled=_bool(hb_raw.get("enabled"), False),
        interval_seconds=_int(hb_raw.get("interval_seconds"), 300),
        message=_as_str(hb_raw.get("message"), "❤️ heartbeat"),
        mode=_as_str(hb_raw.get("mode"), "broadcast"),
        targets=_str_list(hb_raw.get("targets")),
        channel=_int(hb_raw.get("channel"), 0),
    )

    # schedules
    sch_raw = raw.get("schedules", {}) or {}
    schedules = SchedulesCfg(
        enabled=_bool(sch_raw.get("enabled"), True) if isinstance(sch_raw, dict) else True,
        timezone=_as_str(sch_raw.get("timezone"), "Europe/Berlin") if isinstance(sch_raw, dict) else "Europe/Berlin",
        items=[],
    )
    items_raw = sch_raw.get("items", []) if isinstance(sch_raw, dict) else []
    if isinstance(items_raw, list):
        for it in items_raw:
            if not isinstance(it, dict):
                continue
            t = _as_str(it.get("time")).strip()
            msg = _as_str(it.get("text")).strip()
            if not t or not msg:
                continue
            schedules.items.append(
                ScheduleItemCfg(
                    time=t,
                    channel=_int(it.get("channel"), 0),
                    destination_id=_as_str(it.get("destination_id"), "^all"),
                    text=msg,
                    days=[d.strip().lower()[:3] for d in _str_list(it.get("days"))],
                    node=_as_str(it.get("node"), ""),
                )
            )

    # logging
    log_raw = raw.get("logging", {}) or {}
    logging_cfg = LoggingCfg(level=_as_str(log_raw.get("level"), "INFO"))

    # API
    api_raw = raw.get("api", {}) or {}
    api = ApiCfg(
        listen_host=_as_str(api_raw.get("listen_host"), "0.0.0.0"),
        listen_port=_int(api_raw.get("listen_port"), 8080),
        tokens=_str_list(api_raw.get("tokens")),
    )

    # plugins
    plugins_raw = raw.get("plugins", {}) or {}
    plugins = PluginsCfg(
        enabled=_bool(plugins_raw.get("enabled"), True),
        path=_as_str(plugins_raw.get("path"), "/plugins"),
    )

    # DB
    db_raw = raw.get("db", {}) or {}
    db = DbCfg(
        path=_as_str(db_raw.get("path"), "/config/bot.db"),
        keep_per_conversation=_int(db_raw.get("keep_per_conversation"), 10000),
    )

    # custom commands
    commands_raw = raw.get("commands", {}) or {}
    cmd_list_raw = commands_raw.get("list", []) if isinstance(commands_raw, dict) else []
    cmd_list: List[CommandCfg] = []
    if isinstance(cmd_list_raw, list):
        for c in cmd_list_raw:
            if not isinstance(c, dict):
                continue
            trig = _as_str(c.get("trigger")).strip()
            resp = _as_str(c.get("response"))
            if trig and resp:
                cmd_list.append(CommandCfg(trigger=trig, response=resp))
    commands = CommandsCfg(list=cmd_list)

    return AppCfg(
        nodes=nodes,
        bot=bot,
        webhook=webhook,
        heartbeat=heartbeat,
        schedules=schedules,
        logging=logging_cfg,
        api=api,
        plugins=plugins,
        db=db,
        commands=commands,
    )


def save_config(path: str, cfg: AppCfg) -> None:
    """
    Persist config back to TOML.

    This is primarily used to create a default config file on first start.
    """
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data: Dict[str, Any] = {
        "nodes": {"list": [{"host": n.host, "port": n.port, "name": n.name} for n in cfg.nodes]},
        "bot": {
            "name": cfg.bot.name,
            "channels": cfg.bot.channels,
            "channel_names": {str(k): v for k, v in (cfg.bot.channel_names or {}).items()},
            "strict": cfg.bot.strict,
            "commands_enabled": cfg.bot.commands_enabled,
            "command_blocks": {k: v for k, v in (cfg.bot.command_blocks or {}).items()},
        },
        "webhook": {"url": cfg.webhook.url, "timeout_seconds": cfg.webhook.timeout_seconds},
        "heartbeat": {
            "enabled": cfg.heartbeat.enabled,
            "interval_seconds": cfg.heartbeat.interval_seconds,
            "message": cfg.heartbeat.message,
            "mode": cfg.heartbeat.mode,
            "targets": cfg.heartbeat.targets,
            "channel": cfg.heartbeat.channel,
        },
        "schedules": {
            "enabled": cfg.schedules.enabled,
            "timezone": cfg.schedules.timezone,
            "items": [
                {"time": s.time, "channel": s.channel, "destination_id": s.destination_id, "text": s.text, "days": s.days, "node": s.node}
                for s in cfg.schedules.items
            ],
        },
        "logging": {"level": cfg.logging.level},
        "api": {"listen_host": cfg.api.listen_host, "listen_port": cfg.api.listen_port, "tokens": cfg.api.tokens},
        "db": {"path": cfg.db.path, "keep_per_conversation": cfg.db.keep_per_conversation},
        "commands": {"list": [{"trigger": c.trigger, "response": c.response} for c in cfg.commands.list]},
    }
    p.write_text(tomli_w.dumps(data), encoding="utf-8")
