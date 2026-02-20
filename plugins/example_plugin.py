"""Example plugin for the MeshBot.

This file acts as a **template** for developers.

Place plugin files into `./plugins` (mounted to `/plugins`).
They will be loaded in alphabetical order at bot startup.

Entry point:
- define `setup(bot)` and return a plugin instance.

Callbacks (all optional):
- on_start()
- on_stop()
- on_tick(now_ts: int)
- on_packet(packet: dict, interface: Any, node_key: str)
- on_text(text: str, meta: dict)
- on_command(cmd_key: str, text: str, meta: dict) -> Optional[bool]
  Return True to mark as handled (skip default handling).
- on_reply(text: str, meta: dict)

The plugin receives the `bot` object, so it can call:
- bot.send_reply(...)
- bot.api_send_channel(...)
- bot.api_send_dm(...)
- bot.db, bot.cfg, bot.log
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional


class ExamplePlugin:
    def __init__(self, bot: Any) -> None:
        self.bot = bot
        self._last_hello = 0.0

    def on_start(self) -> None:
        self.bot.log.info("[example_plugin] ready")

    def on_packet(self, packet: Dict[str, Any], interface: Any, node_key: str) -> None:
        # Called for every incoming packet (including non-text).
        if self.bot.log.isEnabledFor(5):  # TRACE level in this project
            self.bot.log.trace("[example_plugin] RX packet from=%s node=%s", packet.get("fromId"), node_key)

    def on_text(self, text: str, meta: Dict[str, Any]) -> None:
        # Example: greet the channel at most once every 10 minutes.
        if meta.get("is_dm"):
            return
        now = time.time()
        if now - self._last_hello < 600:
            return
        self._last_hello = now
        ch = int(meta.get("channel") or 0)
        #self.bot.api_send_channel(channel=ch, destination_id="^all", text="👋 Hello from example plugin!")

    def on_command(self, cmd_key: str, text: str, meta: Dict[str, Any]) -> Optional[bool]:
        # Example: add a plugin-only command
        if cmd_key == "/hi2":
            dest = meta.get("reply_dest") or "^all"
            ch = int(meta.get("channel") or 0)
            self.bot.send_reply("Hi from /hi2 (plugin)!", destination_id=dest, channel_index=ch, node_hint=meta.get("node_key"))
            return True
        return None

    def on_stop(self) -> None:
        self.bot.log.info("[example_plugin] stopped")


def setup(bot: Any) -> ExamplePlugin:
    return ExamplePlugin(bot)
