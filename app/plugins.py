"""
Plugin system for MeshBot.
by Till Vennefrohne - https://github.com/mokny/meshbot

Plugins are single Python files (*.py) placed into a configured directory (default: /plugins).
Files are loaded at startup in **alphabetical order** (case-insensitive).

A plugin module may provide one of these entry points:
- setup(bot) -> plugin instance (preferred)
- Plugin class -> instantiated as Plugin(bot)
- register(bot) -> plugin instance (alternative)

The plugin instance is "duck typed" and may implement any of these optional callbacks:
- on_start()
- on_stop()
- on_tick(now_ts: int)
- on_packet(packet: dict, interface: Any, node_key: str)
- on_text(text: str, meta: dict)
- on_command(cmd_key: str, text: str, meta: dict) -> Optional[bool]
  If any plugin returns True, default command handling is skipped.
- on_reply(text: str, meta: dict)

All callback invocations are protected by try/except so faulty plugins cannot crash the bot.
"""

from __future__ import annotations

import importlib.util
import pathlib
from types import ModuleType
from typing import Any, List, Optional


class PluginManager:
    def __init__(self, bot: Any, path: str, enabled: bool, log: Any) -> None:
        self.bot = bot
        self.path = path
        self.enabled = enabled
        self.log = log
        self.plugins: List[Any] = []
        self.modules: List[ModuleType] = []

    def load_all(self) -> None:
        self.plugins.clear()
        self.modules.clear()

        if not self.enabled:
            self.log.info("Plugins disabled.")
            return

        folder = pathlib.Path(self.path)
        if not folder.exists() or not folder.is_dir():
            self.log.info("Plugin directory not found: %s", self.path)
            return

        files = sorted(
            [p for p in folder.iterdir() if p.is_file() and p.suffix == ".py" and not p.name.startswith("_")],
            key=lambda p: p.name.lower(),
        )
        self.log.info("Plugins: loading %d file(s) from %s", len(files), str(folder))

        for f in files:
            try:
                mod = self._load_module(f)
                self.modules.append(mod)
                inst = self._instantiate(mod)
                if inst is None:
                    self.log.warning("Plugin %s has no setup()/Plugin/register()", f.name)
                    continue
                self.plugins.append(inst)
                self.log.info("Plugins: loaded %s", f.name)
            except Exception as e:
                self.log.error("Plugins: failed to load %s: %r", f.name, e)

    def _load_module(self, filepath: pathlib.Path) -> ModuleType:
        name = f"bot_plugin_{filepath.stem}"
        spec = importlib.util.spec_from_file_location(name, str(filepath))
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot import plugin: {filepath}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
        return mod

    def _instantiate(self, mod: ModuleType) -> Optional[Any]:
        if hasattr(mod, "setup") and callable(getattr(mod, "setup")):
            return mod.setup(self.bot)  # type: ignore[attr-defined]
        if hasattr(mod, "Plugin"):
            cls = getattr(mod, "Plugin")
            if callable(cls):
                return cls(self.bot)
        if hasattr(mod, "register") and callable(getattr(mod, "register")):
            return mod.register(self.bot)  # type: ignore[attr-defined]
        return None

    def _call(self, plugin: Any, method: str, *args: Any, **kwargs: Any) -> Any:
        fn = getattr(plugin, method, None)
        if not callable(fn):
            return None
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            self.log.warning("Plugin error in %s.%s: %r", plugin.__class__.__name__, method, e)
            return None

    def on_start(self) -> None:
        for p in self.plugins:
            self._call(p, "on_start")

    def on_stop(self) -> None:
        for p in self.plugins:
            self._call(p, "on_stop")

    def on_tick(self, now_ts: int) -> None:
        for p in self.plugins:
            self._call(p, "on_tick", now_ts)

    def on_packet(self, packet: dict, interface: Any, node_key: str) -> None:
        for p in self.plugins:
            self._call(p, "on_packet", packet, interface, node_key)

    def on_text(self, text: str, meta: dict) -> None:
        for p in self.plugins:
            self._call(p, "on_text", text, meta)

    def on_reply(self, text: str, meta: dict) -> None:
        for p in self.plugins:
            self._call(p, "on_reply", text, meta)

    def on_command(self, cmd_key: str, text: str, meta: dict) -> bool:
        handled = False
        for p in self.plugins:
            res = self._call(p, "on_command", cmd_key, text, meta)
            if res is True:
                handled = True
        return handled
