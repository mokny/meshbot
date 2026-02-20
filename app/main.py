"""
Entry point for the MeshBot.
by Till Vennefrohne - https://github.com/mokny/meshbot

What this module does
---------------------
1) Determine the config file path (CONFIG_PATH env var, default: /config/config.toml)
2) Create a default config file if it does not exist yet
3) Load config, set up logging (incl. TRACE)
4) Start MeshBot (node connections + bot loop + scheduler)
5) Start FastAPI/uvicorn in a background thread

The container stays alive by keeping the main thread in an infinite sleep loop.
"""

from __future__ import annotations

import os
import logging
import threading
import time

import uvicorn

from .config import load_config, save_config, AppCfg
from .bot import MeshBot, TRACE, install_trace_level
from .api import create_app


def _setup_logging(level: str) -> None:
    """
    Configure logging for the whole process.

    TRACE is a custom level below DEBUG. All other levels use standard logging levels.
    """
    install_trace_level()
    lvl = (level or "INFO").upper()
    if lvl == "TRACE":
        logging.basicConfig(level=TRACE, format="%(asctime)s %(levelname)s %(message)s")
    else:
        logging.basicConfig(level=getattr(logging, lvl, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def ensure_default_config(path: str) -> None:
    """
    Create a default config file on first start.

    This makes the Docker container "bootable" even if the user did not create config.toml yet.
    The user should then edit ./config/config.toml on the host.
    """
    if os.path.exists(path):
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cfg = AppCfg()
    cfg.api.tokens = ["CHANGE_ME_TOKEN"]
    save_config(path, cfg)


def main() -> None:
    config_path = os.environ.get("CONFIG_PATH", "/config/config.toml")
    ensure_default_config(config_path)

    cfg = load_config(config_path)

    _setup_logging(cfg.logging.level)
    log = logging.getLogger("meshtastic-bot")

    if not cfg.nodes:
        log.error("No nodes configured. Set [nodes].list in config.toml")
        return

    # Start the bot (connections + threads)
    bot = MeshBot(cfg=cfg, config_path=config_path, logger=log)
    # --- diagnostics: confirm which bot.py is running inside the container ---
    try:
        import app.bot as _b
        log.info("bot module file: %s", getattr(_b, "__file__", "?"))
        log.info("MeshBot has _loop=%s start=%s", hasattr(_b.MeshBot, "_loop"), hasattr(_b.MeshBot, "start"))
    except Exception as _e:
        log.warning("bot diagnostics failed: %r", _e)
    bot.start()

    # Create and start the API server in a separate thread
    app = create_app(bot)
    host = cfg.api.listen_host or "0.0.0.0"
    port = int(cfg.api.listen_port or 8080)
    log.info("Starting API on http://%s:%s", host, port)

    # Run Uvicorn in a background thread.
    # Docker keeps the container alive via the main thread sleep loop.
    def run_api() -> None:
        uvicorn.run(app, host=host, port=port, log_level="info")

    threading.Thread(target=run_api, name="api", daemon=True).start()

    # Keep container running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        bot.stop()


if __name__ == "__main__":
    main()
