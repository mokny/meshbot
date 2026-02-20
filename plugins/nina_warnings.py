"""
NINA warnings plugin (Warnung.bund.de) for MeshBot
https://github.com/mokny/meshbot by Till Vennefrohne
========================================================

This plugin polls current NINA warnings and posts them into a Meshtastic channel.

Requested behavior (latest)
---------------------------
- Startup warnings:
  - wait 20 seconds after bot start
  - then send startup warnings WITHOUT waiting for connection
  - single attempt (no retry). If sending fails, we log and drop the startup queue.

Posting format
--------------
Only this is posted:
    HH:MM <Kreis/Stadt-Name>: <Warn-Titel>

Configuration via environment variables
---------------------------------------
NINA_ENABLED=true|false              (default true)
NINA_INTERVAL_SECONDS=300            (default 300)
NINA_CHANNEL=7                       (default 7)
NINA_DESTINATION_ID=^all             (default ^all)
NINA_DB_PATH=/config/nina.sqlite     (default /config/nina.sqlite)
NINA_MAX_PER_TICK=5                  (default 5)

Targets (choose one mode)
-------------------------
NINA_ARS=...                         (comma separated 12-digit ARS; optional)
NINA_COUNTIES=...                    (comma separated full Kreis/Stadt names; optional)
NINA_STATE=NRW                       (default NRW; if neither ARS nor COUNTIES specified)

Optional source filters (best-effort)
-------------------------------------
NINA_INCLUDE_POLICE=false            (default false)
NINA_INCLUDE_DWD=true                (default true)
NINA_INCLUDE_MOWAS=true              (default true)

"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def _env_csv(name: str) -> List[str]:
    v = os.getenv(name)
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


def _http_get_json(url: str, timeout: int = 30) -> Any:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "meshtastic-bot-nina-plugin/2.4", "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def _compact(s: str, max_len: int = 1200) -> str:
    s = " ".join((s or "").replace("\n", " ").replace("\r", " ").split())
    return s[:max_len]


def _ts_to_local(ts: Any) -> str:
    try:
        if isinstance(ts, str):
            return ts.replace("T", " ")[:16]
        t = int(ts)
        if t > 10_000_000_000:
            t //= 1000
        return time.strftime("%Y-%m-%d %H:%M", time.localtime(t))
    except Exception:
        return ""


def _time_hhmm(ts: Any) -> str:
    s = _ts_to_local(ts)
    if s:
        m = re.search(r"(\d{2}:\d{2})", s)
        if m:
            return m.group(1)
    return time.strftime("%H:%M", time.localtime())


def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def _log_info(log: Any, msg: str, *args: Any) -> None:
    try:
        if log:
            log.info(msg, *args)
            return
    except Exception:
        pass
    try:
        print("INFO " + (msg % args if args else msg), flush=True)
    except Exception:
        pass


def _log_warning(log: Any, msg: str, *args: Any) -> None:
    try:
        if log:
            log.warning(msg, *args)
            return
    except Exception:
        pass
    try:
        print("WARN " + (msg % args if args else msg), flush=True)
    except Exception:
        pass


STATE_ALIASES: Dict[str, str] = {
    "nrw": "05", "nordrhein westfalen": "05", "nordrhein-westfalen": "05",
    "he": "06", "hessen": "06",
    "sh": "01", "schleswig holstein": "01", "schleswig-holstein": "01",
    "hh": "02", "hamburg": "02",
    "ni": "03", "niedersachsen": "03",
    "hb": "04", "bremen": "04",
    "rp": "07", "rheinland pfalz": "07", "rheinland-pfalz": "07",
    "bw": "08", "baden wuerttemberg": "08", "baden-wuerttemberg": "08", "baden württemberg": "08", "baden-württemberg": "08",
    "by": "09", "bayern": "09",
    "sl": "10", "saarland": "10",
    "be": "11", "berlin": "11",
    "bb": "12", "brandenburg": "12",
    "mv": "13", "mecklenburg vorpommern": "13", "mecklenburg-vorpommern": "13",
    "sn": "14", "sachsen": "14",
    "st": "15", "sachsen anhalt": "15", "sachsen-anhalt": "15",
    "th": "16", "thueringen": "16", "thüringen": "16",
}


# ---------------------------------------------------------------------
# SQLite stores
# ---------------------------------------------------------------------

class PostedStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS posted(identifier TEXT PRIMARY KEY, first_seen_ts INTEGER NOT NULL)"
        )
        self.conn.commit()

    def has(self, ident: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM posted WHERE identifier=? LIMIT 1", (ident,))
        return cur.fetchone() is not None

    def add(self, ident: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO posted(identifier, first_seen_ts) VALUES(?,?)",
            (ident, int(time.time())),
        )
        self.conn.commit()


class RegionsCache:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS regions_cache(name_norm TEXT PRIMARY KEY, display_name TEXT NOT NULL, ars TEXT NOT NULL)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS regions_cache_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self.conn.commit()

    def get_meta(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM regions_cache_meta WHERE key=? LIMIT 1", (key,))
        r = cur.fetchone()
        return r[0] if r else None

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO regions_cache_meta(key,value) VALUES(?,?)", (key, value))

    def count_rows(self) -> int:
        cur = self.conn.execute("SELECT COUNT(1) FROM regions_cache")
        return int(cur.fetchone()[0])

    def put(self, name_norm: str, display_name: str, ars: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO regions_cache(name_norm, display_name, ars) VALUES(?,?,?)",
            (name_norm, display_name, ars),
        )

    def get_ars(self, name_norm: str) -> Optional[str]:
        cur = self.conn.execute("SELECT ars FROM regions_cache WHERE name_norm=? LIMIT 1", (name_norm,))
        r = cur.fetchone()
        return r[0] if r else None

    def distinct_ars_by_state(self, state_code: str) -> List[str]:
        cur = self.conn.execute("SELECT DISTINCT ars FROM regions_cache WHERE ars LIKE ?", (f"{state_code}%",))
        out = []
        for (ars,) in cur.fetchall():
            if isinstance(ars, str) and re.fullmatch(r"\d{12}", ars):
                out.append(ars)
        return out


class ArsNameStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS ars_names(ars TEXT PRIMARY KEY, name TEXT NOT NULL, source TEXT NOT NULL)"
        )
        self.conn.commit()

    def get_name(self, ars: str) -> Optional[str]:
        cur = self.conn.execute("SELECT name FROM ars_names WHERE ars=? LIMIT 1", (ars,))
        r = cur.fetchone()
        return r[0] if r else None

    def put(self, ars: str, name: str, source: str) -> None:
        # Prefer kreis-level sources over fallbacks.
        cur = self.conn.execute("SELECT source FROM ars_names WHERE ars=? LIMIT 1", (ars,))
        r = cur.fetchone()
        if r:
            existing = str(r[0] or "")
            if existing == "kreis":
                return
            if source != "kreis":
                return
        self.conn.execute("INSERT OR REPLACE INTO ars_names(ars,name,source) VALUES(?,?,?)", (ars, name, source))


# ---------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------

class NinaWarningsPlugin:
    DASHBOARD_URL = "https://warnung.bund.de/api31/dashboard/{ars}.json"
    DESTATS_RS_JSON_URL = (
        "https://www.xrepository.de/api/xrepository/"
        "urn:de:bund:destatis:bevoelkerungsstatistik:schluessel:rs_2021-07-31/"
        "download/Regionalschl_ssel_2021-07-31.json"
    )

    def __init__(self, bot: Any) -> None:
        self.bot = bot
        self.log = getattr(bot, "log", None) or logging.getLogger("nina")

        self.enabled = _env_bool("NINA_ENABLED", True)
        self.interval_seconds = max(30, _env_int("NINA_INTERVAL_SECONDS", 300))
        self.channel = _env_int("NINA_CHANNEL", 7)
        self.destination_id = _env_str("NINA_DESTINATION_ID", "^all")
        self.db_path = _env_str("NINA_DB_PATH", "/config/nina.sqlite")
        self.max_per_tick = max(1, _env_int("NINA_MAX_PER_TICK", 5))

        self.include_police = _env_bool("NINA_INCLUDE_POLICE", False)
        self.include_dwd = _env_bool("NINA_INCLUDE_DWD", True)
        self.include_mowas = _env_bool("NINA_INCLUDE_MOWAS", True)

        self.ars_list = _env_csv("NINA_ARS")
        self.counties = _env_csv("NINA_COUNTIES")
        self.state = _env_str("NINA_STATE", "NRW")

        self._conn: Optional[sqlite3.Connection] = None
        self._posted: Optional[PostedStore] = None
        self._regions: Optional[RegionsCache] = None
        self._ars_names: Optional[ArsNameStore] = None

        self._resolved_ars: List[str] = []
        self._next_poll = 0.0

        self._startup_pending: List[Tuple[str, str]] = []
        self._startup_delay_seconds = 20
        self._stop_flag = False
        self._startup_thread: Optional[threading.Thread] = None

    # ---------------- lifecycle ----------------

    def on_start(self) -> None:
        _log_info(self.log, "[nina] on_start() called")
        if not self.enabled:
            _log_info(self.log, "[nina] disabled via NINA_ENABLED=false")
            return

        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._posted = PostedStore(self._conn)
        self._regions = RegionsCache(self._conn)
        self._ars_names = ArsNameStore(self._conn)

        self._ensure_region_cache()

        self._resolved_ars = self._resolve_targets()
        _log_info(self.log, "[nina] resolved %d ARS target(s)", len(self._resolved_ars))

        if self._resolved_ars:
            self._startup_snapshot_and_queue()

        _log_info(self.log, "[nina] startup: pending to post=%d (delay=%ss, no-wait, single-attempt)", len(self._startup_pending), self._startup_delay_seconds)

        # Start one-shot delayed sender thread.
        if self._startup_thread is None:
            self._startup_thread = threading.Thread(target=self._startup_sender_once, name="nina-startup-once", daemon=True)
            self._startup_thread.start()

        _log_info(self.log, "[nina] ready. targets=%d ars (channel=%s, interval=%ss)", len(self._resolved_ars), self.channel, self.interval_seconds)
        self._next_poll = time.time() + 5

    def on_stop(self) -> None:
        self._stop_flag = True
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    def on_tick(self, now_ts: int) -> None:
        if not self.enabled:
            return

        # Periodic polling
        now = time.time()
        if now < self._next_poll:
            return
        self._next_poll = now + self.interval_seconds

        if not self._resolved_ars or not self._posted:
            return

        try:
            sent = 0
            for ars in self._resolved_ars:
                for it in self._fetch_items(ars):
                    ident = self._identifier_for(it, ars)
                    if not ident or self._posted.has(ident):
                        continue

                    msg = self._format_warning(it, ars)
                    if not msg:
                        continue

                    self.bot.api_send_channel(channel=int(self.channel), destination_id=self.destination_id, text=msg)
                    self._posted.add(ident)
                    sent += 1
                    if sent >= self.max_per_tick:
                        return
        except Exception as e:
            _log_warning(self.log, "[nina] polling failed: %r", e)

    # ---------------- startup sender (single attempt, no wait) ----------------

    def _startup_sender_once(self) -> None:
        """Wait fixed delay, then attempt to send startup warnings once (no retries)."""
        try:
            if self._startup_delay_seconds > 0:
                _log_info(self.log, "[nina] startup sender: sleeping %ss before sending startup warnings", self._startup_delay_seconds)
                time.sleep(self._startup_delay_seconds)

            if self._stop_flag or (not self.enabled):
                return

            if not self._startup_pending:
                _log_info(self.log, "[nina] startup sender: nothing queued.")
                return

            if not hasattr(self.bot, "api_send_channel"):
                _log_warning(self.log, "[nina] bot has no api_send_channel(); cannot send startup warnings (dropping %d).", len(self._startup_pending))
                self._startup_pending.clear()
                return

            _log_info(self.log, "[nina] startup sender: sending %d startup warning(s) (single attempt, no wait)", len(self._startup_pending))

            # Drain queue quickly (no retries on error)
            while self._startup_pending and (not self._stop_flag) and self.enabled:
                before = len(self._startup_pending)
                ok = self._flush_startup_pending_once()
                if not ok:
                    break
                if len(self._startup_pending) == before:
                    break

            if self._startup_pending:
                _log_warning(self.log, "[nina] startup sender: %d startup warning(s) NOT sent (dropping, no retry).", len(self._startup_pending))
                self._startup_pending.clear()

        except Exception as e:
            _log_warning(self.log, "[nina] startup sender crashed: %r", e)
            try:
                self._startup_pending.clear()
            except Exception:
                pass

    def _flush_startup_pending_once(self) -> bool:
        """Try to send up to max_per_tick startup messages. Return False on first send error."""
        if not self._startup_pending or not self._posted:
            return True

        to_send = min(len(self._startup_pending), self.max_per_tick)
        sent_ok = 0
        for _ in range(to_send):
            ident, msg = self._startup_pending[0]
            try:
                self.bot.api_send_channel(channel=int(self.channel), destination_id=self.destination_id, text=msg)
                self._posted.add(ident)
                self._startup_pending.pop(0)
                sent_ok += 1
            except Exception as e:
                _log_warning(self.log, "[nina] startup send failed: %r", e)
                return False

        if sent_ok:
            _log_info(self.log, "[nina] startup sender: sent %d, remaining=%d", sent_ok, len(self._startup_pending))
        return True

    # ---------------- identifiers + field extraction ----------------

    def _identifier_for(self, item: Dict[str, Any], ars: str) -> str:
        ident = str(item.get("identifier") or item.get("id") or "").strip()
        if ident:
            return ident
        title = self._get_title(item)
        sent = self._get_sent(item)
        sender = ""
        data = item.get("data")
        if isinstance(data, dict):
            snd = data.get("sender")
            if isinstance(snd, dict):
                sender = str(snd.get("name") or "").strip()
            elif isinstance(snd, str):
                sender = snd.strip()
        base = f"{ars}|{title}|{sent}|{sender}"
        return "hash:" + hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()

    def _get_sent(self, item: Dict[str, Any]) -> Any:
        sent = item.get("sent") or item.get("published") or item.get("timestamp") or item.get("date") or ""
        data = item.get("data")
        if isinstance(data, dict):
            sent = sent or (data.get("sent") or data.get("published") or data.get("timestamp") or data.get("date") or "")
        return sent

    def _get_title(self, item: Dict[str, Any]) -> str:
        def pick_from_i18n(i18n: Any) -> str:
            if isinstance(i18n, dict):
                for k in ("de", "DE", "text", "value", "title"):
                    v = i18n.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
            if isinstance(i18n, str) and i18n.strip():
                return i18n.strip()
            if isinstance(i18n, list):
                for v in i18n:
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                    if isinstance(v, dict):
                        vv = v.get("de") or v.get("DE") or v.get("text") or v.get("value") or v.get("title")
                        if isinstance(vv, str) and vv.strip():
                            return vv.strip()
            return ""

        def pick_from_obj(obj: Any) -> str:
            if not isinstance(obj, dict):
                return ""
            for key in ("i18nTitle", "i18nHeadline", "i18nEvent"):
                t = pick_from_i18n(obj.get(key))
                if t:
                    return t
            for key in ("headline", "title", "event", "info", "description", "shortText", "msg", "text"):
                v = obj.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()
                if isinstance(v, dict):
                    vv = v.get("de") or v.get("DE") or v.get("text") or v.get("value") or v.get("title")
                    if isinstance(vv, str) and vv.strip():
                        return vv.strip()
            cap = obj.get("cap")
            if isinstance(cap, dict):
                for key in ("headline", "event", "description"):
                    v = cap.get(key)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
            return ""

        t = pick_from_obj(item)
        if t:
            return t
        t = pick_from_obj(item.get("data"))
        if t:
            return t
        t = pick_from_obj(item.get("payload"))
        if t:
            return t
        return ""

    # ---------------- startup snapshot ----------------

    def _startup_snapshot_and_queue(self) -> None:
        if not self._posted:
            return

        _log_info(self.log, "[nina] startup snapshot: fetching current warnings for %d ARS ...", len(self._resolved_ars))
        shown = 0
        queued = 0
        cap = 300

        for ars in self._resolved_ars:
            kreis_name = self._ars_names.get_name(ars) if self._ars_names else None
            if not kreis_name:
                kreis_name = f"ARS {ars}"

            for it in self._fetch_items(ars):
                ident = self._identifier_for(it, ars)
                title = self._get_title(it)
                hhmm = _time_hhmm(self._get_sent(it))

                if not title:
                    dk = sorted(list(it.get("data", {}).keys()))[:20] if isinstance(it.get("data"), dict) else []
                    _log_warning(self.log, "[nina] startup item without title (ars=%s ident=%s keys=%s data_keys=%s)", ars, ident, sorted(list(it.keys()))[:20], dk)
                    title = "(no title)"

                _log_info(self.log, _compact(f"[nina][startup] {hhmm} {kreis_name}: {title}", 1400))
                shown += 1

                msg = self._format_warning(it, ars)
                if msg and ident and (not self._posted.has(ident)):
                    self._startup_pending.append((ident, msg))
                    queued += 1

                if shown >= cap:
                    break

        _log_info(self.log, "[nina] startup snapshot: done (logged %d warnings).", shown)
        _log_info(self.log, "[nina] startup snapshot: queued %d warning(s) for channel posting.", queued)

    # ---------------- targets & region cache ----------------

    def _resolve_targets(self) -> List[str]:
        if self.ars_list:
            out = []
            for a in self.ars_list:
                d = re.sub(r"\D", "", a)
                if re.fullmatch(r"\d{12}", d):
                    out.append(d)
                else:
                    _log_warning(self.log, "[nina] ignoring invalid ARS: %r", a)
            return sorted(list(dict.fromkeys(out)))

        if not self._regions:
            return []

        if self.counties:
            out = []
            for name in self.counties:
                n = _norm_name(name)
                ars = self._regions.get_ars(n)
                if ars:
                    out.append(ars)
                else:
                    stripped = " ".join([w for w in n.split() if w not in ("landkreis","kreis","stadt","gemeinde","staedteregion","stadteregion")])
                    ars2 = self._regions.get_ars(stripped) if stripped else None
                    if ars2:
                        out.append(ars2)
                    else:
                        _log_warning(self.log, "[nina] could not resolve county name: %r", name)
            return sorted(list(dict.fromkeys(out)))

        state_code = STATE_ALIASES.get(_norm_name(self.state)) or "05"
        _log_info(self.log, "[nina] state resolve: state=%s -> code=%s", self.state, state_code)
        ars_all = self._regions.distinct_ars_by_state(state_code)
        _log_info(self.log, "[nina] state resolve: found %d ARS targets for code=%s", len(ars_all), state_code)
        return sorted(list(dict.fromkeys(ars_all)))

    def _ensure_region_cache(self) -> None:
        if not self._regions or not self._ars_names:
            return

        loaded = self._regions.get_meta("loaded") == "1"
        rows = self._regions.count_rows()
        if loaded and rows > 0:
            _log_info(self.log, "[nina] regions cache meta=loaded, rows=%d", rows)
            return

        _log_info(self.log, "[nina] building Landkreis/ARS cache from Destatis dataset ...")
        _log_info(self.log, "[nina] downloading regions dataset: %s", self.DESTATS_RS_JSON_URL)

        data = _http_get_json(self.DESTATS_RS_JSON_URL, timeout=60)
        rows_data = data.get("daten") if isinstance(data, dict) else None
        if not isinstance(rows_data, list):
            raise RuntimeError("Destatis dataset schema unexpected: missing data['daten'] list")

        inserted = 0
        for row in rows_data:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            ars_raw = str(row[0] or "").strip()
            name_s = str(row[1] or "").strip()
            ars_digits = re.sub(r"\D", "", ars_raw)
            if not re.fullmatch(r"\d{12}", ars_digits) or not name_s:
                continue

            ars_kreis = ars_digits[:5] + "0000000"
            if not re.fullmatch(r"\d{12}", ars_kreis):
                continue

            n = _norm_name(name_s)
            if not n:
                continue

            self._regions.put(n, name_s, ars_kreis)
            src = "kreis" if ars_digits.endswith("0000000") else "fallback"
            self._ars_names.put(ars_kreis, name_s, src)
            inserted += 1

        self._conn.commit()
        if inserted > 0:
            self._regions.set_meta("loaded", "1")
            self._regions.set_meta("loaded_ts", str(int(time.time())))
            self._conn.commit()
            _log_info(self.log, "[nina] region cache ready: %d name->ARS entries", inserted)
        else:
            raise RuntimeError("Regions dataset parsed but inserted 0 rows")

    # ---------------- NINA fetch + formatting ----------------

    def _fetch_items(self, ars: str) -> List[Dict[str, Any]]:
        url = self.DASHBOARD_URL.format(ars=ars)
        data = _http_get_json(url, timeout=25)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for key in ("items", "warnings", "results", "data"):
                v = data.get(key)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        return []

    def _format_warning(self, item: Dict[str, Any], ars: str) -> str:
        title = self._get_title(item)
        if not title:
            return ""

        # Best-effort filters (pull sender/category from nested data if available)
        sender = ""
        category = ""
        data = item.get("data")
        if isinstance(data, dict):
            snd = data.get("sender")
            if isinstance(snd, dict):
                sender = str(snd.get("name") or "").strip()
            elif isinstance(snd, str):
                sender = snd.strip()
            category = str(data.get("provider") or data.get("type") or data.get("category") or "").strip()

        sender = sender or (str(item.get("sender") or "").strip())
        category = category or (str(item.get("provider") or item.get("type") or item.get("category") or "").strip())

        cat_l = category.lower()
        snd_l = sender.lower()
        if (not self.include_police) and ("police" in cat_l or "polizei" in cat_l or "police" in snd_l or "polizei" in snd_l):
            return ""
        if (not self.include_dwd) and ("dwd" in cat_l or "wetter" in cat_l or "dwd" in snd_l or "wetter" in snd_l):
            return ""
        if (not self.include_mowas) and ("mowas" in cat_l or "katastroph" in cat_l or "mowas" in snd_l):
            return ""

        hhmm = _time_hhmm(self._get_sent(item))

        kreis_name = self._ars_names.get_name(ars) if self._ars_names else None
        if not kreis_name:
            kreis_name = f"ARS {ars}"

        return _compact(f"{hhmm} {kreis_name}: {title}", 900)


def setup(bot: Any) -> NinaWarningsPlugin:
    return NinaWarningsPlugin(bot)
