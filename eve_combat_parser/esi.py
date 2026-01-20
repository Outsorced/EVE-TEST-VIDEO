from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import AffiliationRecord
from .prompts import Prompter


ESI_BASE = "https://esi.evetech.net/latest"
ESI_DATASOURCE = "tranquility"
ESI_HEADERS = {"User-Agent": "EVE-Combat-Log-Parser/0.x (+local script)"}


def requests_import_guard() -> None:
    try:
        import requests  # noqa: F401
    except ImportError as e:
        raise SystemExit("Missing dependency: requests. Install with: pip install requests") from e


def load_cache(cache_file: str) -> Dict[str, Any]:
    if not cache_file or not os.path.exists(cache_file):
        return {
            "character_ids": {},
            "corp_info": {},
            "pilot_alliance": {},
            # Ship/type enrichment
            "type_ids": {},
            "type_info": {},
            "group_info": {},
        }
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("character_ids", {})
        data.setdefault("corp_info", {})
        data.setdefault("pilot_alliance", {})
        data.setdefault("type_ids", {})
        data.setdefault("type_info", {})
        data.setdefault("group_info", {})
        return data
    except Exception:
        return {
            "character_ids": {},
            "corp_info": {},
            "pilot_alliance": {},
            "type_ids": {},
            "type_info": {},
            "group_info": {},
        }


def save_cache(cache_file: str, cache: Dict[str, Any]) -> None:
    if not cache_file:
        return
    tmp = cache_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, cache_file)


def maybe_reset_cache(cache_file: str, prompter: Prompter) -> Tuple[Dict[str, Any], bool]:
    if not cache_file:
        return {
            "character_ids": {},
            "corp_info": {},
            "pilot_alliance": {},
            "type_ids": {},
            "type_info": {},
            "group_info": {},
        }, False

    if os.path.exists(cache_file):
        print(f"ESI cache file found: {cache_file}")
        ans = prompter.choice(
            "Use existing ESI cache?",
            choices={"y": "use", "u": "update/rebuild", "n": "no file"},
            default="y",
        )
        if ans == "y":
            return load_cache(cache_file), True
        if ans == "u":
            try:
                os.remove(cache_file)
                print("Deleted old ESI cache. Will rebuild fresh.")
            except OSError:
                pass
            return {
                "character_ids": {},
                "corp_info": {},
                "pilot_alliance": {},
                "type_ids": {},
                "type_info": {},
                "group_info": {},
            }, True
        if ans == "n":
            print("Will use in-memory ESI cache only (not saving to disk).")
            return {
                "character_ids": {},
                "corp_info": {},
                "pilot_alliance": {},
                "type_ids": {},
                "type_info": {},
                "group_info": {},
            }, False
        return load_cache(cache_file), True

    print(f"No ESI cache file found at: {cache_file}")
    if prompter.confirm("Create and save a new ESI cache file?", default=True):
        return {
            "character_ids": {},
            "corp_info": {},
            "pilot_alliance": {},
            "type_ids": {},
            "type_info": {},
            "group_info": {},
        }, True
    print("Will use in-memory ESI cache only (not saving to disk).")
    return {
        "character_ids": {},
        "corp_info": {},
        "pilot_alliance": {},
        "type_ids": {},
        "type_info": {},
        "group_info": {},
    }, False


def esi_get_character_id(name: str, cache: Dict[str, Any], sleep_s: float) -> Optional[int]:
    name = name.strip()
    if not name:
        return None
    if name in cache["character_ids"]:
        return cache["character_ids"][name]

    import requests

    try:
        r = requests.post(
            f"{ESI_BASE}/universe/ids/?datasource={ESI_DATASOURCE}",
            json=[name],
            headers={**ESI_HEADERS, "Content-Type": "application/json"},
            timeout=10,
        )
        if r.status_code != 200:
            cache["character_ids"][name] = None
            return None

        data = r.json()
        for c in data.get("characters", []):
            if c.get("name") == name:
                cache["character_ids"][name] = c.get("id")
                time.sleep(sleep_s)
                return cache["character_ids"][name]

        cache["character_ids"][name] = None
        time.sleep(sleep_s)
        return None
    except Exception:
        cache["character_ids"][name] = None
        return None


def esi_get_type_id(name: str, cache: Dict[str, Any], sleep_s: float) -> Optional[int]:
    """Resolve an EVE type ID from a type name via /universe/ids.

    Uses the same endpoint as character ID resolution, but reads the "inventory_types"
    section of the response.
    """

    name = name.strip()
    if not name:
        return None
    if name in cache.get("type_ids", {}):
        return cache["type_ids"][name]

    import requests

    try:
        r = requests.post(
            f"{ESI_BASE}/universe/ids/?datasource={ESI_DATASOURCE}",
            json=[name],
            headers={**ESI_HEADERS, "Content-Type": "application/json"},
            timeout=10,
        )
        if r.status_code != 200:
            cache["type_ids"][name] = None
            return None

        data = r.json()
        for t in data.get("inventory_types", []) or []:
            if t.get("name") == name:
                cache["type_ids"][name] = t.get("id")
                time.sleep(sleep_s)
                return cache["type_ids"][name]

        cache["type_ids"][name] = None
        time.sleep(sleep_s)
        return None
    except Exception:
        cache["type_ids"][name] = None
        return None


def esi_get_type_info(type_id: int, cache: Dict[str, Any], sleep_s: float) -> Dict[str, Any]:
    """Return cached type info (notably group_id) for a type_id."""
    key = str(int(type_id))
    if key in cache.get("type_info", {}):
        return cache["type_info"][key] or {}

    import requests

    try:
        r = requests.get(
            f"{ESI_BASE}/universe/types/{int(type_id)}/?datasource={ESI_DATASOURCE}&language=en",
            headers=ESI_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            cache["type_info"][key] = {}
            time.sleep(sleep_s)
            return {}
        info = r.json() or {}
        cache["type_info"][key] = info
        time.sleep(sleep_s)
        return info
    except Exception:
        cache["type_info"][key] = {}
        return {}


def esi_get_group_info(group_id: int, cache: Dict[str, Any], sleep_s: float) -> Dict[str, Any]:
    """Return cached group info (notably group name + category_id)."""
    key = str(int(group_id))
    if key in cache.get("group_info", {}):
        return cache["group_info"][key] or {}

    import requests

    try:
        r = requests.get(
            f"{ESI_BASE}/universe/groups/{int(group_id)}/?datasource={ESI_DATASOURCE}&language=en",
            headers=ESI_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            cache["group_info"][key] = {}
            time.sleep(sleep_s)
            return {}
        info = r.json() or {}
        cache["group_info"][key] = info
        time.sleep(sleep_s)
        return info
    except Exception:
        cache["group_info"][key] = {}
        return {}


def esi_get_alliance_ticker(alliance_id: int, sleep_s: float) -> str:
    import requests

    try:
        r = requests.get(
            f"{ESI_BASE}/alliances/{alliance_id}/?datasource={ESI_DATASOURCE}",
            headers=ESI_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            time.sleep(sleep_s)
            return ""
        ticker = (r.json().get("ticker") or "").strip()
        time.sleep(sleep_s)
        return ticker
    except Exception:
        return ""


def esi_get_corp_ticker_and_alliance_ticker(corp_id: int, cache: Dict[str, Any], sleep_s: float) -> Tuple[str, str]:
    key = str(corp_id)
    if key in cache["corp_info"]:
        info = cache["corp_info"][key] or {}
        return (info.get("ticker") or ""), (info.get("alliance_ticker") or "")

    import requests

    try:
        r = requests.get(
            f"{ESI_BASE}/corporations/{corp_id}/?datasource={ESI_DATASOURCE}",
            headers=ESI_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            cache["corp_info"][key] = {"ticker": "", "alliance_ticker": ""}
            return "", ""

        corp = r.json()
        corp_ticker = (corp.get("ticker") or "").strip()

        alliance_ticker = ""
        alliance_id = corp.get("alliance_id")
        if alliance_id:
            alliance_ticker = esi_get_alliance_ticker(int(alliance_id), sleep_s=sleep_s)

        cache["corp_info"][key] = {"ticker": corp_ticker, "alliance_ticker": alliance_ticker}
        time.sleep(sleep_s)
        return corp_ticker, alliance_ticker
    except Exception:
        cache["corp_info"][key] = {"ticker": "", "alliance_ticker": ""}
        return "", ""


def esi_get_pilot_alliance_and_corpinfo(pilot_name: str, cache: Dict[str, Any], sleep_s: float) -> Tuple[str, str]:
    """Return (alliance_ticker, corp_ticker) for a pilot name."""

    pilot_name = pilot_name.strip()
    if not pilot_name:
        return "", ""

    if pilot_name in cache["pilot_alliance"]:
        return cache["pilot_alliance"][pilot_name] or "", ""

    import requests

    char_id = esi_get_character_id(pilot_name, cache, sleep_s)
    if not char_id:
        cache["pilot_alliance"][pilot_name] = ""
        return "", ""

    try:
        r = requests.get(
            f"{ESI_BASE}/characters/{char_id}/?datasource={ESI_DATASOURCE}",
            headers=ESI_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            cache["pilot_alliance"][pilot_name] = ""
            time.sleep(sleep_s)
            return "", ""

        corp_id = r.json().get("corporation_id")
        if not corp_id:
            cache["pilot_alliance"][pilot_name] = ""
            time.sleep(sleep_s)
            return "", ""

        corp_ticker, alliance_ticker = esi_get_corp_ticker_and_alliance_ticker(int(corp_id), cache, sleep_s)
        cache["pilot_alliance"][pilot_name] = alliance_ticker or ""
        time.sleep(sleep_s)
        return alliance_ticker or "", corp_ticker or ""
    except Exception:
        cache["pilot_alliance"][pilot_name] = ""
        return "", ""


def enrich_missing_alliances_via_esi(
    rows: List[Dict[str, Any]],
    cache: Dict[str, Any],
    sleep_s: float,
    item_names_lower: Set[str],
    aff_esi: Dict[str, AffiliationRecord],
) -> Tuple[int, int]:
    """ESI fallback pilot lookup for remaining blanks.

    Also learns corp_ticker -> alliance_ticker into aff_esi.
    Returns (filled_fields, learned_corp_mappings).
    """

    def ok_to_lookup_pilot(name: str) -> bool:
        return name.strip().lower() not in item_names_lower

    need: List[str] = []
    seen = set()

    for row in rows:
        sp = (row.get("source_pilot") or "").strip()
        tp = (row.get("target_pilot") or "").strip()

        if sp and not (row.get("source_alliance") or "").strip() and ok_to_lookup_pilot(sp):
            if sp not in seen:
                need.append(sp)
                seen.add(sp)

        if tp and not (row.get("target_alliance") or "").strip() and ok_to_lookup_pilot(tp):
            if tp not in seen:
                need.append(tp)
                seen.add(tp)

    if not need:
        print("ESI: no missing alliances to resolve (after log DBs + SDE filter).")
        return 0, 0

    print(f"ESI: resolving missing alliances for {len(need)} pilots (cached, best-effort)...")

    resolved: Dict[str, str] = {}
    learned = 0

    for i, pilot in enumerate(need, 1):
        alliance_ticker, corp_ticker = esi_get_pilot_alliance_and_corpinfo(pilot, cache, sleep_s)
        resolved[pilot] = alliance_ticker or ""

        if corp_ticker and alliance_ticker:
            if corp_ticker not in aff_esi:
                learned += 1
                now = datetime.now()
                aff_esi[corp_ticker] = AffiliationRecord(alliance=alliance_ticker, first_seen=now, last_seen=now)
            else:
                aff_esi[corp_ticker].alliance = alliance_ticker
                aff_esi[corp_ticker].last_seen = datetime.now()

        if i % 10 == 0 or i == len(need):
            print(f"  {i}/{len(need)} resolved")

    filled = 0
    for row in rows:
        sp = (row.get("source_pilot") or "").strip()
        tp = (row.get("target_pilot") or "").strip()

        if sp and not (row.get("source_alliance") or "").strip():
            v = resolved.get(sp, "") or ""
            if v:
                row["source_alliance"] = v
                filled += 1

        if tp and not (row.get("target_alliance") or "").strip():
            v = resolved.get(tp, "") or ""
            if v:
                row["target_alliance"] = v
                filled += 1

    return filled, learned
