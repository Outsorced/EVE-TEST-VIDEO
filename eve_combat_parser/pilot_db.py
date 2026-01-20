from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Set

from .text import normalize_key
from .npc import looks_like_charge, looks_like_drone


def _nk(s: str) -> str:
    return normalize_key(s or "")


@dataclass
class PilotInfo:
    corp: str = ""
    alliance: str = ""
    ship_type: str = ""


PilotDB = Dict[str, PilotInfo]


def load_pilot_db(path: str) -> PilotDB:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        out: PilotDB = {}
        if isinstance(data, dict):
            for pilot, rec in data.items():
                p = _nk(pilot)
                if not p or not isinstance(rec, dict):
                    continue
                out[p] = PilotInfo(
                    corp=(rec.get("corp") or "").strip(),
                    alliance=(rec.get("alliance") or "").strip(),
                    ship_type=(rec.get("ship_type") or "").strip(),
                )
        return out
    except Exception:
        return {}


def save_pilot_db(path: str, db: PilotDB) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({k: asdict(v) for k, v in db.items()}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def learn_from_rows(db: PilotDB, rows: list[dict[str, Any]], *, learn_ship: bool = True) -> int:
    """Update the pilot DB with any non-empty fields we see.

    Parameters
    ----------
    learn_ship:
        If False, do not learn ship types. This is important when the input
        rows span multiple fights where pilots may have swapped ships.
    """

    updates = 0
    for r in rows:
        for side in ("source", "target"):
            pilot = _nk(r.get(f"{side}_pilot", ""))
            if not pilot:
                continue
            corp = (r.get(f"{side}_corp", "") or "").strip()
            allc = (r.get(f"{side}_alliance", "") or "").strip()
            ship = (r.get(f"{side}_ship_type", "") or "").strip()

            cur = db.get(pilot) or PilotInfo()
            changed = False
            if corp and corp != cur.corp:
                cur.corp = corp
                changed = True
            if allc and allc != cur.alliance:
                cur.alliance = allc
                changed = True
            if learn_ship and ship and ship != cur.ship_type:
                cur.ship_type = ship
                changed = True
            if changed:
                db[pilot] = cur
                updates += 1
    return updates


def learn_from_rows_excluding_items(
    db: PilotDB,
    rows: list[dict[str, Any]],
    *,
    item_names_lower: Set[str],
    ship_meta: Optional[Any] = None,
    learn_ship: bool = True,
) -> int:
    """Like learn_from_rows, but skip drones/charges as "pilots".

    This prevents the persistent DB and within-fight cross-reference from
    getting polluted by entities like "Vespa EC-600" or "Scourge Heavy Missile".
    """

    # ship_meta is accepted for forward-compatibility with callers that may
    # want to learn additional ship metadata in the future. The PilotDB only
    # stores ship_type today, so this parameter is intentionally unused.
    _ = ship_meta

    updates = 0
    for r in rows:
        for side in ("source", "target"):
            pilot_raw = str(r.get(f"{side}_pilot", "") or "")
            if looks_like_drone(pilot_raw, item_names_lower) or looks_like_charge(pilot_raw, item_names_lower):
                continue

            pilot = _nk(pilot_raw)
            if not pilot:
                continue
            corp = (r.get(f"{side}_corp", "") or "").strip()
            allc = (r.get(f"{side}_alliance", "") or "").strip()
            ship = (r.get(f"{side}_ship_type", "") or "").strip()

            cur = db.get(pilot) or PilotInfo()
            changed = False
            if corp and corp != cur.corp:
                cur.corp = corp
                changed = True
            if allc and allc != cur.alliance:
                cur.alliance = allc
                changed = True
            if learn_ship and ship and ship != cur.ship_type:
                cur.ship_type = ship
                changed = True
            if changed:
                db[pilot] = cur
                updates += 1
    return updates


def backfill_rows_from_db(db: PilotDB, rows: list[dict[str, Any]], *, fill_ship: bool = True) -> int:
    """Fill missing corp/alliance/ship_type fields from the pilot DB.

    Parameters
    ----------
    fill_ship:
        If False, do not backfill ship types. This is important when the
        rows may span multiple fights (ship swaps between fights are common).
    """

    filled = 0
    for r in rows:
        for side in ("source", "target"):
            pilot = _nk(r.get(f"{side}_pilot", ""))
            if not pilot:
                continue
            info: Optional[PilotInfo] = db.get(pilot)
            if not info:
                continue

            corp_key = f"{side}_corp"
            all_key = f"{side}_alliance"
            ship_key = f"{side}_ship_type"

            if not (r.get(corp_key) or "").strip() and info.corp:
                r[corp_key] = info.corp
                filled += 1
            if not (r.get(all_key) or "").strip() and info.alliance:
                r[all_key] = info.alliance
                filled += 1
            if fill_ship and (not (r.get(ship_key) or "").strip()) and info.ship_type:
                r[ship_key] = info.ship_type
                filled += 1
    return filled
