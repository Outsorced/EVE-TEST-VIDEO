from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

from .constants import TS_FMT
from .models import AffiliationRecord
from .text import parse_ts, normalize_key
from .prompts import Prompter


AffDB = Dict[str, AffiliationRecord]

# Pilot-level tickers learned from the logs during a run.
PilotToCorp = Dict[str, str]
PilotToAlliance = Dict[str, str]


def _norm_key(s: str) -> str:
    return normalize_key(s or "")


def build_pilot_ticker_maps(rows: List[Dict[str, Any]]) -> Tuple[PilotToCorp, PilotToAlliance]:
    """Build pilot->corp and pilot->alliance maps from any row that contains them.

    We use this to backfill missing corp tickers when an alliance is present but the
    corp field failed to parse in some lines.

    Strategy:
      - If we ever see a pilot with a corp ticker, remember it.
      - If we ever see a pilot with an alliance ticker, remember it.

    We don't attempt alliance->corp because that mapping is not 1:1.
    """

    pilot_to_corp: PilotToCorp = {}
    pilot_to_all: PilotToAlliance = {}

    for r in rows:
        for side in ("source", "target"):
            pilot = _norm_key(r.get(f"{side}_pilot", ""))
            corp = _norm_key(r.get(f"{side}_corp", ""))
            allc = _norm_key(r.get(f"{side}_alliance", ""))
            if pilot:
                if corp:
                    pilot_to_corp[pilot] = corp
                if allc:
                    pilot_to_all[pilot] = allc

    return pilot_to_corp, pilot_to_all


def fill_missing_corps_from_pilot_map(rows: List[Dict[str, Any]], pilot_to_corp: PilotToCorp) -> int:
    """Fill blank *_corp fields using a pilot->corp map learned from other lines."""

    filled = 0
    for r in rows:
        for side in ("source", "target"):
            corp_key = f"{side}_corp"
            if _norm_key(r.get(corp_key, "")):
                continue
            pilot = _norm_key(r.get(f"{side}_pilot", ""))
            if pilot and pilot in pilot_to_corp:
                r[corp_key] = pilot_to_corp[pilot]
                filled += 1
    return filled


def update_affiliation_db(aff_db: AffDB, corp: str, alliance: str, ts: datetime) -> None:
    corp = (corp or "").strip()
    alliance = (alliance or "").strip()
    if not corp or not alliance:
        return
    rec = aff_db.get(corp)
    if rec is None:
        aff_db[corp] = AffiliationRecord(alliance=alliance, first_seen=ts, last_seen=ts)
    else:
        rec.last_seen = ts
        rec.alliance = alliance  # keep most recent


def fill_alliance_from_aff_db(rows: List[Dict[str, Any]], aff_db: AffDB) -> int:
    filled = 0
    for r in rows:
        if not (r.get("source_alliance") or "").strip():
            corp = (r.get("source_corp") or "").strip()
            if corp and corp in aff_db:
                r["source_alliance"] = aff_db[corp].alliance
                filled += 1

        if not (r.get("target_alliance") or "").strip():
            corp = (r.get("target_corp") or "").strip()
            if corp and corp in aff_db:
                r["target_alliance"] = aff_db[corp].alliance
                filled += 1
    return filled


def save_aff_db(path: str, aff_db: AffDB) -> None:
    out = {
        corp: {
            "alliance": rec.alliance,
            "first_seen": rec.first_seen.strftime(TS_FMT),
            "last_seen": rec.last_seen.strftime(TS_FMT),
        }
        for corp, rec in aff_db.items()
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_aff_db(path: str) -> AffDB:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        aff_db: AffDB = {}
        for corp, rec in data.items():
            alliance = (rec.get("alliance") or "").strip()
            fs = rec.get("first_seen")
            ls = rec.get("last_seen")
            if not corp or not alliance or not fs or not ls:
                continue
            aff_db[corp] = AffiliationRecord(
                alliance=alliance,
                first_seen=parse_ts(fs),
                last_seen=parse_ts(ls),
            )
        return aff_db
    except Exception:
        return {}


def maybe_reset_aff_db(path: str, label: str, prompter: Prompter) -> Tuple[AffDB, bool]:
    """Load or reset an affiliation DB.

    Returns (db, should_save).
    """

    if not path:
        return {}, False

    if os.path.exists(path):
        print(f"{label} DB found: {path}")
        ans = prompter.choice(
            f"Use existing {label} DB?",
            choices={
                "y": "use",
                "u": "update/rebuild (delete file)",
                "n": "no file (in-memory only)",
            },
            default="y",
        )
        if ans == "y":
            return load_aff_db(path), True
        if ans == "u":
            try:
                os.remove(path)
                print(f"Deleted old {label} DB. Will rebuild fresh from current run.")
            except OSError:
                pass
            return {}, True
        if ans == "n":
            print(f"Will build {label} DB in-memory only (not saving to disk).")
            return {}, False
        return load_aff_db(path), True

    print(f"No {label} DB found at: {path}")
    if prompter.confirm(f"Create and save a new {label} DB from this run?", default=True):
        return {}, True
    print(f"Will build {label} DB in-memory only (not saving to disk).")
    return {}, False
