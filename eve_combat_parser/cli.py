from __future__ import annotations

import argparse
import functools
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from .affiliations import (
    build_pilot_ticker_maps,
    fill_alliance_from_aff_db,
    fill_missing_corps_from_pilot_map,
    maybe_reset_aff_db,
    save_aff_db,
)
from .pilot_db import (
    backfill_rows_from_db,
    learn_from_rows,
    learn_from_rows_excluding_items,
    load_pilot_db,
    save_pilot_db,
)
from .constants import (
    DEFAULT_AFF_ESI_DB_FILE,
    DEFAULT_AFF_LOG_DB_FILE,
    DEFAULT_CACHE_FILE,
    DEFAULT_ESI_SLEEP,
    DEFAULT_LOG_FOLDER,
    DEFAULT_SDE_DIR,
    TS_FMT,
    SCHEMA_VERSION,
    PARSER_VERSION,
    METADATA_HEADERS,
    FIGHT_SUMMARY_HEADERS_BASE,
    PILOT_LIST_HEADERS_BASE,
    PILOT_SHIP_SESSIONS_HEADERS,
    INSTANCE_SUMMARY_HEADERS,
    ALLIANCE_CORP_LIST_HEADERS,
    HEADERS,
    OTHERS_HEADERS,
    OUT_DAMAGE_DONE_NPC,
    OUT_DAMAGE_DONE_PLAYERS,
    OUT_DAMAGE_DONE_DRONES,
    OUT_DAMAGE_DONE_CHARGES,
    OUT_DAMAGE_RECEIVED_NPC,
    OUT_DAMAGE_RECEIVED_PLAYERS,
    OUT_DAMAGE_RECEIVED_DRONES,
    OUT_DAMAGE_RECEIVED_CHARGES,
    OUT_EWAR_DONE_NPC,
    OUT_EWAR_DONE_PLAYERS,
    OUT_EWAR_DONE_DRONES,
    OUT_EWAR_DONE_CHARGES,
    OUT_EWAR_RECEIVED_NPC,
    OUT_EWAR_RECEIVED_PLAYERS,
    OUT_EWAR_RECEIVED_DRONES,
    OUT_EWAR_RECEIVED_CHARGES,
    OUT_EWAR_EFFECTS_DONE_NPC,
    OUT_EWAR_EFFECTS_DONE_PLAYERS,
    OUT_EWAR_EFFECTS_DONE_DRONES,
    OUT_EWAR_EFFECTS_DONE_CHARGES,
    OUT_EWAR_EFFECTS_RECEIVED_NPC,
    OUT_EWAR_EFFECTS_RECEIVED_PLAYERS,
    OUT_EWAR_EFFECTS_RECEIVED_DRONES,
    OUT_EWAR_EFFECTS_RECEIVED_CHARGES,
    OUT_CAP_WARFARE_DONE_NPC,
    OUT_CAP_WARFARE_DONE_PLAYERS,
    OUT_CAP_WARFARE_DONE_DRONES,
    OUT_CAP_WARFARE_DONE_CHARGES,
    OUT_CAP_WARFARE_RECEIVED_NPC,
    OUT_CAP_WARFARE_RECEIVED_PLAYERS,
    OUT_CAP_WARFARE_RECEIVED_DRONES,
    OUT_CAP_WARFARE_RECEIVED_CHARGES,
    OUT_CAPACITOR_DONE_NPC,
    OUT_CAPACITOR_DONE_PLAYERS,
    OUT_CAPACITOR_DONE_DRONES,
    OUT_CAPACITOR_DONE_CHARGES,
    OUT_CAPACITOR_RECEIVED_NPC,
    OUT_CAPACITOR_RECEIVED_PLAYERS,
    OUT_CAPACITOR_RECEIVED_DRONES,
    OUT_CAPACITOR_RECEIVED_CHARGES,
    OUT_OTHERS,
    OUT_PROPULSION_JAM_ATTEMPTS,
    PROP_JAM_HEADERS,
    OUT_REPAIRS_DONE_NPC,
    OUT_REPAIRS_DONE_PLAYERS,
    OUT_REPAIRS_DONE_DRONES,
    OUT_REPAIRS_DONE_CHARGES,
    OUT_REPAIRS_RECEIVED_NPC,
    OUT_REPAIRS_RECEIVED_PLAYERS,
    OUT_REPAIRS_RECEIVED_DRONES,
    OUT_REPAIRS_RECEIVED_CHARGES,
)
from .esi import (
    enrich_missing_alliances_via_esi,
    maybe_reset_cache,
    requests_import_guard,
    save_cache,
)
from . import exporter as exporter
from .exporter import write_csv
from .npc import (
    split_rows_players_npc_drones_charges,
    build_known_players,
    classify_party_kind,
    looks_like_drone_or_item,
)
from .parser import build_ship_timeline_and_afflog, parse_log_file_to_rows
from .prompts import PromptConfig, Prompter
from .sde import ensure_sde_present, load_item_name_set
from .fights import filter_rows_by_window, split_rows_into_fights
from .timeline import lookup_ship, restrict_timeline
from .ship_meta import ShipMetaResolver, MONTH_ABBR_LOWER
from .module_meta import ModuleMetaResolver
from .version import __version__
from .ship_meta import ShipMetaResolver, MONTH_ABBR_LOWER


def _open_folder(path: Path) -> None:
    """Best-effort open of a folder in the user's file explorer."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.run(["open", str(path)], check=False)
        else:
            subprocess.run(["xdg-open", str(path)], check=False)
    except Exception:
        # Never fail the run just because we couldn't open a folder.
        return


def _append_metadata_headers(headers: List[str]) -> List[str]:
    out = list(headers)
    for h in METADATA_HEADERS:
        if h not in out:
            out.append(h)
    return out


def _write_fight_summary(
    path: Path,
    fight_i: int,
    win,
    rows: List[Dict[str, Any]],
    *,
    counts: Dict[str, int] | None = None,
    item_names_lower: set[str] | None = None,
    ship_meta=None,
    metadata: Dict[str, Any] | None = None,
) -> None:
    """Write fight summaries.

    Creates a subfolder "summary" under the fight folder containing:
    - fight_summary.txt (human readable)
    - fight_summary.json (structured)
    - fight_summary.csv (single-row csv)
    - fight_roster.csv (pilots with corp/alliance + ship types)
    - (removed) fight_lists.csv (redundant with fight_roster.csv)
    """

    summary_dir = path / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    meta = metadata or {}

    # Collect sets
    pilots = set()
    ships = set()
    corps = set()
    alliances = set()
    log_files = set()

    # Pilot -> sets for roster
    pilot_to_corps: Dict[str, set[str]] = {}
    pilot_to_alliances: Dict[str, set[str]] = {}
    pilot_to_ships: Dict[str, set[str]] = {}
    pilot_to_fighters: Dict[str, set[str]] = {}

    def _add_pilot(p: str, corp: str = "", alli: str = "", ship: str = "") -> None:
        p = (p or "").strip()
        if not p:
            return
        if item_names_lower is not None and looks_like_drone_or_item(p, item_names_lower):
            return
        pilots.add(p)
        if corp:
            corps.add(corp)
            pilot_to_corps.setdefault(p, set()).add(corp)
        else:
            pilot_to_corps.setdefault(p, set())
        if alli:
            alliances.add(alli)
            pilot_to_alliances.setdefault(p, set()).add(alli)
        else:
            pilot_to_alliances.setdefault(p, set())
        if ship:
            # Filter drones out of Pilot_list ship types (pilots always have a hull).
            # Fighters are an important edge case: keep them, but in a separate list.
            kind = "unknown"
            if ship_meta is not None:
                try:
                    kind = ship_meta.kind(ship)
                except Exception:
                    kind = "unknown"
            if kind == "fighter":
                pilot_to_fighters.setdefault(p, set()).add(ship)
            elif kind == "drone":
                # ignore drone 'ship types' in roster
                pilot_to_ships.setdefault(p, set())
            else:
                ships.add(ship)
                pilot_to_ships.setdefault(p, set()).add(ship)
        else:
            pilot_to_ships.setdefault(p, set())
            pilot_to_fighters.setdefault(p, set())

    for r in rows:
        lf = (r.get("log_file", "") or "").strip()
        if lf:
            log_files.add(lf)

        sp = (r.get("source_pilot", "") or "").strip()
        tp = (r.get("target_pilot", "") or "").strip()
        ll = (r.get("log_listener", "") or "").strip()

        _add_pilot(
            sp,
            (r.get("source_corp", "") or "").strip(),
            (r.get("source_alliance", "") or "").strip(),
            (r.get("source_ship_type", "") or "").strip(),
        )
        _add_pilot(
            tp,
            (r.get("target_corp", "") or "").strip(),
            (r.get("target_alliance", "") or "").strip(),
            (r.get("target_ship_type", "") or "").strip(),
        )
        # Listener is a pilot, but may not have corp/alliance/ship on this row.
        _add_pilot(ll)

    # Window
    try:
        start_dt = win.start
        end_dt = win.end
        # User-preferred summary date format: dd-mm-yyyy
        start = start_dt.strftime("%d-%m-%Y %H:%M:%S")
        end = end_dt.strftime("%d-%m-%Y %H:%M:%S")
        dur_s = int((end_dt - start_dt).total_seconds())
        start_compact = start_dt.strftime("%Y%m%d_%H%M%S")
        end_compact = end_dt.strftime("%Y%m%d_%H%M%S")
    except Exception:
        start = str(win.start)
        end = str(win.end)
        dur_s = 0
        start_compact = ""
        end_compact = ""

    # -------------------- Pilot ship sessions (Option 1) -------------------
    # Track first/last seen times per (pilot, ship_type) so Pilot_list can keep a
    # single primary hull while still preserving reships.
    from datetime import datetime

    def _parse_ts(s: str) -> datetime | None:
        s = (s or "").strip()
        if not s:
            return None
        # Typical: "2026.01.15 23:55:34"
        try:
            return datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
        except Exception:
            return None

    pilot_ship_sessions: Dict[tuple[str, str], Dict[str, Any]] = {}

    def _consider_session(pilot: str, ship: str, ts_s: str) -> None:
        pilot = (pilot or "").strip()
        ship = (ship or "").strip()
        if not pilot or not ship:
            return
        # Filter drones from hull sessions; keep fighters separate.
        if ship_meta is not None:
            try:
                k = ship_meta.kind(ship)
            except Exception:
                k = "unknown"
            if k in ("drone", "fighter"):
                return
        t = _parse_ts(ts_s)
        if t is None:
            return
        key = (pilot, ship)
        rec = pilot_ship_sessions.setdefault(key, {"first": t, "last": t, "count": 0})
        if t < rec["first"]:
            rec["first"] = t
        if t > rec["last"]:
            rec["last"] = t
        rec["count"] += 1

    for r in rows:
        ts_s = str(r.get("timestamp") or "").strip()
        for side in ("source", "target"):
            p = (r.get(f"{side}_pilot") or "").strip()
            if not p:
                continue
            if item_names_lower is not None and looks_like_drone_or_item(p, item_names_lower):
                continue
            sh = (r.get(f"{side}_ship_type") or "").strip()
            _consider_session(p, sh, ts_s)

    n_rows = len(rows)

    # -------------------- TXT --------------------
    lines = [
        f"Fight {fight_i:03d}",
        f"Window: {start} -> {end} (duration {dur_s}s)",
        f"Rows: {n_rows}",
        f"Unique pilots (incl. listener): {len(pilots)}",
        f"Unique alliances: {len(alliances)}",
        f"Unique corps: {len(corps)}",
        f"Unique ship types: {len(ships)}",
        f"Log files contributing: {len(log_files)}",
    ]

    if counts:
        lines.append("")
        lines.append("Outputs (rows):")
        for k in sorted(counts.keys()):
            lines.append(f"- {k}: {counts[k]}")

    if log_files:
        lines.append("")
        lines.append("Files: " + ", ".join(sorted(log_files)))

    (summary_dir / "fight_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # -------------------- JSON -------------------
    summary: Dict[str, Any] = {
        "fight_id": fight_i,
        "window_start": start,
        "window_end": end,
        "window_start_compact": start_compact,
        "window_end_compact": end_compact,
        "duration_seconds": dur_s,
        "rows": n_rows,
        "unique_pilots": len(pilots),
        "unique_alliances": len(alliances),
        "unique_corps": len(corps),
        "unique_ship_types": len(ships),
        "log_files": sorted(log_files),
        "counts": counts or {},
    }

    import json

    (summary_dir / "fight_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # -------------------- CSV summary (1 row) -------------------
    # Keep stable columns; flatten counts into count__<name>
    csv_cols = list(FIGHT_SUMMARY_HEADERS_BASE)
    counts_flat = {}
    for k, v in (counts or {}).items():
        ck = "count__" + str(k)
        counts_flat[ck] = int(v)
    csv_cols.extend(sorted(counts_flat.keys()))
    csv_cols = _append_metadata_headers(csv_cols)

    csv_row = {
        "fight_id": fight_i,
        "window_start": start,
        "window_end": end,
        "duration_seconds": dur_s,
        "rows": n_rows,
        "unique_pilots": len(pilots),
        "unique_alliances": len(alliances),
        "unique_corps": len(corps),
        "unique_ship_types": len(ships),
        "log_files": ";".join(sorted(log_files)),
        **counts_flat,
        **meta,
    }

    import csv

    with (summary_dir / "fight_summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_cols)
        w.writeheader()
        w.writerow({k: csv_row.get(k, "") for k in csv_cols})

    # -------------------- Roster CSV -------------------
    # Pilot_list includes ship meta + aggregated stats (Suggestion A)
    # Pilot_list: keep a single primary hull in ship_types. Preserve reships in
    # summary/Pilot_ship_sessions.csv.
    roster_cols_base = list(PILOT_LIST_HEADERS_BASE)
    roster_rows = []
    for p in sorted(pilots, key=str.lower):
        a = ",".join(sorted(pilot_to_alliances.get(p, set()), key=str.lower))
        c = ",".join(sorted(pilot_to_corps.get(p, set()), key=str.lower))
        ships_list = sorted(pilot_to_ships.get(p, set()), key=str.lower)
        fighters_list = sorted(pilot_to_fighters.get(p, set()), key=str.lower)

        # Choose a stable primary ship for this pilot (earliest first_seen).
        primary_ship = ""
        best_first = None
        for sh in ships_list:
            rec = pilot_ship_sessions.get((p, sh))
            if not rec:
                continue
            t = rec.get("first")
            if t is None:
                continue
            if best_first is None or t < best_first:
                best_first = t
                primary_ship = sh
        if not primary_ship and ships_list:
            primary_ship = ships_list[0]

        ship_classes = []
        ship_techs = []
        hull_rarities = []
        if ships_list and ship_meta is not None:
            for sh in ships_list:
                try:
                    cls, tech, rarity = ship_meta.resolve_extended(sh)
                except Exception:
                    cls, tech, rarity = "", "", ""
                if cls and cls not in ship_classes:
                    ship_classes.append(cls)
                if tech and tech not in ship_techs:
                    ship_techs.append(tech)
                if rarity and rarity not in hull_rarities:
                    hull_rarities.append(rarity)

        roster_rows.append({
            "pilot": p,
            "alliance": a,
            "corp": c,
            "ship_types": primary_ship,
            "ship_types_seen": ",".join(ships_list),
            "ship_types_seen_count": len(ships_list),
            "fighter_types": ",".join(fighters_list),
            "ship_classes": ",".join(ship_classes),
            "ship_tech_levels": ",".join(ship_techs),
            "hull_rarities": ",".join(hull_rarities),
        })

    # Aggregated pilot stats (per dataset)
    datasets = [
        "damage_done",
        "damage_received",
        "repairs_done",
        "repairs_received",
        "ewar_effects_done",
        "ewar_effects_received",
        "cap_warfare_done",
        "cap_warfare_received",
        "capacitor_done",
        "capacitor_received",
        "propulsion_jam_attempts",
    ]

    pilot_stats = {p: {d: {"count": 0, "total": 0, "value_count": 0} for d in datasets} for p in pilots}

    def _as_int(v):
        try:
            if v is None or v == "":
                return None
            if isinstance(v, int):
                return v
            vs = str(v).strip()
            if vs.isdigit() or (vs.startswith("-") and vs[1:].isdigit()):
                return int(vs)
        except Exception:
            return None
        return None

    for r in rows:
        ds = str(r.get("dataset") or "").strip()
        if ds not in datasets:
            continue
        amt = _as_int(r.get("amount"))

        if ds in ("damage_done", "repairs_done", "ewar_effects_done", "cap_warfare_done", "capacitor_done"):
            p = (r.get("source_pilot") or "").strip()
            if p in pilot_stats:
                pilot_stats[p][ds]["count"] += 1
                if amt is not None:
                    pilot_stats[p][ds]["total"] += amt
                    pilot_stats[p][ds]["value_count"] += 1
        elif ds in ("damage_received", "repairs_received", "ewar_effects_received", "cap_warfare_received", "capacitor_received"):
            p = (r.get("target_pilot") or "").strip()
            if p in pilot_stats:
                pilot_stats[p][ds]["count"] += 1
                if amt is not None:
                    pilot_stats[p][ds]["total"] += amt
                    pilot_stats[p][ds]["value_count"] += 1
        elif ds == "propulsion_jam_attempts":
            for side in ("source_pilot", "target_pilot"):
                p = (r.get(side) or "").strip()
                if p in pilot_stats:
                    pilot_stats[p][ds]["count"] += 1

    # Attach stats columns to roster_rows
    stat_cols = []
    for ds in datasets:
        stat_cols.extend([f"{ds}_count", f"{ds}_total", f"{ds}_avg"])

    roster_cols = roster_cols_base + stat_cols
    roster_cols = _append_metadata_headers(roster_cols)

    for rr in roster_rows:
        p = rr.get("pilot")
        sdict = pilot_stats.get(p, {})
        for ds in datasets:
            rr[f"{ds}_count"] = int(sdict.get(ds, {}).get("count") or 0)
            if ds == "propulsion_jam_attempts":
                rr[f"{ds}_total"] = ""
                rr[f"{ds}_avg"] = ""
            else:
                tot = int(sdict.get(ds, {}).get("total") or 0)
                vc = int(sdict.get(ds, {}).get("value_count") or 0)
                rr[f"{ds}_total"] = tot if vc > 0 else ""
                rr[f"{ds}_avg"] = (tot / vc) if vc > 0 else ""

    # Instance summary (Suggestion A)
    def _write_instance_summary(out_dir):
        import csv
        import re

        def _norm_mod(v: Any) -> str:
            s = str(v or "").strip()
            # Strip leading dash/space artifacts ("- Module", "- - Module")
            s2 = re.sub(r"^[\s\-\u2013\u2014]+", "", s).strip()
            s2 = re.sub(r"\s+", " ", s2)
            # If SDE is available and the cleaned name exists, use it.
            if item_names_lower is not None and s2 and (s2.lower() in item_names_lower):
                return s2
            return s2 or s
        inst = {}
        for r in rows:
            ds = str(r.get("dataset") or "").strip()
            res = str(r.get("result") or "").strip()
            mod = _norm_mod(r.get("module"))
            key = (ds, res, mod)
            inst.setdefault(key, {"count": 0, "total": 0, "value_count": 0})
            inst[key]["count"] += 1
            amt = _as_int(r.get("amount"))
            if amt is not None:
                inst[key]["total"] += amt
                inst[key]["value_count"] += 1

        cols = list(INSTANCE_SUMMARY_HEADERS)
        cols = _append_metadata_headers(cols)
        with (out_dir / "Instance_Summary.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for (ds, res, mod), v in sorted(inst.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
                vc = int(v.get("value_count") or 0)
                tot = int(v.get("total") or 0)
                row = {
                    "dataset": ds,
                    "result": res,
                    "module": mod,
                    "count": int(v.get("count") or 0),
                    "total_amount": tot if vc > 0 else "",
                    "avg_amount": (tot / vc) if vc > 0 else "",
                }
                row.update(meta)
                w.writerow(row)

    _write_instance_summary(summary_dir)

    # User-requested rename: fight_roster -> Pilot_list
    with (summary_dir / "Pilot_list.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=roster_cols)
        w.writeheader()
        for rr in roster_rows:
            row = dict(rr)
            row.update(meta)
            w.writerow(row)

    # -------------------- Pilot_ship_sessions.csv (Option 1) -------------------
    # One row per (pilot, ship) showing first/last seen. Drones are excluded;
    # fighters are tracked in Pilot_list.fighter_types.
    sess_cols = list(PILOT_SHIP_SESSIONS_HEADERS)
    sess_cols = _append_metadata_headers(sess_cols)
    with (summary_dir / "Pilot_ship_sessions.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=sess_cols)
        w.writeheader()
        # deterministic ordering: pilot, first_seen, ship_type
        items = []
        for (p, sh), rec in pilot_ship_sessions.items():
            first = rec.get("first")
            last = rec.get("last")
            items.append((p, first, sh, last, rec))
        for p, first, sh, last, rec in sorted(items, key=lambda t: (t[0].lower(), (t[1] or start_dt), t[2].lower())):
            a = ",".join(sorted(pilot_to_alliances.get(p, set()), key=str.lower))
            c = ",".join(sorted(pilot_to_corps.get(p, set()), key=str.lower))
            cls = tech = rarity = ""
            if ship_meta is not None:
                try:
                    cls, tech, rarity = ship_meta.resolve_extended(sh)
                except Exception:
                    cls, tech, rarity = "", "", ""
            if first is None or last is None:
                continue
            row = {
                "pilot": p,
                "alliance": a,
                "corp": c,
                "ship_type": sh,
                "ship_class": cls,
                "ship_tech": tech,
                "hull_rarity": rarity,
                "first_seen": first.strftime("%d-%m-%Y %H:%M:%S"),
                "last_seen": last.strftime("%d-%m-%Y %H:%M:%S"),
                "duration_s": int((last - first).total_seconds()),
                "seen_events_count": int(rec.get("count") or 0),
            }
            row.update(meta)
            w.writerow(row)

    # -------------------- Alliance-corp_list CSV -------------------
    # Count unique pilots per (alliance, corp) with best-effort primary affiliation.
    pilot_corp_counts: Dict[str, Dict[str, int]] = {}
    pilot_all_counts: Dict[str, Dict[str, int]] = {}

    def _bump(m: Dict[str, Dict[str, int]], pilot: str, key: str) -> None:
        if not pilot or not key:
            return
        m.setdefault(pilot, {})
        m[pilot][key] = m[pilot].get(key, 0) + 1

    for r in rows:
        for side in ("source", "target"):
            p = (r.get(f"{side}_pilot") or "").strip()
            if not p:
                continue
            if item_names_lower is not None and looks_like_drone_or_item(p, item_names_lower):
                continue
            _bump(pilot_corp_counts, p, (r.get(f"{side}_corp") or "").strip() or "UNKNOWN")
            _bump(pilot_all_counts, p, (r.get(f"{side}_alliance") or "").strip() or "UNKNOWN")

    def _primary(counts: Dict[str, int]) -> str:
        if not counts:
            return "UNKNOWN"
        # pick highest count, tie-break alphabetically for determinism
        return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))[0][0]

    bucket: Dict[tuple[str, str], set[str]] = {}
    for p in pilots:
        if item_names_lower is not None and looks_like_drone_or_item(p, item_names_lower):
            continue
        corp = _primary(pilot_corp_counts.get(p, {}))
        allc = _primary(pilot_all_counts.get(p, {}))
        bucket.setdefault((allc, corp), set()).add(p)

    ac_cols = list(ALLIANCE_CORP_LIST_HEADERS)
    ac_cols = _append_metadata_headers(ac_cols)
    with (summary_dir / "Alliance-corp_list.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ac_cols)
        w.writeheader()
        for (allc, corp), ps in sorted(bucket.items(), key=lambda kv: (kv[0][0].lower(), kv[0][1].lower())):
            row = {
                "alliance": allc,
                "corp": corp,
                "pilot_count": len(ps),
                "pilots": ";".join(sorted(ps, key=str.lower)),
            }
            row.update(meta)
            w.writerow(row)

    # fight_lists.csv removed (redundant with fight_roster.csv).


def _iter_log_files(folder: Path) -> List[Path]:
    return sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".txt"])


def _safe_folder_name(name: str) -> str:
    """Make a folder name safe across platforms (especially Windows)."""
    # Replace path separators and trim.
    s = name.strip().replace("/", "_").replace("\\", "_")
    # Collapse whitespace to underscores.
    s = "_".join(s.split())
    # Remove characters Windows dislikes in folder names.
    bad = '<>:"|?*'
    s = "".join(ch for ch in s if ch not in bad)
    return s or "logs"


def _next_run_id(output_root: Path, base_name: str) -> int:
    """Return the next 1-based run id for a given base name.

    Looks for folders like: 001_<base_name>, 002_<base_name>, ...
    """
    if not output_root.exists():
        return 1

    max_id = 0
    prefix_marker = "_" + base_name.lower()
    for p in output_root.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        # Expected: NNN_<base>
        if len(name) < 5:
            continue
        if name[3:4] != "_":
            continue
        if not name[:3].isdigit():
            continue
        if not name.lower().endswith(prefix_marker):
            continue
        max_id = max(max_id, int(name[:3]))
    return max_id + 1


def _count_txt(folder: Path) -> int:
    try:
        return sum(1 for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".txt")
    except FileNotFoundError:
        return 0


def _choose_log_subfolder(
    log_root: Path,
    prompter: Prompter,
    *,
    preferred_name: str | None = None,
    preferred_index: int | None = None,
) -> Path:
    """Pick a subfolder under log_root.

    Behavior:
    - If log_root contains .txt files, that is a valid option.
    - Any immediate subfolder containing at least 1 .txt is also a valid option.
    - If there is exactly one valid option, it is chosen automatically.
    - If multiple valid options exist, the user is prompted to select.
    """

    candidates: List[Path] = []

    # Root itself
    if _count_txt(log_root) > 0:
        candidates.append(log_root)

    # Immediate subfolders
    for p in sorted([d for d in log_root.iterdir() if d.is_dir()], key=lambda x: x.name.lower()):
        if _count_txt(p) > 0:
            candidates.append(p)

    if not candidates:
        return log_root

    # Direct selection by name
    if preferred_name:
        for c in candidates:
            if c.name.lower() == preferred_name.lower():
                return c

    # Direct selection by index (1-based)
    if preferred_index is not None:
        if 1 <= preferred_index <= len(candidates):
            return candidates[preferred_index - 1]

    if len(candidates) == 1:
        return candidates[0]

    # Interactive selection
    lines: List[str] = []
    for i, c in enumerate(candidates, start=1):
        label = "(root)" if c == log_root else c.name
        n = _count_txt(c)
        lines.append(f"{i}. folder name: {label} - contains {n} .txt files")

    msg = "Select which log folder to use:\n" + "\n".join(lines) + "\n\nEnter a number:"  # noqa: E501

    if prompter.config.non_interactive:
        raise SystemExit(
            "Multiple log subfolders found, but --non-interactive is enabled. "
            "Use --subfolder NAME or --folder-index N."
        )

    while True:
        try:
            raw = input(msg + " ").strip()
        except EOFError:
            raise SystemExit("No selection provided.")

        if not raw:
            # default: first option (alphabetical because candidates are sorted)
            return candidates[0]

        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(candidates):
                return candidates[idx - 1]

        print(f"Invalid selection '{raw}'. Please enter 1..{len(candidates)}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Parse EVE logs -> repairs/damage/ewar CSVs + others.csv")
    p.add_argument("--version", action="version", version=f"eve_combat_parser {__version__}")

    p.add_argument(
        "--log-folder",
        default=DEFAULT_LOG_FOLDER,
        help=(
            "Root folder that contains EVE .txt log files, or subfolders with .txt logs. "
            "If the folder contains multiple subfolders with logs, you will be prompted to choose."
        ),
    )
    p.add_argument(
        "--subfolder",
        default=None,
        help="Select a specific subfolder name under --log-folder (skips the selection prompt).",
    )
    p.add_argument(
        "--folder-index",
        type=int,
        default=None,
        help="Select folder by the prompt number (1-based). Useful with --non-interactive.",
    )
    p.add_argument(
        "--output-folder",
        default="./output",
        help=(
            "Root output folder. By default, the tool creates a per-run subfolder inside it, e.g. "
            "./output/001_FIGHTNAME/ (sorted by run id)."
        ),
    )
    p.add_argument(
        "--no-run-folder",
        action="store_true",
        help="Write CSVs directly into --output-folder (old behavior).",
    )

    p.add_argument(
        "--no-open",
        action="store_true",
        help="Do not auto-open the output folder when the run completes.",
    )

    p.add_argument("--no-esi", action="store_true", help="Disable ESI fallback and ESI affiliation learning")
    p.add_argument("--cache-file", default=DEFAULT_CACHE_FILE, help="ESI cache JSON")
    p.add_argument("--aff-log-db-file", default=DEFAULT_AFF_LOG_DB_FILE, help="Aff DB from logs")
    p.add_argument("--aff-esi-db-file", default=DEFAULT_AFF_ESI_DB_FILE, help="Aff DB from ESI")
    p.add_argument("--sleep", type=float, default=DEFAULT_ESI_SLEEP, help="Sleep between ESI calls")
    p.add_argument("--sde-dir", default=DEFAULT_SDE_DIR, help="SDE folder")

    p.add_argument(
        "--yes",
        action="store_true",
        help="Auto-accept prompts (also auto-downloads missing SDE files)",
    )
    p.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail instead of prompting (useful for unattended runs)",
    )

    return p


def main(argv: List[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)

    log_folder = Path(args.log_folder)
    out_root = Path(args.output_folder)

    # UX: create the logs folder automatically (common first-run case)
    if not log_folder.exists():
        try:
            log_folder.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"ERROR: Could not create log folder '{log_folder}': {e}")
            return 2

    if not log_folder.is_dir():
        print(f"ERROR: log folder is not a directory: {log_folder}")
        return 2

    # UX: if there are no .txt files anywhere under log_folder, give a helpful hint and exit.
    def _has_any_txt_logs(root: Path) -> bool:
        try:
            for p in root.rglob("*.txt"):
                if p.is_file():
                    return True
        except Exception:
            return False
        return False

    if not _has_any_txt_logs(log_folder):
        print(f"No log files found in {log_folder}\n")
        print("Please copy your EVE Online combat log (.txt) files into:")
        print(f"  {log_folder}/<any_subfolder_name>/\n")
        print("Example:")
        print(f"  {log_folder}/testfight/20260115_225941_548047231.txt")
        return 0

    out_root.mkdir(parents=True, exist_ok=True)

    prompter = Prompter(PromptConfig(assume_yes=bool(args.yes), non_interactive=bool(args.non_interactive)))

    # If the root has no .txt files but contains subfolders that do, offer a selection.
    # This makes it easy to keep a stable "logs" root with multiple fights inside.
    chosen_folder = _choose_log_subfolder(
        log_folder,
        prompter,
        preferred_name=getattr(args, "subfolder", None),
        preferred_index=getattr(args, "folder_index", None),
    )

    # Output folder: ./output/<run_id>_<chosen_folder_name>/
    # This makes repeated runs non-destructive by default.
    if getattr(args, "no_run_folder", False):
        out_folder = out_root
        run_id_str = "0"
    else:
        base = _safe_folder_name(chosen_folder.name if chosen_folder != log_folder else log_folder.name)
        run_id = _next_run_id(out_root, base)
        run_id_str = f"{run_id:03d}"
        out_folder = out_root / f"{run_id_str}_{base}"

    metadata = {
        "schema_version": SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "run_id": run_id_str,
    }
    write_csv = functools.partial(  # type: ignore[assignment]
        exporter.write_csv,
        metadata=metadata,
        metadata_headers=METADATA_HEADERS,
    )

    out_folder.mkdir(parents=True, exist_ok=True)
    log_paths = _iter_log_files(chosen_folder)
    if not log_paths:
        print(f"No .txt log files found in '{chosen_folder}'.")
        return 0

    if chosen_folder != log_folder:
        print(f"Using log subfolder: {chosen_folder}")
    print(f"Output folder: {out_folder}")

    print("Found the following log files:")
    for fn in log_paths:
        print(f" - {fn.name}")

    if not prompter.confirm("Continue analysis?", default=True):
        print("Analysis cancelled.")
        return 0

    # SDE (for item-name filter)
    ensure_sde_present(str(args.sde_dir), prompter)
    item_names_lower = load_item_name_set(str(args.sde_dir))

    # Aff DBs prompts
    base_aff_log, save_aff_log = maybe_reset_aff_db(str(args.aff_log_db_file), "affiliations_from_logs", prompter)
    base_aff_esi, save_aff_esi = maybe_reset_aff_db(str(args.aff_esi_db_file), "affiliations_from_esi", prompter)

    # Timeline + update affiliations_from_logs
    ship_timeline, aff_log = build_ship_timeline_and_afflog(log_paths, base_aff_log)
    if save_aff_log and args.aff_log_db_file:
        save_aff_db(str(args.aff_log_db_file), aff_log)
        print(f"Saved affiliations_from_logs: {args.aff_log_db_file} ({len(aff_log)} corp->alliance mappings)")
    else:
        print(f"affiliations_from_logs mappings (in-memory): {len(aff_log)}")

    # affiliations_from_esi base
    aff_esi = dict(base_aff_esi)
    print(f"affiliations_from_esi mappings loaded: {len(aff_esi)}")

    # Parse rows
    others_rows: List[Dict[str, Any]] = []
    repairs_done_rows: List[Dict[str, Any]] = []
    repairs_received_rows: List[Dict[str, Any]] = []
    damage_done_rows: List[Dict[str, Any]] = []
    damage_received_rows: List[Dict[str, Any]] = []
    # EWAR split: effects vs capacitor warfare
    ewar_effects_done_rows: List[Dict[str, Any]] = []
    ewar_effects_received_rows: List[Dict[str, Any]] = []
    cap_warfare_done_rows: List[Dict[str, Any]] = []
    cap_warfare_received_rows: List[Dict[str, Any]] = []

        # Propulsion jamming attempts (warp scramble/disruption attempt)
    propulsion_jam_attempt_rows: List[Dict[str, Any]] = []

# Remote capacitor transfer (positive)
    capacitor_done_rows: List[Dict[str, Any]] = []
    capacitor_received_rows: List[Dict[str, Any]] = []

    for p in log_paths:
        parsed = parse_log_file_to_rows(p, ship_timeline, others_rows, item_names_lower=item_names_lower)
        repairs_done_rows.extend(parsed["repairs_done"])
        repairs_received_rows.extend(parsed["repairs_received"])
        damage_done_rows.extend(parsed["damage_done"])
        damage_received_rows.extend(parsed["damage_received"])
        ewar_effects_done_rows.extend(parsed["ewar_effects_done"])
        ewar_effects_received_rows.extend(parsed["ewar_effects_received"])
        cap_warfare_done_rows.extend(parsed["cap_warfare_done"])
        cap_warfare_received_rows.extend(parsed["cap_warfare_received"])
        capacitor_done_rows.extend(parsed["capacitor_done"])
        capacitor_received_rows.extend(parsed["capacitor_received"])
        propulsion_jam_attempt_rows.extend(parsed.get("propulsion_jam_attempts", []))

    all_combat_rows = (
        repairs_done_rows
        + repairs_received_rows
        + damage_done_rows
        + damage_received_rows
        + ewar_effects_done_rows
        + ewar_effects_received_rows
        + cap_warfare_done_rows
        + cap_warfare_received_rows
        + capacitor_done_rows
        + capacitor_received_rows
        + propulsion_jam_attempt_rows
    )

    # --------------------------------------------------------------
    # Split into fights
    #
    # Logs can span multiple fights and pilots can swap ships between
    # fights. We split output into separate "fight" folders using a
    # simple inactivity heuristic: if more than 15 minutes pass between
    # any combat events (damage/ewar/rep), we start a new fight.
    # --------------------------------------------------------------

    fight_windows = split_rows_into_fights(all_combat_rows, gap_minutes=15)
    if not fight_windows:
        print("No combat events found.")
        return 0

    # Persistent pilot DB: used only for corp/alliance (NOT ship types)
    # to avoid cross-fight ship backfills.
    pilot_db_path = out_root / ".cache" / "pilots.json"
    pilot_db = load_pilot_db(str(pilot_db_path))
    pilot_db_updates_total = 0

    # ESI cache is shared across fights.
    if not args.no_esi:
        try:
            requests_import_guard()
        except SystemExit as e:
            print(str(e))
            return 2
        cache, save_cache_to_disk = maybe_reset_cache(str(args.cache_file), prompter)
    else:
        cache, save_cache_to_disk = ({}, False)

    # Ship class/tech resolver (primary: fuzzwork invTypes, redundancy: ESI)
    ship_meta = ShipMetaResolver(
        sde_dir=str(args.sde_dir),
        cache=cache,
        enable_esi=not args.no_esi,
        esi_sleep_s=max(0.0, float(args.sleep)),
    )

    # Module tech/meta resolver (same SDE sources, plus dgmTypeAttributes for meta level)
    module_meta = ModuleMetaResolver(
        sde_dir=str(args.sde_dir),
        cache=cache,
        enable_esi=not args.no_esi,
        esi_sleep_s=max(0.0, float(args.sleep)),
    )

    def _apply_listener_state(rows: List[Dict[str, Any]], tl) -> None:
        """Overwrite listener (log_listener) ship/corp/alliance from fight timeline."""

        for r in rows:
            ts = r.get("timestamp", "")
            try:
                t = datetime.strptime(ts.strip(), TS_FMT)
            except Exception:
                continue
            listener = (r.get("log_listener", "") or "").strip()
            if not listener:
                continue
            for side in ("source", "target"):
                pilot = (r.get(f"{side}_pilot", "") or "").strip()
                if not pilot or pilot != listener:
                    continue
                ship, allc, corp = lookup_ship(tl, pilot, t)
                if ship:
                    r[f"{side}_ship_type"] = ship
                if allc:
                    r[f"{side}_alliance"] = allc
                if corp:
                    r[f"{side}_corp"] = corp

    def _annotate_fight_and_entity_kinds(
        rows: List[Dict[str, Any]],
        *,
        fight_id: str,
        item_names_lower,
    ) -> None:
        """Add explicit fight_id + entity kind columns to each combat row."""

        known_players = build_known_players(rows)
        for r in rows:
            r["fight_id"] = fight_id
            for side in ("source", "target"):
                pilot = str(r.get(f"{side}_pilot", "") or "")
                ship_type = str(r.get(f"{side}_ship_type", "") or "")
                corp = str(r.get(f"{side}_corp", "") or "")
                allc = str(r.get(f"{side}_alliance", "") or "")
                # Listener is always a player entity (unless it was mistakenly parsed
                # as an item), even when corp/alliance tickers are missing.
                listener = str(r.get("log_listener", "") or "")
                if listener and pilot.strip() == listener.strip() and not looks_like_drone_or_item(pilot, item_names_lower):
                    r[f"{side}_kind"] = "player"
                else:
                    r[f"{side}_kind"] = classify_party_kind(
                        pilot=pilot,
                        ship_type=ship_type,
                        corp=corp,
                        alliance=allc,
                        item_names_lower=item_names_lower,
                        known_players=known_players,
                    )

    def _annotate_ship_meta(rows: List[Dict[str, Any]]) -> None:
        """Add ship class/tech/rarity + module tech/meta level columns."""

        for r in rows:
            ship_meta.annotate_row(r)
            module_meta.annotate_row(r)

    def _dedupe_propulsion_jam_attempts(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge duplicate attempt rows across multiple logs.

        Dedup rule: same (attempt_type, module, source_pilot, target_pilot) where timestamps are within ±1 second.
        When merged, we keep one row and add:
          - origin_count
          - origin_log_files (semicolon)
          - origin_line_numbers (semicolon)

        Note: module must match (as requested).
        """
        from datetime import datetime

        def _parse_ts(ts: str) -> datetime:
            return datetime.strptime(ts, TS_FMT)

        # sort deterministically
        rows_sorted = sorted(rows, key=lambda r: (r.get("source_pilot",""), r.get("target_pilot",""), r.get("timestamp","")))
        out: List[Dict[str, Any]] = []
        for r in rows_sorted:
            ts = r.get("timestamp") or ""
            try:
                dt_r = _parse_ts(ts)
            except Exception:
                dt_r = None

            key = (
                (r.get("attempt_type") or ""),
                (r.get("module") or ""),
                (r.get("source_pilot") or ""),
                (r.get("target_pilot") or ""),
            )

            if not out:
                rr = dict(r)
                rr["origin_count"] = 1
                rr["origin_log_files"] = str(r.get("log_file") or "")
                rr["origin_line_numbers"] = str(r.get("Log_file_line_number") or "")
                out.append(rr)
                continue

            prev = out[-1]
            prev_key = (
                (prev.get("attempt_type") or ""),
                (prev.get("module") or ""),
                (prev.get("source_pilot") or ""),
                (prev.get("target_pilot") or ""),
            )
            if key != prev_key:
                rr = dict(r)
                rr["origin_count"] = 1
                rr["origin_log_files"] = str(r.get("log_file") or "")
                rr["origin_line_numbers"] = str(r.get("Log_file_line_number") or "")
                out.append(rr)
                continue

            # same key, check time proximity
            try:
                dt_prev = _parse_ts(prev.get("timestamp") or "")
            except Exception:
                dt_prev = None

            close = False
            if dt_r is not None and dt_prev is not None:
                close = abs((dt_r - dt_prev).total_seconds()) <= 1.0

            if close:
                prev["origin_count"] = int(prev.get("origin_count") or 1) + 1
                lf = str(r.get("log_file") or "")
                ln = str(r.get("Log_file_line_number") or "")
                prev["origin_log_files"] = ";".join([x for x in (prev.get("origin_log_files") or "").split(";") if x] + ([lf] if lf else []))
                prev["origin_line_numbers"] = ";".join([x for x in (prev.get("origin_line_numbers") or "").split(";") if x] + ([ln] if ln else []))
                # keep first Log_file_original_line as representative
            else:
                rr = dict(r)
                rr["origin_count"] = 1
                rr["origin_log_files"] = str(r.get("log_file") or "")
                rr["origin_line_numbers"] = str(r.get("Log_file_line_number") or "")
                out.append(rr)

        # de-dupe lists inside origin columns (preserve order)
        for rr in out:
            for col in ("origin_log_files", "origin_line_numbers"):
                seen=set()
                items=[]
                for x in (rr.get(col) or "").split(";"):
                    x=x.strip()
                    if not x or x in seen:
                        continue
                    seen.add(x)
                    items.append(x)
                rr[col] = ";".join(items)
        return out


    # Combined outputs (Option B): we always write per-fight CSVs, and additionally
    # write a combined set at the end of the run under "_combined/".
    combined: Dict[str, List[Dict[str, Any]]] = {
        OUT_REPAIRS_DONE_PLAYERS: [],
        OUT_REPAIRS_DONE_NPC: [],
        OUT_REPAIRS_DONE_DRONES: [],
        OUT_REPAIRS_DONE_CHARGES: [],
        OUT_REPAIRS_RECEIVED_PLAYERS: [],
        OUT_REPAIRS_RECEIVED_NPC: [],
        OUT_REPAIRS_RECEIVED_DRONES: [],
        OUT_REPAIRS_RECEIVED_CHARGES: [],
        OUT_DAMAGE_DONE_PLAYERS: [],
        OUT_DAMAGE_DONE_NPC: [],
        OUT_DAMAGE_DONE_DRONES: [],
        OUT_DAMAGE_DONE_CHARGES: [],
        OUT_DAMAGE_RECEIVED_PLAYERS: [],
        OUT_DAMAGE_RECEIVED_NPC: [],
        OUT_DAMAGE_RECEIVED_DRONES: [],
        OUT_DAMAGE_RECEIVED_CHARGES: [],
        # EWAR split
        OUT_EWAR_EFFECTS_DONE_PLAYERS: [],
        OUT_EWAR_EFFECTS_DONE_NPC: [],
        OUT_EWAR_EFFECTS_DONE_DRONES: [],
        OUT_EWAR_EFFECTS_DONE_CHARGES: [],
        OUT_EWAR_EFFECTS_RECEIVED_PLAYERS: [],
        OUT_EWAR_EFFECTS_RECEIVED_NPC: [],
        OUT_EWAR_EFFECTS_RECEIVED_DRONES: [],
        OUT_EWAR_EFFECTS_RECEIVED_CHARGES: [],

        OUT_CAP_WARFARE_DONE_PLAYERS: [],
        OUT_CAP_WARFARE_DONE_NPC: [],
        OUT_CAP_WARFARE_DONE_DRONES: [],
        OUT_CAP_WARFARE_DONE_CHARGES: [],
        OUT_CAP_WARFARE_RECEIVED_PLAYERS: [],
        OUT_CAP_WARFARE_RECEIVED_NPC: [],
        OUT_CAP_WARFARE_RECEIVED_DRONES: [],
        OUT_CAP_WARFARE_RECEIVED_CHARGES: [],

        # Remote capacitor transfer
        OUT_CAPACITOR_DONE_PLAYERS: [],
        OUT_CAPACITOR_DONE_NPC: [],
        OUT_CAPACITOR_DONE_DRONES: [],
        OUT_CAPACITOR_DONE_CHARGES: [],
        OUT_CAPACITOR_RECEIVED_PLAYERS: [],
        OUT_CAPACITOR_RECEIVED_NPC: [],
        OUT_CAPACITOR_RECEIVED_DRONES: [],
        OUT_CAPACITOR_RECEIVED_CHARGES: [],
        OUT_OTHERS: [],
        OUT_PROPULSION_JAM_ATTEMPTS: [],
    }

    def _fight_folder_label(fight_i: int, win) -> str:
        """User-requested fight folder format.

        Example: fight_001_15jan2026_21-30_TO_16jan2026_01-11
        """
        s = win.start
        e = win.end
        s_date = f"{s.day:02d}{MONTH_ABBR_LOWER.get(s.month, 'unk')}{s.year:04d}"
        e_date = f"{e.day:02d}{MONTH_ABBR_LOWER.get(e.month, 'unk')}{e.year:04d}"
        s_time = f"{s.hour:02d}-{s.minute:02d}"
        e_time = f"{e.hour:02d}-{e.minute:02d}"
        return f"fight_{fight_i:03d}_{s_date}_{s_time}_TO_{e_date}_{e_time}"

    # Process each fight independently.
    for fight_i, win in enumerate(fight_windows, start=1):
        fight_folder = out_folder / _fight_folder_label(fight_i, win)
        fight_folder.mkdir(parents=True, exist_ok=True)

        # Fight-scoped rows
        f_rep_done = filter_rows_by_window(repairs_done_rows, win)
        f_rep_recv = filter_rows_by_window(repairs_received_rows, win)
        f_dmg_done = filter_rows_by_window(damage_done_rows, win)
        f_dmg_recv = filter_rows_by_window(damage_received_rows, win)
        f_ewar_eff_done = filter_rows_by_window(ewar_effects_done_rows, win)
        f_ewar_eff_recv = filter_rows_by_window(ewar_effects_received_rows, win)
        f_cap_warf_done = filter_rows_by_window(cap_warfare_done_rows, win)
        f_cap_warf_recv = filter_rows_by_window(cap_warfare_received_rows, win)
        f_cap_done = filter_rows_by_window(capacitor_done_rows, win)
        f_cap_recv = filter_rows_by_window(capacitor_received_rows, win)
        f_others = filter_rows_by_window(others_rows, win)
        f_prop_jam = filter_rows_by_window(propulsion_jam_attempt_rows, win)

        # Dataset labels (used for summaries/stats)
        for r in f_rep_done:
            r["dataset"] = "repairs_done"
        for r in f_rep_recv:
            r["dataset"] = "repairs_received"
        for r in f_dmg_done:
            r["dataset"] = "damage_done"
        for r in f_dmg_recv:
            r["dataset"] = "damage_received"
        for r in f_ewar_eff_done:
            r["dataset"] = "ewar_effects_done"
        for r in f_ewar_eff_recv:
            r["dataset"] = "ewar_effects_received"
        for r in f_cap_warf_done:
            r["dataset"] = "cap_warfare_done"
        for r in f_cap_warf_recv:
            r["dataset"] = "cap_warfare_received"
        for r in f_cap_done:
            r["dataset"] = "capacitor_done"
        for r in f_cap_recv:
            r["dataset"] = "capacitor_received"
        for r in f_prop_jam:
            r["dataset"] = "propulsion_jam_attempts"

        f_prop_jam = _dedupe_propulsion_jam_attempts(f_prop_jam)

        fight_combat_rows = (
            f_rep_done
            + f_rep_recv
            + f_dmg_done
            + f_dmg_recv
            + f_ewar_eff_done
            + f_ewar_eff_recv
            + f_cap_warf_done
            + f_cap_warf_recv
            + f_cap_done
            + f_cap_recv
            + f_prop_jam
        )
        if not fight_combat_rows:
            continue

        # Fight-scoped ship timeline (prevents cross-fight ship backfill)
        tl_fight = restrict_timeline(ship_timeline, win.start, win.end)

        # Ensure listener ship/corp/alliance is filled only within this fight
        _apply_listener_state(fight_combat_rows, tl_fight)

        # --------------------------------------------------------------
        # Enrichment/backfill (fight-scoped)
        # --------------------------------------------------------------

        filled_persist = backfill_rows_from_db(pilot_db, fight_combat_rows, fill_ship=False)
        if filled_persist:
            print(f"Fight {fight_i}: filled {filled_persist} fields from persistent pilot DB (corp/alliance only).")

        tmp_db: Dict[str, Any] = {}
        learn_from_rows_excluding_items(tmp_db, fight_combat_rows, item_names_lower=item_names_lower, learn_ship=True)
        filled_tmp = backfill_rows_from_db(tmp_db, fight_combat_rows, fill_ship=True)
        if filled_tmp:
            print(f"Fight {fight_i}: filled {filled_tmp} fields from within-fight pilot cross-reference.")

        # Fill alliances from DBs before ESI
        fill_alliance_from_aff_db(fight_combat_rows, aff_log)
        fill_alliance_from_aff_db(fight_combat_rows, aff_esi)

        # ESI fallback per fight
        if not args.no_esi and fight_combat_rows:
            filled_fields, learned = enrich_missing_alliances_via_esi(
                fight_combat_rows,
                cache=cache,
                sleep_s=max(0.0, float(args.sleep)),
                item_names_lower=item_names_lower,
                aff_esi=aff_esi,
            )
            if filled_fields:
                print(f"Fight {fight_i}: ESI filled {filled_fields} alliance fields.")
            if learned:
                print(f"Fight {fight_i}: learned {learned} new corp->alliance mappings into affiliations_from_esi.")
                fill_alliance_from_aff_db(fight_combat_rows, aff_esi)

        # Backfill missing corp tickers when alliance is present (fight-scoped)
        pilot_to_corp, _pilot_to_all = build_pilot_ticker_maps(fight_combat_rows)
        filled_corps = fill_missing_corps_from_pilot_map(fight_combat_rows, pilot_to_corp)
        if filled_corps:
            fill_alliance_from_aff_db(fight_combat_rows, aff_log)
            fill_alliance_from_aff_db(fight_combat_rows, aff_esi)

        # Final within-fight pass
        final_tmp_db: Dict[str, Any] = {}
        learn_from_rows_excluding_items(
            final_tmp_db,
            fight_combat_rows,
            item_names_lower=item_names_lower,
            ship_meta=ship_meta,
            learn_ship=True,
        )
        backfill_rows_from_db(final_tmp_db, fight_combat_rows, fill_ship=True)

        # Update persistent pilot DB (corp/alliance only)
        pilot_db_updates_total += learn_from_rows_excluding_items(
            pilot_db,
            fight_combat_rows,
            item_names_lower=item_names_lower,
            ship_meta=ship_meta,
            learn_ship=False,
        )

        # Add stable identifiers for downstream analysis and for combined outputs.
        fight_id = fight_folder.name
        _annotate_fight_and_entity_kinds(fight_combat_rows, fight_id=fight_id, item_names_lower=item_names_lower)
        _annotate_ship_meta(fight_combat_rows)

        # Split and write outputs (per fight)
        rep_done_players, rep_done_npc, rep_done_drones, rep_done_charges = split_rows_players_npc_drones_charges(
            f_rep_done, other_prefix="target", item_names_lower=item_names_lower
        )
        rep_recv_players, rep_recv_npc, rep_recv_drones, rep_recv_charges = split_rows_players_npc_drones_charges(
            f_rep_recv, other_prefix="source", item_names_lower=item_names_lower
        )
        dmg_done_players, dmg_done_npc, dmg_done_drones, dmg_done_charges = split_rows_players_npc_drones_charges(
            f_dmg_done, other_prefix="target", item_names_lower=item_names_lower
        )
        dmg_recv_players, dmg_recv_npc, dmg_recv_drones, dmg_recv_charges = split_rows_players_npc_drones_charges(
            f_dmg_recv, other_prefix="source", item_names_lower=item_names_lower
        )
        # EWAR effects (ECM, damps, points, webs...) and cap warfare (neut/nos)
        ewar_eff_done_players, ewar_eff_done_npc, ewar_eff_done_drones, ewar_eff_done_charges = split_rows_players_npc_drones_charges(
            f_ewar_eff_done, other_prefix="target", item_names_lower=item_names_lower
        )
        ewar_eff_recv_players, ewar_eff_recv_npc, ewar_eff_recv_drones, ewar_eff_recv_charges = split_rows_players_npc_drones_charges(
            f_ewar_eff_recv, other_prefix="source", item_names_lower=item_names_lower
        )
        cap_warf_done_players, cap_warf_done_npc, cap_warf_done_drones, cap_warf_done_charges = split_rows_players_npc_drones_charges(
            f_cap_warf_done, other_prefix="target", item_names_lower=item_names_lower
        )
        cap_warf_recv_players, cap_warf_recv_npc, cap_warf_recv_drones, cap_warf_recv_charges = split_rows_players_npc_drones_charges(
            f_cap_warf_recv, other_prefix="source", item_names_lower=item_names_lower
        )

        cap_done_players, cap_done_npc, cap_done_drones, cap_done_charges = split_rows_players_npc_drones_charges(
            f_cap_done, other_prefix="target", item_names_lower=item_names_lower
        )
        cap_recv_players, cap_recv_npc, cap_recv_drones, cap_recv_charges = split_rows_players_npc_drones_charges(
            f_cap_recv, other_prefix="source", item_names_lower=item_names_lower
        )

        # Accumulate for combined outputs
        combined[OUT_REPAIRS_DONE_PLAYERS].extend(rep_done_players)
        combined[OUT_REPAIRS_DONE_NPC].extend(rep_done_npc)
        combined[OUT_REPAIRS_DONE_DRONES].extend(rep_done_drones)
        combined[OUT_REPAIRS_DONE_CHARGES].extend(rep_done_charges)
        combined[OUT_REPAIRS_RECEIVED_PLAYERS].extend(rep_recv_players)
        combined[OUT_REPAIRS_RECEIVED_NPC].extend(rep_recv_npc)
        combined[OUT_REPAIRS_RECEIVED_DRONES].extend(rep_recv_drones)
        combined[OUT_REPAIRS_RECEIVED_CHARGES].extend(rep_recv_charges)

        combined[OUT_DAMAGE_DONE_PLAYERS].extend(dmg_done_players)
        combined[OUT_DAMAGE_DONE_NPC].extend(dmg_done_npc)
        combined[OUT_DAMAGE_DONE_DRONES].extend(dmg_done_drones)
        combined[OUT_DAMAGE_DONE_CHARGES].extend(dmg_done_charges)
        combined[OUT_DAMAGE_RECEIVED_PLAYERS].extend(dmg_recv_players)
        combined[OUT_DAMAGE_RECEIVED_NPC].extend(dmg_recv_npc)
        combined[OUT_DAMAGE_RECEIVED_DRONES].extend(dmg_recv_drones)
        combined[OUT_DAMAGE_RECEIVED_CHARGES].extend(dmg_recv_charges)

        combined[OUT_EWAR_EFFECTS_DONE_PLAYERS].extend(ewar_eff_done_players)
        combined[OUT_EWAR_EFFECTS_DONE_NPC].extend(ewar_eff_done_npc)
        combined[OUT_EWAR_EFFECTS_DONE_DRONES].extend(ewar_eff_done_drones)
        combined[OUT_EWAR_EFFECTS_DONE_CHARGES].extend(ewar_eff_done_charges)
        combined[OUT_EWAR_EFFECTS_RECEIVED_PLAYERS].extend(ewar_eff_recv_players)
        combined[OUT_EWAR_EFFECTS_RECEIVED_NPC].extend(ewar_eff_recv_npc)
        combined[OUT_EWAR_EFFECTS_RECEIVED_DRONES].extend(ewar_eff_recv_drones)
        combined[OUT_EWAR_EFFECTS_RECEIVED_CHARGES].extend(ewar_eff_recv_charges)

        combined[OUT_CAP_WARFARE_DONE_PLAYERS].extend(cap_warf_done_players)
        combined[OUT_CAP_WARFARE_DONE_NPC].extend(cap_warf_done_npc)
        combined[OUT_CAP_WARFARE_DONE_DRONES].extend(cap_warf_done_drones)
        combined[OUT_CAP_WARFARE_DONE_CHARGES].extend(cap_warf_done_charges)
        combined[OUT_CAP_WARFARE_RECEIVED_PLAYERS].extend(cap_warf_recv_players)
        combined[OUT_CAP_WARFARE_RECEIVED_NPC].extend(cap_warf_recv_npc)
        combined[OUT_CAP_WARFARE_RECEIVED_DRONES].extend(cap_warf_recv_drones)
        combined[OUT_CAP_WARFARE_RECEIVED_CHARGES].extend(cap_warf_recv_charges)

        combined[OUT_CAPACITOR_DONE_PLAYERS].extend(cap_done_players)
        combined[OUT_CAPACITOR_DONE_NPC].extend(cap_done_npc)
        combined[OUT_CAPACITOR_DONE_DRONES].extend(cap_done_drones)
        combined[OUT_CAPACITOR_DONE_CHARGES].extend(cap_done_charges)
        combined[OUT_CAPACITOR_RECEIVED_PLAYERS].extend(cap_recv_players)
        combined[OUT_CAPACITOR_RECEIVED_NPC].extend(cap_recv_npc)
        combined[OUT_CAPACITOR_RECEIVED_DRONES].extend(cap_recv_drones)
        combined[OUT_CAPACITOR_RECEIVED_CHARGES].extend(cap_recv_charges)

        combined[OUT_OTHERS].extend(f_others)

        combined[OUT_PROPULSION_JAM_ATTEMPTS].extend(f_prop_jam)

        write_csv(fight_folder / OUT_REPAIRS_DONE_PLAYERS, rep_done_players, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_DONE_NPC, rep_done_npc, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_DONE_DRONES, rep_done_drones, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_DONE_CHARGES, rep_done_charges, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_RECEIVED_PLAYERS, rep_recv_players, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_RECEIVED_NPC, rep_recv_npc, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_RECEIVED_DRONES, rep_recv_drones, HEADERS)
        write_csv(fight_folder / OUT_REPAIRS_RECEIVED_CHARGES, rep_recv_charges, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_DONE_PLAYERS, dmg_done_players, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_DONE_NPC, dmg_done_npc, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_DONE_DRONES, dmg_done_drones, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_DONE_CHARGES, dmg_done_charges, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_RECEIVED_PLAYERS, dmg_recv_players, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_RECEIVED_NPC, dmg_recv_npc, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_RECEIVED_DRONES, dmg_recv_drones, HEADERS)
        write_csv(fight_folder / OUT_DAMAGE_RECEIVED_CHARGES, dmg_recv_charges, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_DONE_PLAYERS, ewar_eff_done_players, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_DONE_NPC, ewar_eff_done_npc, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_DONE_DRONES, ewar_eff_done_drones, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_DONE_CHARGES, ewar_eff_done_charges, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_RECEIVED_PLAYERS, ewar_eff_recv_players, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_RECEIVED_NPC, ewar_eff_recv_npc, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_RECEIVED_DRONES, ewar_eff_recv_drones, HEADERS)
        write_csv(fight_folder / OUT_EWAR_EFFECTS_RECEIVED_CHARGES, ewar_eff_recv_charges, HEADERS)

        write_csv(fight_folder / OUT_CAP_WARFARE_DONE_PLAYERS, cap_warf_done_players, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_DONE_NPC, cap_warf_done_npc, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_DONE_DRONES, cap_warf_done_drones, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_DONE_CHARGES, cap_warf_done_charges, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_RECEIVED_PLAYERS, cap_warf_recv_players, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_RECEIVED_NPC, cap_warf_recv_npc, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_RECEIVED_DRONES, cap_warf_recv_drones, HEADERS)
        write_csv(fight_folder / OUT_CAP_WARFARE_RECEIVED_CHARGES, cap_warf_recv_charges, HEADERS)

        write_csv(fight_folder / OUT_CAPACITOR_DONE_PLAYERS, cap_done_players, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_DONE_NPC, cap_done_npc, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_DONE_DRONES, cap_done_drones, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_DONE_CHARGES, cap_done_charges, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_RECEIVED_PLAYERS, cap_recv_players, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_RECEIVED_NPC, cap_recv_npc, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_RECEIVED_DRONES, cap_recv_drones, HEADERS)
        write_csv(fight_folder / OUT_CAPACITOR_RECEIVED_CHARGES, cap_recv_charges, HEADERS)
        write_csv(fight_folder / OUT_OTHERS, f_others, OTHERS_HEADERS)
        write_csv(fight_folder / OUT_PROPULSION_JAM_ATTEMPTS, f_prop_jam, PROP_JAM_HEADERS)

        # Per-fight run summary
        _write_fight_summary(
            fight_folder,
            fight_i,
            win,
            fight_combat_rows,
            item_names_lower=item_names_lower,
            ship_meta=ship_meta,
            metadata=metadata,
            counts={
                OUT_REPAIRS_DONE_PLAYERS: len(rep_done_players),
                OUT_REPAIRS_DONE_NPC: len(rep_done_npc),
                OUT_REPAIRS_DONE_DRONES: len(rep_done_drones),
                OUT_REPAIRS_DONE_CHARGES: len(rep_done_charges),
                OUT_REPAIRS_RECEIVED_PLAYERS: len(rep_recv_players),
                OUT_REPAIRS_RECEIVED_NPC: len(rep_recv_npc),
                OUT_REPAIRS_RECEIVED_DRONES: len(rep_recv_drones),
                OUT_REPAIRS_RECEIVED_CHARGES: len(rep_recv_charges),
                OUT_DAMAGE_DONE_PLAYERS: len(dmg_done_players),
                OUT_DAMAGE_DONE_NPC: len(dmg_done_npc),
                OUT_DAMAGE_DONE_DRONES: len(dmg_done_drones),
                OUT_DAMAGE_DONE_CHARGES: len(dmg_done_charges),
                OUT_DAMAGE_RECEIVED_PLAYERS: len(dmg_recv_players),
                OUT_DAMAGE_RECEIVED_NPC: len(dmg_recv_npc),
                OUT_DAMAGE_RECEIVED_DRONES: len(dmg_recv_drones),
                OUT_DAMAGE_RECEIVED_CHARGES: len(dmg_recv_charges),
                OUT_EWAR_EFFECTS_DONE_PLAYERS: len(ewar_eff_done_players),
                OUT_EWAR_EFFECTS_DONE_NPC: len(ewar_eff_done_npc),
                OUT_EWAR_EFFECTS_DONE_DRONES: len(ewar_eff_done_drones),
                OUT_EWAR_EFFECTS_DONE_CHARGES: len(ewar_eff_done_charges),
                OUT_EWAR_EFFECTS_RECEIVED_PLAYERS: len(ewar_eff_recv_players),
                OUT_EWAR_EFFECTS_RECEIVED_NPC: len(ewar_eff_recv_npc),
                OUT_EWAR_EFFECTS_RECEIVED_DRONES: len(ewar_eff_recv_drones),
                OUT_EWAR_EFFECTS_RECEIVED_CHARGES: len(ewar_eff_recv_charges),

                OUT_CAP_WARFARE_DONE_PLAYERS: len(cap_warf_done_players),
                OUT_CAP_WARFARE_DONE_NPC: len(cap_warf_done_npc),
                OUT_CAP_WARFARE_DONE_DRONES: len(cap_warf_done_drones),
                OUT_CAP_WARFARE_DONE_CHARGES: len(cap_warf_done_charges),
                OUT_CAP_WARFARE_RECEIVED_PLAYERS: len(cap_warf_recv_players),
                OUT_CAP_WARFARE_RECEIVED_NPC: len(cap_warf_recv_npc),
                OUT_CAP_WARFARE_RECEIVED_DRONES: len(cap_warf_recv_drones),
                OUT_CAP_WARFARE_RECEIVED_CHARGES: len(cap_warf_recv_charges),

                OUT_CAPACITOR_DONE_PLAYERS: len(cap_done_players),
                OUT_CAPACITOR_DONE_NPC: len(cap_done_npc),
                OUT_CAPACITOR_DONE_DRONES: len(cap_done_drones),
                OUT_CAPACITOR_DONE_CHARGES: len(cap_done_charges),
                OUT_CAPACITOR_RECEIVED_PLAYERS: len(cap_recv_players),
                OUT_CAPACITOR_RECEIVED_NPC: len(cap_recv_npc),
                OUT_CAPACITOR_RECEIVED_DRONES: len(cap_recv_drones),
                OUT_CAPACITOR_RECEIVED_CHARGES: len(cap_recv_charges),
                OUT_OTHERS: len(f_others),
                OUT_PROPULSION_JAM_ATTEMPTS: len(f_prop_jam),
            },
        )

        # --------------------------------------------------------------
        # Per-fight combined outputs (requested)
        # --------------------------------------------------------------
        fight_combined_folder = fight_folder / "_combined"
        fight_combined_folder.mkdir(parents=True, exist_ok=True)

        fight_sets: Dict[str, List[Dict[str, Any]]] = {
            OUT_REPAIRS_DONE_PLAYERS: rep_done_players,
            OUT_REPAIRS_DONE_NPC: rep_done_npc,
            OUT_REPAIRS_DONE_DRONES: rep_done_drones,
            OUT_REPAIRS_DONE_CHARGES: rep_done_charges,
            OUT_REPAIRS_RECEIVED_PLAYERS: rep_recv_players,
            OUT_REPAIRS_RECEIVED_NPC: rep_recv_npc,
            OUT_REPAIRS_RECEIVED_DRONES: rep_recv_drones,
            OUT_REPAIRS_RECEIVED_CHARGES: rep_recv_charges,
            OUT_DAMAGE_DONE_PLAYERS: dmg_done_players,
            OUT_DAMAGE_DONE_NPC: dmg_done_npc,
            OUT_DAMAGE_DONE_DRONES: dmg_done_drones,
            OUT_DAMAGE_DONE_CHARGES: dmg_done_charges,
            OUT_DAMAGE_RECEIVED_PLAYERS: dmg_recv_players,
            OUT_DAMAGE_RECEIVED_NPC: dmg_recv_npc,
            OUT_DAMAGE_RECEIVED_DRONES: dmg_recv_drones,
            OUT_DAMAGE_RECEIVED_CHARGES: dmg_recv_charges,
            OUT_EWAR_EFFECTS_DONE_PLAYERS: ewar_eff_done_players,
            OUT_EWAR_EFFECTS_DONE_NPC: ewar_eff_done_npc,
            OUT_EWAR_EFFECTS_DONE_DRONES: ewar_eff_done_drones,
            OUT_EWAR_EFFECTS_DONE_CHARGES: ewar_eff_done_charges,
            OUT_EWAR_EFFECTS_RECEIVED_PLAYERS: ewar_eff_recv_players,
            OUT_EWAR_EFFECTS_RECEIVED_NPC: ewar_eff_recv_npc,
            OUT_EWAR_EFFECTS_RECEIVED_DRONES: ewar_eff_recv_drones,
            OUT_EWAR_EFFECTS_RECEIVED_CHARGES: ewar_eff_recv_charges,
            OUT_CAP_WARFARE_DONE_PLAYERS: cap_warf_done_players,
            OUT_CAP_WARFARE_DONE_NPC: cap_warf_done_npc,
            OUT_CAP_WARFARE_DONE_DRONES: cap_warf_done_drones,
            OUT_CAP_WARFARE_DONE_CHARGES: cap_warf_done_charges,
            OUT_CAP_WARFARE_RECEIVED_PLAYERS: cap_warf_recv_players,
            OUT_CAP_WARFARE_RECEIVED_NPC: cap_warf_recv_npc,
            OUT_CAP_WARFARE_RECEIVED_DRONES: cap_warf_recv_drones,
            OUT_CAP_WARFARE_RECEIVED_CHARGES: cap_warf_recv_charges,
            OUT_CAPACITOR_DONE_PLAYERS: cap_done_players,
            OUT_CAPACITOR_DONE_NPC: cap_done_npc,
            OUT_CAPACITOR_DONE_DRONES: cap_done_drones,
            OUT_CAPACITOR_DONE_CHARGES: cap_done_charges,
            OUT_CAPACITOR_RECEIVED_PLAYERS: cap_recv_players,
            OUT_CAPACITOR_RECEIVED_NPC: cap_recv_npc,
            OUT_CAPACITOR_RECEIVED_DRONES: cap_recv_drones,
            OUT_CAPACITOR_RECEIVED_CHARGES: cap_recv_charges,
            OUT_PROPULSION_JAM_ATTEMPTS: f_prop_jam,
        }

        def _write_fight_combined(name: str, files: List[str]) -> None:
            all_rows: List[Dict[str, Any]] = []
            for f in files:
                for r in fight_sets.get(f, []) or []:
                    rr = dict(r)
                    rr["dataset"] = f.replace(".csv", "")
                    all_rows.append(rr)
            if all_rows:
                write_csv(fight_combined_folder / name, all_rows, ["dataset"] + HEADERS)

        _write_fight_combined(
            "combined_all_combat.csv",
            [
                OUT_REPAIRS_DONE_PLAYERS,
                OUT_REPAIRS_DONE_NPC,
                OUT_REPAIRS_DONE_DRONES,
                OUT_REPAIRS_DONE_CHARGES,
                OUT_REPAIRS_RECEIVED_PLAYERS,
                OUT_REPAIRS_RECEIVED_NPC,
                OUT_REPAIRS_RECEIVED_DRONES,
                OUT_REPAIRS_RECEIVED_CHARGES,
                OUT_DAMAGE_DONE_PLAYERS,
                OUT_DAMAGE_DONE_NPC,
                OUT_DAMAGE_DONE_DRONES,
                OUT_DAMAGE_DONE_CHARGES,
                OUT_DAMAGE_RECEIVED_PLAYERS,
                OUT_DAMAGE_RECEIVED_NPC,
                OUT_DAMAGE_RECEIVED_DRONES,
                OUT_DAMAGE_RECEIVED_CHARGES,
                OUT_EWAR_EFFECTS_DONE_PLAYERS,
                OUT_EWAR_EFFECTS_DONE_NPC,
                OUT_EWAR_EFFECTS_DONE_DRONES,
                OUT_EWAR_EFFECTS_DONE_CHARGES,
                OUT_EWAR_EFFECTS_RECEIVED_PLAYERS,
                OUT_EWAR_EFFECTS_RECEIVED_NPC,
                OUT_EWAR_EFFECTS_RECEIVED_DRONES,
                OUT_EWAR_EFFECTS_RECEIVED_CHARGES,
                OUT_CAP_WARFARE_DONE_PLAYERS,
                OUT_CAP_WARFARE_DONE_NPC,
                OUT_CAP_WARFARE_DONE_DRONES,
                OUT_CAP_WARFARE_DONE_CHARGES,
                OUT_CAP_WARFARE_RECEIVED_PLAYERS,
                OUT_CAP_WARFARE_RECEIVED_NPC,
                OUT_CAP_WARFARE_RECEIVED_DRONES,
                OUT_CAP_WARFARE_RECEIVED_CHARGES,
                OUT_CAPACITOR_DONE_PLAYERS,
                OUT_CAPACITOR_DONE_NPC,
                OUT_CAPACITOR_DONE_DRONES,
                OUT_CAPACITOR_DONE_CHARGES,
                OUT_CAPACITOR_RECEIVED_PLAYERS,
                OUT_CAPACITOR_RECEIVED_NPC,
                OUT_CAPACITOR_RECEIVED_DRONES,
                OUT_CAPACITOR_RECEIVED_CHARGES,
            ],
        )

        # Convenience per-category combined files.
        _write_fight_combined(
            "combined_damage.csv",
            [
                OUT_DAMAGE_DONE_PLAYERS,
                OUT_DAMAGE_DONE_NPC,
                OUT_DAMAGE_DONE_DRONES,
                OUT_DAMAGE_DONE_CHARGES,
                OUT_DAMAGE_RECEIVED_PLAYERS,
                OUT_DAMAGE_RECEIVED_NPC,
                OUT_DAMAGE_RECEIVED_DRONES,
                OUT_DAMAGE_RECEIVED_CHARGES,
            ],
        )
        _write_fight_combined(
            "combined_repairs.csv",
            [
                OUT_REPAIRS_DONE_PLAYERS,
                OUT_REPAIRS_DONE_NPC,
                OUT_REPAIRS_DONE_DRONES,
                OUT_REPAIRS_DONE_CHARGES,
                OUT_REPAIRS_RECEIVED_PLAYERS,
                OUT_REPAIRS_RECEIVED_NPC,
                OUT_REPAIRS_RECEIVED_DRONES,
                OUT_REPAIRS_RECEIVED_CHARGES,
            ],
        )
        _write_fight_combined(
            "combined_ewar_effects.csv",
            [
                OUT_EWAR_EFFECTS_DONE_PLAYERS,
                OUT_EWAR_EFFECTS_DONE_NPC,
                OUT_EWAR_EFFECTS_DONE_DRONES,
                OUT_EWAR_EFFECTS_DONE_CHARGES,
                OUT_EWAR_EFFECTS_RECEIVED_PLAYERS,
                OUT_EWAR_EFFECTS_RECEIVED_NPC,
                OUT_EWAR_EFFECTS_RECEIVED_DRONES,
                OUT_EWAR_EFFECTS_RECEIVED_CHARGES,
            ],
        )
        _write_fight_combined(
            "combined_cap_warfare.csv",
            [
                OUT_CAP_WARFARE_DONE_PLAYERS,
                OUT_CAP_WARFARE_DONE_NPC,
                OUT_CAP_WARFARE_DONE_DRONES,
                OUT_CAP_WARFARE_DONE_CHARGES,
                OUT_CAP_WARFARE_RECEIVED_PLAYERS,
                OUT_CAP_WARFARE_RECEIVED_NPC,
                OUT_CAP_WARFARE_RECEIVED_DRONES,
                OUT_CAP_WARFARE_RECEIVED_CHARGES,
            ],
        )
        _write_fight_combined(
            "combined_capacitor_transfers.csv",
            [
                OUT_CAPACITOR_DONE_PLAYERS,
                OUT_CAPACITOR_DONE_NPC,
                OUT_CAPACITOR_DONE_DRONES,
                OUT_CAPACITOR_DONE_CHARGES,
                OUT_CAPACITOR_RECEIVED_PLAYERS,
                OUT_CAPACITOR_RECEIVED_NPC,
                OUT_CAPACITOR_RECEIVED_DRONES,
                OUT_CAPACITOR_RECEIVED_CHARGES,
            ],
        )

        # Combined propulsion jamming attempts (deduped)
        write_csv(fight_combined_folder / "combined_propulsion_jamming_attempts.csv", f_prop_jam, PROP_JAM_HEADERS)


        # --------------------------------------------------------------
        # Player-specific folder exports (point 5)
        # --------------------------------------------------------------
        players_root = fight_folder / "Players"
        players_root.mkdir(parents=True, exist_ok=True)

        def _safe_fs_name(name: str) -> str:
            import re
            name = (name or "").strip()
            name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
            return name.strip("_") or "UNKNOWN"

        # Determine pilot roster from fight rows (exclude drones/charges/items)
        pilots_in_fight = set()
        for r in fight_combat_rows:
            for side in ("source", "target"):
                pp = (r.get(f"{side}_pilot") or "").strip()
                if not pp:
                    continue
                if item_names_lower is not None and looks_like_drone_or_item(pp, item_names_lower):
                    continue
                pilots_in_fight.add(pp)

        # Helper: write a player folder mirroring fight files, filtered to involvement.
        def _write_player_folder(pilot: str) -> None:
            pdir = players_root / _safe_fs_name(pilot)
            pdir.mkdir(parents=True, exist_ok=True)

            # Filter helpers
            def _src(rows):
                return [r for r in rows if (r.get("source_pilot") or "") == pilot]
            def _tgt(rows):
                return [r for r in rows if (r.get("target_pilot") or "") == pilot]
            def _either(rows):
                return [r for r in rows if (r.get("source_pilot") or "") == pilot or (r.get("target_pilot") or "") == pilot]

            # Write the same set of CSVs as the fight folder (where applicable)
            # Repairs
            write_csv(pdir / OUT_REPAIRS_DONE_PLAYERS, _src(rep_done_players), HEADERS)
            write_csv(pdir / OUT_REPAIRS_DONE_NPC, _src(rep_done_npc), HEADERS)
            write_csv(pdir / OUT_REPAIRS_DONE_DRONES, _src(rep_done_drones), HEADERS)
            write_csv(pdir / OUT_REPAIRS_DONE_CHARGES, _src(rep_done_charges), HEADERS)

            write_csv(pdir / OUT_REPAIRS_RECEIVED_PLAYERS, _tgt(rep_recv_players), HEADERS)
            write_csv(pdir / OUT_REPAIRS_RECEIVED_NPC, _tgt(rep_recv_npc), HEADERS)
            write_csv(pdir / OUT_REPAIRS_RECEIVED_DRONES, _tgt(rep_recv_drones), HEADERS)
            write_csv(pdir / OUT_REPAIRS_RECEIVED_CHARGES, _tgt(rep_recv_charges), HEADERS)

            # Damage
            write_csv(pdir / OUT_DAMAGE_DONE_PLAYERS, _src(dmg_done_players), HEADERS)
            write_csv(pdir / OUT_DAMAGE_DONE_NPC, _src(dmg_done_npc), HEADERS)
            write_csv(pdir / OUT_DAMAGE_DONE_DRONES, _src(dmg_done_drones), HEADERS)
            write_csv(pdir / OUT_DAMAGE_DONE_CHARGES, _src(dmg_done_charges), HEADERS)

            write_csv(pdir / OUT_DAMAGE_RECEIVED_PLAYERS, _tgt(dmg_recv_players), HEADERS)
            write_csv(pdir / OUT_DAMAGE_RECEIVED_NPC, _tgt(dmg_recv_npc), HEADERS)
            write_csv(pdir / OUT_DAMAGE_RECEIVED_DRONES, _tgt(dmg_recv_drones), HEADERS)
            write_csv(pdir / OUT_DAMAGE_RECEIVED_CHARGES, _tgt(dmg_recv_charges), HEADERS)

            # EWAR effects + cap warfare
            write_csv(pdir / OUT_EWAR_EFFECTS_DONE_PLAYERS, _src(ewar_eff_done_players), HEADERS)
            write_csv(pdir / OUT_EWAR_EFFECTS_DONE_NPC, _src(ewar_eff_done_npc), HEADERS)
            write_csv(pdir / OUT_EWAR_EFFECTS_DONE_DRONES, _src(ewar_eff_done_drones), HEADERS)
            write_csv(pdir / OUT_EWAR_EFFECTS_DONE_CHARGES, _src(ewar_eff_done_charges), HEADERS)

            write_csv(pdir / OUT_EWAR_EFFECTS_RECEIVED_PLAYERS, _tgt(ewar_eff_recv_players), HEADERS)
            write_csv(pdir / OUT_EWAR_EFFECTS_RECEIVED_NPC, _tgt(ewar_eff_recv_npc), HEADERS)
            write_csv(pdir / OUT_EWAR_EFFECTS_RECEIVED_DRONES, _tgt(ewar_eff_recv_drones), HEADERS)
            write_csv(pdir / OUT_EWAR_EFFECTS_RECEIVED_CHARGES, _tgt(ewar_eff_recv_charges), HEADERS)

            write_csv(pdir / OUT_CAP_WARFARE_DONE_PLAYERS, _src(cap_warf_done_players), HEADERS)
            write_csv(pdir / OUT_CAP_WARFARE_DONE_NPC, _src(cap_warf_done_npc), HEADERS)
            write_csv(pdir / OUT_CAP_WARFARE_DONE_DRONES, _src(cap_warf_done_drones), HEADERS)
            write_csv(pdir / OUT_CAP_WARFARE_DONE_CHARGES, _src(cap_warf_done_charges), HEADERS)

            write_csv(pdir / OUT_CAP_WARFARE_RECEIVED_PLAYERS, _tgt(cap_warf_recv_players), HEADERS)
            write_csv(pdir / OUT_CAP_WARFARE_RECEIVED_NPC, _tgt(cap_warf_recv_npc), HEADERS)
            write_csv(pdir / OUT_CAP_WARFARE_RECEIVED_DRONES, _tgt(cap_warf_recv_drones), HEADERS)
            write_csv(pdir / OUT_CAP_WARFARE_RECEIVED_CHARGES, _tgt(cap_warf_recv_charges), HEADERS)

            # Capacitor transfers
            write_csv(pdir / OUT_CAPACITOR_DONE_PLAYERS, _src(cap_done_players), HEADERS)
            write_csv(pdir / OUT_CAPACITOR_DONE_NPC, _src(cap_done_npc), HEADERS)
            write_csv(pdir / OUT_CAPACITOR_DONE_DRONES, _src(cap_done_drones), HEADERS)
            write_csv(pdir / OUT_CAPACITOR_DONE_CHARGES, _src(cap_done_charges), HEADERS)

            write_csv(pdir / OUT_CAPACITOR_RECEIVED_PLAYERS, _tgt(cap_recv_players), HEADERS)
            write_csv(pdir / OUT_CAPACITOR_RECEIVED_NPC, _tgt(cap_recv_npc), HEADERS)
            write_csv(pdir / OUT_CAPACITOR_RECEIVED_DRONES, _tgt(cap_recv_drones), HEADERS)
            write_csv(pdir / OUT_CAPACITOR_RECEIVED_CHARGES, _tgt(cap_recv_charges), HEADERS)

            # Others (per listener)
            write_csv(pdir / OUT_OTHERS, [r for r in f_others if (r.get("log_listener") or "") == pilot], OTHERS_HEADERS)

            # Propulsion jamming attempts (either side)
            write_csv(pdir / OUT_PROPULSION_JAM_ATTEMPTS, _either(f_prop_jam), PROP_JAM_HEADERS)

            # Player _combined mirror (combat only)
            p_combined = pdir / "_combined"
            p_combined.mkdir(parents=True, exist_ok=True)
            # Build combined_all_combat with dataset column
            sets = {
                OUT_REPAIRS_DONE_PLAYERS: _src(rep_done_players),
                OUT_REPAIRS_DONE_NPC: _src(rep_done_npc),
                OUT_REPAIRS_DONE_DRONES: _src(rep_done_drones),
                OUT_REPAIRS_DONE_CHARGES: _src(rep_done_charges),
                OUT_REPAIRS_RECEIVED_PLAYERS: _tgt(rep_recv_players),
                OUT_REPAIRS_RECEIVED_NPC: _tgt(rep_recv_npc),
                OUT_REPAIRS_RECEIVED_DRONES: _tgt(rep_recv_drones),
                OUT_REPAIRS_RECEIVED_CHARGES: _tgt(rep_recv_charges),
                OUT_DAMAGE_DONE_PLAYERS: _src(dmg_done_players),
                OUT_DAMAGE_DONE_NPC: _src(dmg_done_npc),
                OUT_DAMAGE_DONE_DRONES: _src(dmg_done_drones),
                OUT_DAMAGE_DONE_CHARGES: _src(dmg_done_charges),
                OUT_DAMAGE_RECEIVED_PLAYERS: _tgt(dmg_recv_players),
                OUT_DAMAGE_RECEIVED_NPC: _tgt(dmg_recv_npc),
                OUT_DAMAGE_RECEIVED_DRONES: _tgt(dmg_recv_drones),
                OUT_DAMAGE_RECEIVED_CHARGES: _tgt(dmg_recv_charges),
                OUT_EWAR_EFFECTS_DONE_PLAYERS: _src(ewar_eff_done_players),
                OUT_EWAR_EFFECTS_DONE_NPC: _src(ewar_eff_done_npc),
                OUT_EWAR_EFFECTS_DONE_DRONES: _src(ewar_eff_done_drones),
                OUT_EWAR_EFFECTS_DONE_CHARGES: _src(ewar_eff_done_charges),
                OUT_EWAR_EFFECTS_RECEIVED_PLAYERS: _tgt(ewar_eff_recv_players),
                OUT_EWAR_EFFECTS_RECEIVED_NPC: _tgt(ewar_eff_recv_npc),
                OUT_EWAR_EFFECTS_RECEIVED_DRONES: _tgt(ewar_eff_recv_drones),
                OUT_EWAR_EFFECTS_RECEIVED_CHARGES: _tgt(ewar_eff_recv_charges),
                OUT_CAP_WARFARE_DONE_PLAYERS: _src(cap_warf_done_players),
                OUT_CAP_WARFARE_DONE_NPC: _src(cap_warf_done_npc),
                OUT_CAP_WARFARE_DONE_DRONES: _src(cap_warf_done_drones),
                OUT_CAP_WARFARE_DONE_CHARGES: _src(cap_warf_done_charges),
                OUT_CAP_WARFARE_RECEIVED_PLAYERS: _tgt(cap_warf_recv_players),
                OUT_CAP_WARFARE_RECEIVED_NPC: _tgt(cap_warf_recv_npc),
                OUT_CAP_WARFARE_RECEIVED_DRONES: _tgt(cap_warf_recv_drones),
                OUT_CAP_WARFARE_RECEIVED_CHARGES: _tgt(cap_warf_recv_charges),
                OUT_CAPACITOR_DONE_PLAYERS: _src(cap_done_players),
                OUT_CAPACITOR_DONE_NPC: _src(cap_done_npc),
                OUT_CAPACITOR_DONE_DRONES: _src(cap_done_drones),
                OUT_CAPACITOR_DONE_CHARGES: _src(cap_done_charges),
                OUT_CAPACITOR_RECEIVED_PLAYERS: _tgt(cap_recv_players),
                OUT_CAPACITOR_RECEIVED_NPC: _tgt(cap_recv_npc),
                OUT_CAPACITOR_RECEIVED_DRONES: _tgt(cap_recv_drones),
                OUT_CAPACITOR_RECEIVED_CHARGES: _tgt(cap_recv_charges),
            }
            all_rows = []
            for fn, rs in sets.items():
                for r in rs:
                    rr = dict(r)
                    rr["dataset"] = fn.replace(".csv", "")
                    all_rows.append(rr)
            if all_rows:
                write_csv(p_combined / "combined_all_combat.csv", all_rows, ["dataset"] + HEADERS)

            # Player summary files
            sdir = pdir / "summary"
            sdir.mkdir(parents=True, exist_ok=True)
            involved_rows = _either(fight_combat_rows)

            # Build Pilot_list row (single) + ship sessions (Option 1)
            ships = set()
            fighters = set()
            corps = set()
            alls = set()
            for r in involved_rows:
                if (r.get("source_pilot") or "") == pilot:
                    sh = (r.get("source_ship_type") or "").strip()
                    if sh:
                        k = "unknown"
                        if ship_meta is not None:
                            try:
                                k = ship_meta.kind(sh)
                            except Exception:
                                k = "unknown"
                        if k == "fighter":
                            fighters.add(sh)
                        elif k != "drone":
                            ships.add(sh)
                    corps.add((r.get("source_corp") or "").strip())
                    alls.add((r.get("source_alliance") or "").strip())
                if (r.get("target_pilot") or "") == pilot:
                    sh = (r.get("target_ship_type") or "").strip()
                    if sh:
                        k = "unknown"
                        if ship_meta is not None:
                            try:
                                k = ship_meta.kind(sh)
                            except Exception:
                                k = "unknown"
                        if k == "fighter":
                            fighters.add(sh)
                        elif k != "drone":
                            ships.add(sh)
                    corps.add((r.get("target_corp") or "").strip())
                    alls.add((r.get("target_alliance") or "").strip())
            ships.discard("")
            corps.discard("")
            alls.discard("")

            # Reuse _write_fight_summary logic by creating a tiny local roster + instance summary.
            # fight_summary.txt
            win_line = f"Window: {win.start.strftime('%d-%m-%Y %H:%M:%S')} -> {win.end.strftime('%d-%m-%Y %H:%M:%S')} (duration {int((win.end-win.start).total_seconds())}s)"
            (sdir / "fight_summary.txt").write_text("\n".join([f"Pilot: {pilot}", win_line, f"Rows involving pilot: {len(involved_rows)}"]) + "\n", encoding="utf-8")

            # Instance summary for player
            import csv
            import re

            def _norm_mod(v: Any) -> str:
                s = str(v or "").strip()
                s2 = re.sub(r"^[\s\-\u2013\u2014]+", "", s).strip()
                s2 = re.sub(r"\s+", " ", s2)
                if item_names_lower is not None and s2 and (s2.lower() in item_names_lower):
                    return s2
                return s2 or s
            def _as_int(v):
                try:
                    if v is None or v == "":
                        return None
                    if isinstance(v, int):
                        return v
                    vs = str(v).strip()
                    if vs.isdigit() or (vs.startswith("-") and vs[1:].isdigit()):
                        return int(vs)
                except Exception:
                    return None
                return None
            inst = {}
            for r in involved_rows:
                ds = str(r.get("dataset") or "").strip()
                res = str(r.get("result") or "").strip()
                mod = _norm_mod(r.get("module"))
                key = (ds, res, mod)
                inst.setdefault(key, {"count":0,"total":0,"value_count":0})
                inst[key]["count"] += 1
                amt = _as_int(r.get("amount"))
                if amt is not None:
                    inst[key]["total"] += amt
                    inst[key]["value_count"] += 1
            cols = list(INSTANCE_SUMMARY_HEADERS)
            cols = _append_metadata_headers(cols)
            with (sdir / "Instance_Summary.csv").open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                for (ds,res,mod), v in sorted(inst.items(), key=lambda kv:(kv[0][0],kv[0][1],kv[0][2])):
                    vc = int(v.get("value_count") or 0)
                    tot = int(v.get("total") or 0)
                    row = {
                        "dataset": ds,
                        "result": res,
                        "module": mod,
                        "count": int(v.get("count") or 0),
                        "total_amount": tot if vc>0 else "",
                        "avg_amount": (tot/vc) if vc>0 else "",
                    }
                    row.update(metadata)
                    w.writerow(row)

            # Pilot_list (single row) with ship meta + basic stats
            ship_classes = []
            ship_techs = []
            hull_rarities = []
            if ship_meta is not None:
                for sh in sorted(ships, key=str.lower):
                    try:
                        cls, tech, rarity = ship_meta.resolve_extended(sh)
                    except Exception:
                        cls, tech, rarity = "","",""
                    if cls and cls not in ship_classes:
                        ship_classes.append(cls)
                    if tech and tech not in ship_techs:
                        ship_techs.append(tech)
                    if rarity and rarity not in hull_rarities:
                        hull_rarities.append(rarity)

            # Stats columns (same layout as fight Pilot_list)
            datasets = [
                "damage_done","damage_received","repairs_done","repairs_received",
                "ewar_effects_done","ewar_effects_received","cap_warfare_done","cap_warfare_received",
                "capacitor_done","capacitor_received","propulsion_jam_attempts",
            ]
            stats = {d:{"count":0,"total":0,"value_count":0} for d in datasets}
            for r in involved_rows:
                ds = str(r.get("dataset") or "").strip()
                if ds not in stats:
                    continue
                amt = _as_int(r.get("amount"))
                # Count if this pilot is relevant for the dataset direction
                if ds in ("damage_done","repairs_done","ewar_effects_done","cap_warfare_done","capacitor_done"):
                    if (r.get("source_pilot") or "") != pilot:
                        continue
                elif ds in ("damage_received","repairs_received","ewar_effects_received","cap_warfare_received","capacitor_received"):
                    if (r.get("target_pilot") or "") != pilot:
                        continue
                # propulsion_jam_attempts counts any involvement
                stats[ds]["count"] += 1
                if ds != "propulsion_jam_attempts" and amt is not None:
                    stats[ds]["total"] += amt
                    stats[ds]["value_count"] += 1

            # Build local ship sessions for this pilot (exclude drones/fighters)
            pilot_ship_sessions_local: Dict[str, Dict[str, Any]] = {}
            for r in involved_rows:
                ts_s = str(r.get("timestamp") or "").strip()
                for side in ("source", "target"):
                    if (r.get(f"{side}_pilot") or "") != pilot:
                        continue
                    sh = (r.get(f"{side}_ship_type") or "").strip()
                    if not sh:
                        continue
                    if ship_meta is not None:
                        try:
                            k = ship_meta.kind(sh)
                        except Exception:
                            k = "unknown"
                        if k in ("drone", "fighter"):
                            continue
                    from datetime import datetime
                    try:
                        t = datetime.strptime(ts_s, TS_FMT)
                    except Exception:
                        continue
                    rec = pilot_ship_sessions_local.setdefault(sh, {"first": t, "last": t, "count": 0})
                    if t < rec["first"]:
                        rec["first"] = t
                    if t > rec["last"]:
                        rec["last"] = t
                    rec["count"] += 1

            primary_ship = ""
            best_first = None
            for sh, rec in pilot_ship_sessions_local.items():
                t = rec.get("first")
                if t is None:
                    continue
                if best_first is None or t < best_first:
                    best_first = t
                    primary_ship = sh
            if not primary_ship and ships:
                primary_ship = sorted(ships, key=str.lower)[0]

            row = {
                "pilot": pilot,
                "alliance": ",".join(sorted(alls, key=str.lower)),
                "corp": ",".join(sorted(corps, key=str.lower)),
                "ship_types": primary_ship,
                "ship_types_seen": ",".join(sorted(ships, key=str.lower)),
                "ship_types_seen_count": len(ships),
                "fighter_types": ",".join(sorted(fighters, key=str.lower)),
                "ship_classes": ",".join(ship_classes),
                "ship_tech_levels": ",".join(ship_techs),
                "hull_rarities": ",".join(hull_rarities),
            }
            for ds in datasets:
                row[f"{ds}_count"] = int(stats[ds]["count"])
                if ds == "propulsion_jam_attempts":
                    row[f"{ds}_total"] = ""
                    row[f"{ds}_avg"] = ""
                else:
                    vc = int(stats[ds]["value_count"])
                    tot = int(stats[ds]["total"])
                    row[f"{ds}_total"] = tot if vc>0 else ""
                    row[f"{ds}_avg"] = (tot/vc) if vc>0 else ""

            cols = list(PILOT_LIST_HEADERS_BASE)
            for ds in datasets:
                cols.extend([f"{ds}_count",f"{ds}_total",f"{ds}_avg"])
            cols = _append_metadata_headers(cols)
            with (sdir / "Pilot_list.csv").open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                row.update(metadata)
                w.writerow(row)

            # Pilot_ship_sessions.csv (player-only)
            sess_cols = [h for h in PILOT_SHIP_SESSIONS_HEADERS if h not in ("alliance", "corp")]
            sess_cols = _append_metadata_headers(sess_cols)
            with (sdir / "Pilot_ship_sessions.csv").open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=sess_cols)
                w.writeheader()
                for sh, rec in sorted(pilot_ship_sessions_local.items(), key=lambda kv: ((kv[1].get("first") or win.start), kv[0].lower())):
                    first = rec.get("first")
                    last = rec.get("last")
                    if first is None or last is None:
                        continue
                    cls = tech = rarity = ""
                    if ship_meta is not None:
                        try:
                            cls, tech, rarity = ship_meta.resolve_extended(sh)
                        except Exception:
                            cls, tech, rarity = "","",""
                    row = {
                        "pilot": pilot,
                        "ship_type": sh,
                        "ship_class": cls,
                        "ship_tech": tech,
                        "hull_rarity": rarity,
                        "first_seen": first.strftime("%d-%m-%Y %H:%M:%S"),
                        "last_seen": last.strftime("%d-%m-%Y %H:%M:%S"),
                        "duration_s": int((last-first).total_seconds()),
                        "seen_events_count": int(rec.get("count") or 0),
                    }
                    row.update(metadata)
                    w.writerow(row)

        for pilot in sorted(pilots_in_fight, key=str.lower):
            _write_player_folder(pilot)

    # --------------------------------------------------------------
    # Combined outputs (Option B)
    # --------------------------------------------------------------

    combined_folder = out_folder / "_combined"
    combined_folder.mkdir(parents=True, exist_ok=True)

    # 1) Mirror the per-fight file set, but combined across all fights.
    for fn, rows in combined.items():
        if fn == OUT_OTHERS:
            write_csv(combined_folder / fn, rows, OTHERS_HEADERS)
        elif fn == OUT_PROPULSION_JAM_ATTEMPTS:
            write_csv(combined_folder / fn, rows, PROP_JAM_HEADERS)
        else:
            write_csv(combined_folder / fn, rows, HEADERS)

    # 2) Category-level combined files with an extra "dataset" column.
    def _write_combined_category(name: str, files: List[str], headers: List[str]) -> None:
        all_rows: List[Dict[str, Any]] = []
        for f in files:
            for r in combined.get(f, []):
                rr = dict(r)
                rr["dataset"] = f.replace(".csv", "")
                all_rows.append(rr)
        if not all_rows:
            return
        write_csv(combined_folder / name, all_rows, ["dataset"] + headers)

    _write_combined_category(
        "combined_repairs.csv",
        [
            OUT_REPAIRS_DONE_PLAYERS,
            OUT_REPAIRS_DONE_NPC,
            OUT_REPAIRS_DONE_DRONES,
            OUT_REPAIRS_DONE_CHARGES,
            OUT_REPAIRS_RECEIVED_PLAYERS,
            OUT_REPAIRS_RECEIVED_NPC,
            OUT_REPAIRS_RECEIVED_DRONES,
            OUT_REPAIRS_RECEIVED_CHARGES,
        ],
        HEADERS,
    )
    _write_combined_category(
        "combined_damage.csv",
        [
            OUT_DAMAGE_DONE_PLAYERS,
            OUT_DAMAGE_DONE_NPC,
            OUT_DAMAGE_DONE_DRONES,
            OUT_DAMAGE_DONE_CHARGES,
            OUT_DAMAGE_RECEIVED_PLAYERS,
            OUT_DAMAGE_RECEIVED_NPC,
            OUT_DAMAGE_RECEIVED_DRONES,
            OUT_DAMAGE_RECEIVED_CHARGES,
        ],
        HEADERS,
    )
    _write_combined_category(
        "combined_ewar_effects.csv",
        [
            OUT_EWAR_EFFECTS_DONE_PLAYERS,
            OUT_EWAR_EFFECTS_DONE_NPC,
            OUT_EWAR_EFFECTS_DONE_DRONES,
            OUT_EWAR_EFFECTS_DONE_CHARGES,
            OUT_EWAR_EFFECTS_RECEIVED_PLAYERS,
            OUT_EWAR_EFFECTS_RECEIVED_NPC,
            OUT_EWAR_EFFECTS_RECEIVED_DRONES,
            OUT_EWAR_EFFECTS_RECEIVED_CHARGES,
        ],
        HEADERS,
    )

    # User-requested: capacitor warfare (neuts/nos) split out separately.
    _write_combined_category(
        "combined_cap_warfare.csv",
        [
            OUT_CAP_WARFARE_DONE_PLAYERS,
            OUT_CAP_WARFARE_DONE_NPC,
            OUT_CAP_WARFARE_DONE_DRONES,
            OUT_CAP_WARFARE_DONE_CHARGES,
            OUT_CAP_WARFARE_RECEIVED_PLAYERS,
            OUT_CAP_WARFARE_RECEIVED_NPC,
            OUT_CAP_WARFARE_RECEIVED_DRONES,
            OUT_CAP_WARFARE_RECEIVED_CHARGES,
        ],
        HEADERS,
    )

    _write_combined_category(
        "combined_capacitor_transfers.csv",
        [
            OUT_CAPACITOR_DONE_PLAYERS,
            OUT_CAPACITOR_DONE_NPC,
            OUT_CAPACITOR_DONE_DRONES,
            OUT_CAPACITOR_DONE_CHARGES,
            OUT_CAPACITOR_RECEIVED_PLAYERS,
            OUT_CAPACITOR_RECEIVED_NPC,
            OUT_CAPACITOR_RECEIVED_DRONES,
            OUT_CAPACITOR_RECEIVED_CHARGES,
        ],
        HEADERS,
    )

    # Combat-only "one file" view.
    _write_combined_category(
        "combined_all_combat.csv",
        [
            OUT_REPAIRS_DONE_PLAYERS,
            OUT_REPAIRS_DONE_NPC,
            OUT_REPAIRS_DONE_DRONES,
            OUT_REPAIRS_DONE_CHARGES,
            OUT_REPAIRS_RECEIVED_PLAYERS,
            OUT_REPAIRS_RECEIVED_NPC,
            OUT_REPAIRS_RECEIVED_DRONES,
            OUT_REPAIRS_RECEIVED_CHARGES,
            OUT_DAMAGE_DONE_PLAYERS,
            OUT_DAMAGE_DONE_NPC,
            OUT_DAMAGE_DONE_DRONES,
            OUT_DAMAGE_DONE_CHARGES,
            OUT_DAMAGE_RECEIVED_PLAYERS,
            OUT_DAMAGE_RECEIVED_NPC,
            OUT_DAMAGE_RECEIVED_DRONES,
            OUT_DAMAGE_RECEIVED_CHARGES,
            OUT_EWAR_EFFECTS_DONE_PLAYERS,
            OUT_EWAR_EFFECTS_DONE_NPC,
            OUT_EWAR_EFFECTS_DONE_DRONES,
            OUT_EWAR_EFFECTS_DONE_CHARGES,
            OUT_EWAR_EFFECTS_RECEIVED_PLAYERS,
            OUT_EWAR_EFFECTS_RECEIVED_NPC,
            OUT_EWAR_EFFECTS_RECEIVED_DRONES,
            OUT_EWAR_EFFECTS_RECEIVED_CHARGES,
            OUT_CAP_WARFARE_DONE_PLAYERS,
            OUT_CAP_WARFARE_DONE_NPC,
            OUT_CAP_WARFARE_DONE_DRONES,
            OUT_CAP_WARFARE_DONE_CHARGES,
            OUT_CAP_WARFARE_RECEIVED_PLAYERS,
            OUT_CAP_WARFARE_RECEIVED_NPC,
            OUT_CAP_WARFARE_RECEIVED_DRONES,
            OUT_CAP_WARFARE_RECEIVED_CHARGES,
            OUT_CAPACITOR_DONE_PLAYERS,
            OUT_CAPACITOR_DONE_NPC,
            OUT_CAPACITOR_DONE_DRONES,
            OUT_CAPACITOR_DONE_CHARGES,
            OUT_CAPACITOR_RECEIVED_PLAYERS,
            OUT_CAPACITOR_RECEIVED_NPC,
            OUT_CAPACITOR_RECEIVED_DRONES,
            OUT_CAPACITOR_RECEIVED_CHARGES,
        ],
        HEADERS,
    )

    # Persist learned corp/alliance after all fights.
    if pilot_db_updates_total:
        save_pilot_db(str(pilot_db_path), pilot_db)
        print(f"Updated persistent pilot DB: {pilot_db_path} (+{pilot_db_updates_total} updates)")

    if not args.no_esi and save_cache_to_disk and args.cache_file:
        save_cache(str(args.cache_file), cache)
        print(f"Saved ESI cache: {args.cache_file}")

    if save_aff_esi and args.aff_esi_db_file:
        save_aff_db(str(args.aff_esi_db_file), aff_esi)
        print(f"Saved affiliations_from_esi: {args.aff_esi_db_file} ({len(aff_esi)} mappings)")

    # Auto-open output folder at the end of a successful run.
    if not args.no_open:
        _open_folder(out_folder)

    return 0
