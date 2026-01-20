from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .affiliations import AffDB, update_affiliation_db
from .entity import parse_damage_entity, parse_entity_any, parse_rep_party
from .ewar import ENERGY_DRAIN_TO_AMOUNT_RE, ENERGY_NEUT_AMOUNT_RE, classify_ewar
from .text import TS_PREFIX_RE, clean_line, parse_ts
from .timeline import Timeline, add_ship_event, finalize_timeline, lookup_ship


# Damage miss parsing
GROUP_MISS_RE = re.compile(
    r"^Your group of (?P<weapon>.+?) misses (?P<target>.+?) completely\s*-\s*(?P<module>.+?)\s*$",
    re.IGNORECASE,
)

DRONE_MISS_RE = re.compile(
    r"^(?P<drone>.+?)\s+belonging\s+to\s+(?P<owner>.+?)\s+misses\s+you\s+completely\s*-\s*(?P<module>.+?)\s*$",
    re.IGNORECASE,
)


def _clean_module_name(mod: str, item_names_lower: set[str] | None = None) -> str:
    """Normalize module names extracted from log lines.

    Fixes cases like "- Heavy Gremlin Compact Energy Neutralizer" which cause
    duplicate groupings in summaries.

    If an SDE name set is provided and the cleaned name exists in it, we return
    the cleaned name.
    """

    if mod is None:
        return ""
    m = str(mod).strip()
    # Remove leading dashes/spaces (handles "- - Module", "- Module", etc.).
    m2 = re.sub(r"^[\s\-\u2013\u2014]+", "", m)
    # Collapse internal whitespace
    m2 = re.sub(r"\s+", " ", m2).strip()
    if item_names_lower is not None and m2 and (m2.lower() in item_names_lower):
        return m2
    return m2 or m


def _is_you_token(name: str) -> bool:
    n = (name or "").strip().lower()
    return n in {"you", "you!", "you !"}


def build_ship_timeline_and_afflog(
    log_paths: List[str | Path],
    base_aff_log: AffDB,
) -> Tuple[Timeline, AffDB]:
    """Pass 1: build ship timeline + update affiliations_from_logs from rep lines."""

    timeline: Timeline = {}
    aff_log: AffDB = dict(base_aff_log)

    for p in log_paths:
        path = str(p)
        listener = ""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                cleaned = clean_line(raw)

                if cleaned.startswith("Listener:"):
                    listener = cleaned.split("Listener:", 1)[1].strip()
                    continue

                m = TS_PREFIX_RE.match(cleaned)
                if not m:
                    continue

                ts = parse_ts(m.group("ts"))
                chan = m.group("chan")
                body = m.group("body")

                if chan == "notify" and "Disembarking from ship" in body and listener:
                    add_ship_event(timeline, listener, ts, ship=None, alliance="", corp="")
                    continue

                if chan != "combat":
                    continue

                # ------------------------------------------------------------------
                # Affiliation learning beyond reps
                #
                # Some combat lines (e.g. warp scramble attempt) include full
                # rep-style entities with BOTH alliance + corp tickers, even
                # when there are no remote-repair lines in the log set.
                # We opportunistically learn corp->alliance from those too.
                # Example formats in logs:
                #   "Warp scramble attempt from <Pilot [ALL][CORP] Ship> to <Pilot [ALL][CORP] Ship>"
                #   "You're jammed by <Pilot [ALL][CORP] Ship> - <module>"
                # ------------------------------------------------------------------

                if "Warp scramble attempt" in body and " from " in body and " to " in body:
                    _left, _, rest = body.partition(" from ")
                    src_part, _, tgt_part = rest.partition(" to ")
                    if src_part and tgt_part:
                        sp, ss, sa, sc = parse_entity_any(src_part.strip())
                        tp, ts2, ta, tc = parse_entity_any(tgt_part.strip())
                        update_affiliation_db(aff_log, sc, sa, ts)
                        update_affiliation_db(aff_log, tc, ta, ts)
                        # also feed ship sightings into timeline if present
                        if ss:
                            add_ship_event(timeline, sp, ts, ship=ss, alliance=sa, corp=sc)
                        if ts2:
                            add_ship_event(timeline, tp, ts, ship=ts2, alliance=ta, corp=tc)
                    continue

                if body.startswith("You're ") and " by " in body and " - " in body:
                    _effect, _, rest = body.partition(" by ")
                    entity_part, _, _module = rest.partition(" - ")
                    if entity_part:
                        sp, ss, sa, sc = parse_entity_any(entity_part.strip())
                        update_affiliation_db(aff_log, sc, sa, ts)
                        if ss:
                            add_ship_event(timeline, sp, ts, ship=ss, alliance=sa, corp=sc)
                    # don't continue; the normal parsing below may still want to
                    # classify the line in pass 2.

                if "remote shield boosted by" in body:
                    _, _, right = body.partition("remote shield boosted by")
                    party_part, sep, _module = right.strip().partition(" - ")
                    if not sep:
                        continue
                    src_pilot, src_ship, src_all, src_corp = parse_rep_party(party_part)
                    update_affiliation_db(aff_log, src_corp, src_all, ts)
                    if src_ship:
                        add_ship_event(timeline, src_pilot, ts, ship=src_ship, alliance=src_all, corp=src_corp)
                    continue

                if "remote shield boosted to" in body:
                    _, _, right = body.partition("remote shield boosted to")
                    party_part, sep, _module = right.strip().partition(" - ")
                    if not sep:
                        continue
                    tgt_pilot, tgt_ship, tgt_all, tgt_corp = parse_rep_party(party_part)
                    update_affiliation_db(aff_log, tgt_corp, tgt_all, ts)
                    if tgt_ship:
                        add_ship_event(timeline, tgt_pilot, ts, ship=tgt_ship, alliance=tgt_all, corp=tgt_corp)
                    continue

                # Damage-style ship sightings
                if " from " in body:
                    _, _, rest = body.partition("from ")
                    parts = rest.split(" - ")
                    if not parts:
                        continue
                    src_entity = parts[0].strip()
                    p2, corp2, ship2 = parse_damage_entity(src_entity)
                    if ship2:
                        add_ship_event(timeline, p2, ts, ship=ship2, alliance="", corp=corp2)
                    continue

                if " to " in body:
                    _, _, rest = body.partition("to ")
                    parts = rest.split(" - ")
                    if not parts:
                        continue
                    tgt_entity = parts[0].strip()
                    p2, corp2, ship2 = parse_damage_entity(tgt_entity)
                    if ship2:
                        add_ship_event(timeline, p2, ts, ship=ship2, alliance="", corp=corp2)
                    continue

    finalize_timeline(timeline)
    return timeline, aff_log


def parse_log_file_to_rows(
    path: str | Path,
    ship_timeline: Timeline,
    others_rows: List[Dict[str, Any]],
    item_names_lower: set[str] | None = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Pass 2: parse rows + collect others."""

    path = Path(path)
    out: Dict[str, List[Dict[str, Any]]] = {
        "repairs_done": [],
        "repairs_received": [],
        "damage_done": [],
        "damage_received": [],
        # EWAR is split into (1) effects (ECM, damps, points, webs, etc.)
        # and (2) capacitor warfare (neut / nos).
        "ewar_effects_done": [],
        "ewar_effects_received": [],
        "cap_warfare_done": [],
        "cap_warfare_received": [],
        # Remote capacitor transfer (positive, like repairs)
        "capacitor_done": [],
        "capacitor_received": [],
        "propulsion_jam_attempts": [],
    }

    listener = ""
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line_no, raw in enumerate(f, start=1):
            cleaned = clean_line(raw)

            if cleaned.startswith("Listener:"):
                listener = cleaned.split("Listener:", 1)[1].strip()
                continue

            m = TS_PREFIX_RE.match(cleaned)
            if not m:
                continue

            ts_str = m.group("ts")
            ts = parse_ts(ts_str)
            chan = m.group("chan")
            body = m.group("body")

            if not listener:
                continue

            listener_ship, listener_all, listener_corp = lookup_ship(ship_timeline, listener, ts)

            classified = False

            # Repairs done
            if chan == "combat" and "remote shield boosted to" in body:
                left, _, right = body.partition("remote shield boosted to")
                amount_str = left.strip().split(" ", 1)[0].strip()
                if amount_str.isdigit():
                    amount = int(amount_str)
                    party_part, sep, module = right.strip().partition(" - ")
                    if sep:
                        tgt_pilot, tgt_ship, tgt_all, tgt_corp = parse_rep_party(party_part)
                        out["repairs_done"].append(
                            {
                                "timestamp": ts_str,
                                "amount": amount,
                                "log_listener": listener,
                                "source_pilot": listener,
                                "source_ship_type": listener_ship,
                                "source_alliance": listener_all,
                                "source_corp": listener_corp,
                                "target_pilot": tgt_pilot,
                                "target_ship_type": tgt_ship,
                                "target_alliance": tgt_all,
                                "target_corp": tgt_corp,
                                "module": module.strip(),
                                "result": "",
                                "Log_file_original_line": cleaned,
                                "Log_file_line_number": line_no,
                                "log_file": path.name,
                            }
                        )
                        classified = True

            # Repairs received
            if chan == "combat" and (not classified) and "remote shield boosted by" in body:
                left, _, right = body.partition("remote shield boosted by")
                amount_str = left.strip().split(" ", 1)[0].strip()
                if amount_str.isdigit():
                    amount = int(amount_str)
                    party_part, sep, module = right.strip().partition(" - ")
                    if sep:
                        src_pilot, src_ship, src_all, src_corp = parse_rep_party(party_part)
                        out["repairs_received"].append(
                            {
                                "timestamp": ts_str,
                                "amount": amount,
                                "log_listener": listener,
                                "source_pilot": src_pilot,
                                "source_ship_type": src_ship,
                                "source_alliance": src_all,
                                "source_corp": src_corp,
                                "target_pilot": listener,
                                "target_ship_type": listener_ship,
                                "target_alliance": listener_all,
                                "target_corp": listener_corp,
                                "module": module.strip(),
                                "result": "",
                                "Log_file_original_line": cleaned,
                                "Log_file_line_number": line_no,
                                "log_file": path.name,
                            }
                        )
                        classified = True

            # EWAR: energy neutralized amount format (RECEIVED by listener)
            if chan == "combat" and (not classified):
                m_neut = ENERGY_NEUT_AMOUNT_RE.match(body)
                if m_neut:
                    amt = int(m_neut.group("amount"))
                    entity_part = m_neut.group("entity").strip()
                    module = m_neut.group("module").strip()
                    src_pilot, src_ship, src_all, src_corp = parse_entity_any(entity_part)
                    out["cap_warfare_received"].append(
                        {
                            "timestamp": ts_str,
                            "amount": amt,
                            "log_listener": listener,
                            "source_pilot": src_pilot,
                            "source_ship_type": src_ship,
                            "source_alliance": src_all,
                            "source_corp": src_corp,
                            "target_pilot": listener,
                            "target_ship_type": listener_ship,
                            "target_alliance": listener_all,
                            "target_corp": listener_corp,
                            "module": _clean_module_name(module, item_names_lower),
                            "result": "Energy Neutralizer",
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            # EWAR: energy drained to ... amount format (RECEIVED by listener)
            if chan == "combat" and (not classified):
                m_drain = ENERGY_DRAIN_TO_AMOUNT_RE.match(body)
                if m_drain:
                    amt = int(m_drain.group("amount"))
                    entity_part = m_drain.group("entity").strip()
                    module = m_drain.group("module").strip()

                    # Primary parse: rep-style, fallback: damage-style
                    src_pilot, src_ship, src_all, src_corp = parse_entity_any(entity_part)

                    out["cap_warfare_received"].append(
                        {
                            "timestamp": ts_str,
                            "amount": abs(amt),
                            "log_listener": listener,
                            "source_pilot": src_pilot,
                            "source_ship_type": src_ship,
                            "source_alliance": src_all,
                            "source_corp": src_corp,
                            "target_pilot": listener,
                            "target_ship_type": listener_ship,
                            "target_alliance": listener_all,
                            "target_corp": listener_corp,
                            "module": _clean_module_name(module, item_names_lower),
                            "result": "Energy Nosferatu",
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            # EWAR: received "You're ... by <entity> - <module>"
            if chan == "combat" and (not classified) and body.startswith("You're ") and " by " in body and " - " in body:
                left, _, rest = body.partition(" by ")
                ewar_type = classify_ewar(left)
                entity_part, _, module = rest.partition(" - ")
                if ewar_type != "Unknown EWAR":
                    src_pilot, src_ship, src_all, src_corp = parse_entity_any(entity_part)
                    # Split cap warfare vs effects
                    key = "cap_warfare_received" if ewar_type in ("Energy Neutralizer", "Energy Nosferatu") else "ewar_effects_received"
                    out[key].append(
                        {
                            "timestamp": ts_str,
                            "amount": "",
                            "log_listener": listener,
                            "source_pilot": src_pilot,
                            "source_ship_type": src_ship,
                            "source_alliance": src_all,
                            "source_corp": src_corp,
                            "target_pilot": listener,
                            "target_ship_type": listener_ship,
                            "target_alliance": listener_all,
                            "target_corp": listener_corp,
                            "module": _clean_module_name(module, item_names_lower),
                            "result": ewar_type,
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            # Remote capacitor transmitted by <entity> - <module>
            # Example observed:
            # 351 remote capacitor transmitted by Basilisk [ECHO.] [INOU] [Ownda] - - Large Remote Capacitor Transmitter II
            if chan == "combat" and (not classified) and "remote capacitor transmitted" in body:
                # We support both "to" and "by" variants. If receiver is missing we still
                # output the "done" view (source -> UNKNOWN).
                amount_str = body.split(" ", 1)[0].strip()
                if amount_str.isdigit():
                    amount = int(amount_str)
                    if " transmitted by " in body:
                        left, _, right = body.partition("transmitted by")
                        party_part, sep, module = right.strip().partition(" - ")
                        if sep:
                            src_pilot, src_ship, src_all, src_corp = parse_rep_party(party_part)
                            out["capacitor_done"].append(
                                {
                                    "timestamp": ts_str,
                                    "amount": amount,
                                    "log_listener": listener,
                                    "source_pilot": src_pilot,
                                    "source_ship_type": src_ship,
                                    "source_alliance": src_all,
                                    "source_corp": src_corp,
                                    "target_pilot": listener,
                                    "target_ship_type": listener_ship,
                                    "target_alliance": listener_all,
                                    "target_corp": listener_corp,
                                    "module": _clean_module_name(module, item_names_lower),
                                    "result": "Remote Capacitor Transfer",
                                    "Log_file_original_line": cleaned,
                                    "Log_file_line_number": line_no,
                                    "log_file": path.name,
                                }
                            )
                            out["capacitor_received"].append(
                                {
                                    "timestamp": ts_str,
                                    "amount": amount,
                                    "log_listener": listener,
                                    "source_pilot": src_pilot,
                                    "source_ship_type": src_ship,
                                    "source_alliance": src_all,
                                    "source_corp": src_corp,
                                    "target_pilot": listener,
                                    "target_ship_type": listener_ship,
                                    "target_alliance": listener_all,
                                    "target_corp": listener_corp,
                                    "module": _clean_module_name(module, item_names_lower),
                                    "result": "Remote Capacitor Transfer",
                                    "Log_file_original_line": cleaned,
                                    "Log_file_line_number": line_no,
                                    "log_file": path.name,
                                }
                            )
                            classified = True
                    elif " transmitted to " in body:
                        left, _, right = body.partition("transmitted to")
                        party_part, sep, module = right.strip().partition(" - ")
                        if sep:
                            tgt_pilot, tgt_ship, tgt_all, tgt_corp = parse_rep_party(party_part)
                            # In this variant, listener is source.
                            out["capacitor_done"].append(
                                {
                                    "timestamp": ts_str,
                                    "amount": amount,
                                    "log_listener": listener,
                                    "source_pilot": listener,
                                    "source_ship_type": listener_ship,
                                    "source_alliance": listener_all,
                                    "source_corp": listener_corp,
                                    "target_pilot": tgt_pilot,
                                    "target_ship_type": tgt_ship,
                                    "target_alliance": tgt_all,
                                    "target_corp": tgt_corp,
                                    "module": _clean_module_name(module, item_names_lower),
                                    "result": "Remote Capacitor Transfer",
                                    "Log_file_original_line": cleaned,
                                    "Log_file_line_number": line_no,
                                    "log_file": path.name,
                                }
                            )
                            classified = True

            # Damage miss pattern (done)
            if chan == "combat" and (not classified):
                m_miss = GROUP_MISS_RE.match(body)
                if m_miss:
                    weapon = (m_miss.group("weapon") or "").strip()
                    target_text = (m_miss.group("target") or "").strip()
                    module = (m_miss.group("module") or "").strip()

                    tgt_pilot, tgt_ship, tgt_all, tgt_corp = parse_entity_any(target_text)
                    out["damage_done"].append(
                        {
                            "timestamp": ts_str,
                            "amount": 0,
                            "log_listener": listener,
                            "source_pilot": listener,
                            "source_ship_type": listener_ship,
                            "source_alliance": listener_all,
                            "source_corp": listener_corp,
                            "target_pilot": tgt_pilot,
                            "target_ship_type": tgt_ship,
                            "target_alliance": tgt_all,
                            "target_corp": tgt_corp,
                            "module": module or weapon,
                            "result": "Misses completely",
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            # EWAR: best-effort "You ... - <module>" (DONE)
            if chan == "combat" and (not classified) and body.startswith("You ") and " - " in body:
                ewar_type = classify_ewar(body)
                if ewar_type != "Unknown EWAR":
                    left, _, module = body.partition(" - ")
                    left = left[4:].strip()  # remove "You "
                    tgt_pilot, tgt_ship, tgt_all, tgt_corp = parse_entity_any(left)
                    key = "cap_warfare_done" if ewar_type in ("Energy Neutralizer", "Energy Nosferatu") else "ewar_effects_done"
                    out[key].append(
                        {
                            "timestamp": ts_str,
                            "amount": "",
                            "log_listener": listener,
                            "source_pilot": listener,
                            "source_ship_type": listener_ship,
                            "source_alliance": listener_all,
                            "source_corp": listener_corp,
                            "target_pilot": tgt_pilot,
                            "target_ship_type": tgt_ship,
                            "target_alliance": tgt_all,
                            "target_corp": tgt_corp,
                            "module": _clean_module_name(module, item_names_lower),
                            "result": ewar_type,
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            # Damage received: "<amount> from <entity> - <weapon> - <result>"
            if chan == "combat" and (not classified) and " from " in body:
                amount_str = body.split(" ", 1)[0].strip()
                if amount_str.isdigit():
                    amount = int(amount_str)
                    _, _, rest = body.partition("from ")
                    parts = rest.split(" - ")
                    if len(parts) >= 3:
                        src_entity = parts[0].strip()
                        weapon = parts[1].strip()
                        result = " - ".join(parts[2:]).strip()
                        src_pilot, src_corp, src_ship = parse_damage_entity(src_entity)
                        out["damage_received"].append(
                            {
                                "timestamp": ts_str,
                                "amount": amount,
                                "log_listener": listener,
                                "source_pilot": src_pilot,
                                "source_ship_type": src_ship,
                                "source_alliance": "",
                                "source_corp": src_corp,
                                "target_pilot": listener,
                                "target_ship_type": listener_ship,
                                "target_alliance": listener_all,
                                "target_corp": listener_corp,
                                "module": weapon,
                                "result": result,
                                "Log_file_original_line": cleaned,
                                "Log_file_line_number": line_no,
                                "log_file": path.name,
                            }
                        )
                        classified = True

            # Damage done: "<amount> to <entity> - <weapon> - <result>"
            if chan == "combat" and (not classified) and " to " in body:
                amount_str = body.split(" ", 1)[0].strip()
                if amount_str.isdigit():
                    amount = int(amount_str)
                    _, _, rest = body.partition("to ")
                    parts = rest.split(" - ")
                    if len(parts) >= 3:
                        tgt_entity = parts[0].strip()
                        weapon = parts[1].strip()
                        result = " - ".join(parts[2:]).strip()
                        tgt_pilot, tgt_corp, tgt_ship = parse_damage_entity(tgt_entity)
                        out["damage_done"].append(
                            {
                                "timestamp": ts_str,
                                "amount": amount,
                                "log_listener": listener,
                                "source_pilot": listener,
                                "source_ship_type": listener_ship,
                                "source_alliance": listener_all,
                                "source_corp": listener_corp,
                                "target_pilot": tgt_pilot,
                                "target_ship_type": tgt_ship,
                                "target_alliance": "",
                                "target_corp": tgt_corp,
                                "module": weapon,
                                "result": result,
                                "Log_file_original_line": cleaned,
                                "Log_file_line_number": line_no,
                                "log_file": path.name,
                            }
                        )
                        classified = True



            # Drone misses: "<Drone> belonging to <Pilot> misses you completely - <module>"
            # Treat as damage received with 0 amount (shot fired but missed).
            if chan == "combat" and (not classified):
                m_dm = DRONE_MISS_RE.match(body)
                if m_dm:
                    drone = m_dm.group("drone").strip()
                    owner = m_dm.group("owner").strip()
                    module = m_dm.group("module").strip()
                    out["damage_received"].append(
                        {
                            "timestamp": ts_str,
                            "amount": 0,
                            "log_listener": listener,
                            "source_pilot": owner,
                            "source_ship_type": drone,
                            "source_alliance": "",
                            "source_corp": "",
                            "target_pilot": listener,
                            "target_ship_type": listener_ship,
                            "target_alliance": listener_all,
                            "target_corp": listener_corp,
                            "module": module,
                            "result": "Misses completely",
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            # Propulsion jamming attempts (warp scramble/disruption attempt)
            # Example: "Warp scramble attempt from <entity> - to <entity> -"
            if chan == "combat" and (not classified):
                attempt_type = ""
                if "Warp scramble attempt" in body and " from " in body and " to " in body:
                    attempt_type = "Warp scramble attempt"
                elif "Warp disruption attempt" in body and " from " in body and " to " in body:
                    attempt_type = "Warp disruption attempt"
                if attempt_type:
                    _left, _, rest = body.partition(" from ")
                    src_part, _, tgt_rest = rest.partition(" to ")
                    # tgt may have trailing " -" module segments
                    tgt_part, _, module_part = tgt_rest.partition(" - ")

                    # In many logs the attempt line is formatted as:
                    #   "... from <entity> - to <entity>" (note the '-' before 'to')
                    # If we do not strip this dash, entity parsing can shift the ship name
                    # into the pilot column. So we remove a trailing '-' token first.
                    sp = src_part.strip()
                    if sp.endswith("-"):
                        sp = sp[:-1].strip()
                    tp = tgt_part.strip()
                    if tp.endswith("-"):
                        tp = tp[:-1].strip()

                    src_pilot, src_ship, src_all, src_corp = parse_entity_any(sp)
                    tgt_pilot, tgt_ship, tgt_all, tgt_corp = parse_entity_any(tp)

                    # "to you!" means the log listener.
                    if _is_you_token(tgt_pilot):
                        tgt_pilot = listener
                        tgt_ship = listener_ship
                        tgt_all = listener_all
                        tgt_corp = listener_corp

                    mod_clean = _clean_module_name(module_part, item_names_lower)
                    out["propulsion_jam_attempts"].append(
                        {
                            "timestamp": ts_str,
                            "log_listener": listener,
                            "source_pilot": src_pilot,
                            "source_ship_type": src_ship,
                            "source_alliance": src_all,
                            "source_corp": src_corp,
                            "target_pilot": tgt_pilot,
                            "target_ship_type": tgt_ship,
                            "target_alliance": tgt_all,
                            "target_corp": tgt_corp,
                            "attempt_type": attempt_type,
                            "module": mod_clean,
                            "result": "Attempt",
                            "Log_file_original_line": cleaned,
                            "Log_file_line_number": line_no,
                            "log_file": path.name,
                        }
                    )
                    classified = True

            if not classified:
                others_rows.append(
                    {
                        "timestamp": ts_str,
                        "channel": chan,
                        "log_listener": listener,
                        "text": body,
                        "Log_file_original_line": cleaned,
                        "Log_file_line_number": line_no,
                        "log_file": path.name,
                    }
                )

    return out
