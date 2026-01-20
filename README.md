# EVE Combat Parser

> ⚠️ **Experimental / Unstable**
>
> This project is under active development.
> Output formats, schemas, and internal APIs are **not stable** and may change
> between versions without notice.
>
> This repository currently focuses on the **EVE Online combat log parser only**.
> Web/UI components are intentionally out of scope at this stage.


Parse EVE Online **combat log** `.txt` files into structured CSV outputs:

- Per-fight folders (auto split by inactivity)
- Per-category combat CSVs (damage, repairs, EWAR, capacitor, etc.)
- Combined CSVs (per fight and across fights)
- Per-player filtered outputs
- Summary CSVs (pilot list, ship sessions, instance summary, corp/alliance breakdown)

This project is designed for **offline-first** use. It can optionally use ESI to backfill missing affiliation and type information, and it can download updatable SDE CSV dumps from fuzzwork.

---

## Requirements

- Python **3.10+**
- Optional but recommended: `requests` (used for ESI lookups)

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Quick start

1. Put combat log files into a subfolder under `./logs/`, for example:

```
logs/
  my_fight_logs/
    20260115_225941_548047231.txt
    20260115_230059_1146915702.txt
```

2. Run:

```bash
python -m eve_combat_parser
```

3. Choose a log subfolder when prompted. Outputs are written under `./output/`.

---

## Data sources

### SDE CSV (fuzzwork)
On first run (or when files are missing), the tool can download required SDE CSVs from fuzzwork into `./sde/`.

Required files:
- `invTypes-nodescription.csv` (type name index)
- `invGroups.csv` (group name / ship class)
- `invMetaTypes.csv` + `invMetaGroups.csv` (tech / rarity)
- `dgmTypeAttributes.csv` (module meta level)

If you keep `./sde/` updated, you control when the dataset changes.

### ESI (optional)
If ESI is enabled (default), the parser can:
- resolve missing corp/alliance IDs
- backfill missing type IDs when the local SDE index can’t find a name

Disable all ESI usage:

```bash
python -m eve_combat_parser --no-esi
```

---

## Outputs

### Output root
Each run writes to `./output/<run_id>_<folder_name>/`.

### Per-fight folders
The parser splits events into fights using a gap heuristic and writes:

```
fight_001_<start>_TO_<end>/
  damage_done_players.csv
  damage_received_players.csv
  repairs_done_players.csv
  ...
  Propulsion_Jamming_attempts.csv
  _combined/
    combined_all_combat.csv
    combined_damage.csv
    combined_repairs.csv
    ...
  Players/
    <Pilot Name>/
      (same CSV types filtered for that pilot)
      summary/
        Pilot_list.csv
        Pilot_ship_sessions.csv
        Instance_Summary.csv
        Pilot_Stats.csv
```

### Summary folder (per run)
`summary/` contains rollups and reference tables:

- `Pilot_list.csv` — roster-style list with enriched ship info
- `Pilot_ship_sessions.csv` — one row per pilot/ship with first/last seen timestamps
- `Instance_Summary.csv` — counts and totals by dataset/result/module
- `Pilot_Stats.csv` — per-pilot totals/count/averages by category
- `Alliance-corp_list.csv` — pilots per alliance+corp

---

## Key columns

Most combat CSVs include:

- `source_*` / `target_*` pilot, ship type, ship class, tech, hull rarity
- `module` plus module enrichment:
  - `module_tech_level` (T1/T2/T3)
  - `module_meta_level` (numeric)
  - `module_meta_group` (e.g., Tech II, Faction, Deadspace)

---

## Changelog

See `CHANGELOG.md`.

---

## Troubleshooting

### “Missing SDE CSV file(s)” on startup
Allow download when prompted, or place the required files into `./sde/` manually.

### ESI errors / rate limiting
Run with `--no-esi` or increase sleep:

```bash
python -m eve_combat_parser --sleep 0.5
```
