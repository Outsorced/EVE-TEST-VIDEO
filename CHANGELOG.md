# Changelog

All notable changes to this project will be documented in this file.

The format follows *Keep a Changelog* (https://keepachangelog.com/) and this project uses
semantic versioning with pre-release tags.

## [0.6.0-beta.2.post8] - 2026-01-20
### Added
- Module enrichment columns in combat outputs:
  - `module_tech_level` (T1/T2/T3)
  - `module_meta_level` (numeric)
  - `module_meta_group` (e.g., Tech II, Faction, Deadspace)
- Auto-download and loading of `dgmTypeAttributes.csv` to compute module meta level.
- `requirements.txt` (recommended dependency: `requests` for ESI).
- Improved README with install, data sources, and outputs overview.

## [0.6.0-beta.2.post6] - 2026-01-20
### Changed
- Ship tech level and hull rarity derived from SDE meta groups (`invMetaTypes`/`invMetaGroups`).

## [0.6.0-beta.2.post5] - 2026-01-20
### Added
- `Pilot_ship_sessions.csv` (pilot+ship first/last seen, duration, counts).
- Filter drones out of Pilot_list ship types; keep fighters in separate column.

## [0.6.0-beta.2] - 2026-01-20
### Added
- Per-fight combined outputs and per-player filtered outputs.
- Split EWAR into effects vs capacitor warfare.
- Propulsion jamming attempts output with cross-log dedupe and provenance.
- Instance summary and pilot stats rollups.
