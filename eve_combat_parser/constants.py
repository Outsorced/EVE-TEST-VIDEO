from __future__ import annotations

TS_FMT = "%Y.%m.%d %H:%M:%S"

# Defaults / Config
DEFAULT_LOG_FOLDER = "./logs"

DEFAULT_CACHE_FILE = "esi_cache.json"

DEFAULT_AFF_LOG_DB_FILE = "affiliations_from_logs.json"
DEFAULT_AFF_ESI_DB_FILE = "affiliations_from_esi.json"

DEFAULT_SDE_DIR = "./sde"
INV_TYPES_CSV = "invTypes-nodescription.csv"
INV_GROUPS_CSV = "invGroups.csv"
INV_META_TYPES_CSV = "invMetaTypes.csv"
INV_META_GROUPS_CSV = "invMetaGroups.csv"
# Attributes for meta level (modules/charges/etc.)
DGM_TYPE_ATTRIBUTES_CSV = "dgmTypeAttributes.csv"
FUZZWORK_BASE = "https://www.fuzzwork.co.uk/dump/latest"

# Optional: ship type enrichment. We primarily rely on invTypes-nodescription.csv
# (user-updatable), and can optionally backfill missing details via ESI.
#
# Note: invTypes does NOT contain group names, only groupID. Group names are
# fetched via ESI (cached) when available.
SHIP_CLASS_UNKNOWN = "UNKNOWN"

DEFAULT_ESI_SLEEP = 0.2

# Output files (split into player vs NPC)
OUT_REPAIRS_DONE_PLAYERS = "repairs_done_players.csv"
OUT_REPAIRS_DONE_NPC = "repairs_done_npc.csv"
OUT_REPAIRS_DONE_DRONES = "repairs_done_drones.csv"
OUT_REPAIRS_DONE_CHARGES = "repairs_done_charges.csv"

OUT_REPAIRS_RECEIVED_PLAYERS = "repairs_received_players.csv"
OUT_REPAIRS_RECEIVED_NPC = "repairs_received_npc.csv"
OUT_REPAIRS_RECEIVED_DRONES = "repairs_received_drones.csv"
OUT_REPAIRS_RECEIVED_CHARGES = "repairs_received_charges.csv"

OUT_DAMAGE_DONE_PLAYERS = "damage_done_players.csv"
OUT_DAMAGE_DONE_NPC = "damage_done_npc.csv"
OUT_DAMAGE_DONE_DRONES = "damage_done_drones.csv"
OUT_DAMAGE_DONE_CHARGES = "damage_done_charges.csv"

OUT_DAMAGE_RECEIVED_PLAYERS = "damage_received_players.csv"
OUT_DAMAGE_RECEIVED_NPC = "damage_received_npc.csv"
OUT_DAMAGE_RECEIVED_DRONES = "damage_received_drones.csv"
OUT_DAMAGE_RECEIVED_CHARGES = "damage_received_charges.csv"

OUT_EWAR_DONE_PLAYERS = "ewar_done_players.csv"
OUT_EWAR_DONE_NPC = "ewar_done_npc.csv"
OUT_EWAR_DONE_DRONES = "ewar_done_drones.csv"
OUT_EWAR_DONE_CHARGES = "ewar_done_charges.csv"

OUT_EWAR_RECEIVED_PLAYERS = "ewar_received_players.csv"
OUT_EWAR_RECEIVED_NPC = "ewar_received_npc.csv"
OUT_EWAR_RECEIVED_DRONES = "ewar_received_drones.csv"
OUT_EWAR_RECEIVED_CHARGES = "ewar_received_charges.csv"

# Split EWAR outputs
OUT_EWAR_EFFECTS_DONE_PLAYERS = "ewar_effects_done_players.csv"
OUT_EWAR_EFFECTS_DONE_NPC = "ewar_effects_done_npc.csv"
OUT_EWAR_EFFECTS_DONE_DRONES = "ewar_effects_done_drones.csv"
OUT_EWAR_EFFECTS_DONE_CHARGES = "ewar_effects_done_charges.csv"

OUT_EWAR_EFFECTS_RECEIVED_PLAYERS = "ewar_effects_received_players.csv"
OUT_EWAR_EFFECTS_RECEIVED_NPC = "ewar_effects_received_npc.csv"
OUT_EWAR_EFFECTS_RECEIVED_DRONES = "ewar_effects_received_drones.csv"
OUT_EWAR_EFFECTS_RECEIVED_CHARGES = "ewar_effects_received_charges.csv"

OUT_CAP_WARFARE_DONE_PLAYERS = "cap_warfare_done_players.csv"
OUT_CAP_WARFARE_DONE_NPC = "cap_warfare_done_npc.csv"
OUT_CAP_WARFARE_DONE_DRONES = "cap_warfare_done_drones.csv"
OUT_CAP_WARFARE_DONE_CHARGES = "cap_warfare_done_charges.csv"

OUT_CAP_WARFARE_RECEIVED_PLAYERS = "cap_warfare_received_players.csv"
OUT_CAP_WARFARE_RECEIVED_NPC = "cap_warfare_received_npc.csv"
OUT_CAP_WARFARE_RECEIVED_DRONES = "cap_warfare_received_drones.csv"
OUT_CAP_WARFARE_RECEIVED_CHARGES = "cap_warfare_received_charges.csv"

# Remote capacitor transfer (positive, like repairs)
OUT_CAPACITOR_DONE_PLAYERS = "capacitor_done_players.csv"
OUT_CAPACITOR_DONE_NPC = "capacitor_done_npc.csv"
OUT_CAPACITOR_DONE_DRONES = "capacitor_done_drones.csv"
OUT_CAPACITOR_DONE_CHARGES = "capacitor_done_charges.csv"

OUT_CAPACITOR_RECEIVED_PLAYERS = "capacitor_received_players.csv"
OUT_CAPACITOR_RECEIVED_NPC = "capacitor_received_npc.csv"
OUT_CAPACITOR_RECEIVED_DRONES = "capacitor_received_drones.csv"
OUT_CAPACITOR_RECEIVED_CHARGES = "capacitor_received_charges.csv"
OUT_OTHERS = "others.csv"

# Propulsion jamming attempts (warp scramble/disruption attempt)
OUT_PROPULSION_JAM_ATTEMPTS = "Propulsion_Jamming_attempts.csv"

# Unified CSV headers (same for combat exports)
HEADERS = [
    "timestamp",
    "amount",
    "log_listener",
    "fight_id",
    "source_pilot",
    "source_ship_type",
    "source_ship_class",
    "source_ship_tech",
    "source_hull_rarity",
    "source_alliance",
    "source_corp",
    "source_kind",
    "target_pilot",
    "target_ship_type",
    "target_ship_class",
    "target_ship_tech",
    "target_hull_rarity",
    "target_alliance",
    "target_corp",
    "target_kind",
    "module",
    "module_tech_level",
    "module_meta_level",
    "module_meta_group",
    "result",
    "Log_file_original_line",
    "Log_file_line_number",
    "log_file",
]

OTHERS_HEADERS = [
    "timestamp",
    "channel",
    "log_listener",
    "text",
    "Log_file_original_line",
    "Log_file_line_number",
    "log_file",
]


# Propulsion jamming attempts (deduped across logs)
PROP_JAM_HEADERS = [
    "timestamp",
    "log_listener",
    "fight_id",
    "source_pilot",
    "source_ship_type",
    "source_ship_class",
    "source_ship_tech",
    "source_hull_rarity",
    "source_alliance",
    "source_corp",
    "source_kind",
    "target_pilot",
    "target_ship_type",
    "target_ship_class",
    "target_ship_tech",
    "target_hull_rarity",
    "target_alliance",
    "target_corp",
    "target_kind",
    "attempt_type",
    "module",
    "module_tech_level",
    "module_meta_level",
    "module_meta_group",
    "result",
    "origin_count",
    "origin_log_files",
    "origin_line_numbers",
    "Log_file_original_line",
]
