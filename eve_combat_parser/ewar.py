from __future__ import annotations

import re


EWAR_KEYWORDS = {
    "jammed": "ECM",
    "energy neutralized": "Energy Neutralizer",
    "neutralized": "Energy Neutralizer",
    "energy drained": "Energy Nosferatu",
    "drained": "Energy Nosferatu",
    "nosferatu": "Energy Nosferatu",
    "warp scrambled": "Warp Scrambler",
    "warp disrupted": "Warp Disruptor",
    "webbed": "Stasis Web",
    "dampened": "Sensor Dampener",
    "tracking disrupted": "Tracking Disruptor",
    "painted": "Target Painter",
}


def classify_ewar(text: str) -> str:
    t = text.lower()
    for k, v in EWAR_KEYWORDS.items():
        if k in t:
            return v
    return "Unknown EWAR"


# Special cap-warfare formats with amount
ENERGY_NEUT_AMOUNT_RE = re.compile(
    r"^(?P<amount>\d+)\s+GJ\s+energy\s+neutralized\s+(?P<entity>.+?)\s+-\s+(?P<module>.+)$",
    re.IGNORECASE,
)

ENERGY_DRAIN_TO_AMOUNT_RE = re.compile(
    r"^(?P<amount_sign>-)?(?P<amount>\d+)\s+GJ\s+energy\s+drained\s+to\s+(?P<entity>.+?)\s+-\s+(?P<module>.+)$",
    re.IGNORECASE,
)
