"""EVE Combat Log Parser.

This package is a modular refactor of the original single-file script.

Entry point:

  python -m eve_combat_parser --log-folder ./logs
"""

from .version import __version__

__all__ = ["__version__"]
