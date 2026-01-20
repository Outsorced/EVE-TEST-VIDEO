from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PromptConfig:
    """Controls interactive prompting.

    - assume_yes: if True, all yes/no prompts return True.
    - non_interactive: if True, any prompt that would require user input raises.
      (Useful for CI / unattended runs where you want failures instead of blocks.)
    """

    assume_yes: bool = False
    non_interactive: bool = False


class Prompter:
    def __init__(self, cfg: PromptConfig) -> None:
        # Historical naming: some modules referenced `prompter.config`.
        # Keep both to avoid breaking older code.
        self.cfg = cfg
        self.config = cfg

    def confirm(self, question: str, default: bool = False) -> bool:
        """Ask a y/n question."""
        if self.cfg.assume_yes:
            return True
        if self.cfg.non_interactive:
            raise RuntimeError(f"Non-interactive mode: prompt blocked: {question}")

        suf = "[Y/n]" if default else "[y/N]"
        ans = input(f"{question} {suf}: ").strip().lower()
        if not ans:
            return default
        return ans.startswith("y")

    def choice(self, question: str, choices: dict[str, str], default: str) -> str:
        """Ask for a choice key, returning the key."""
        if self.cfg.assume_yes:
            return default
        if self.cfg.non_interactive:
            raise RuntimeError(f"Non-interactive mode: prompt blocked: {question}")

        # Render in stable order
        items = ", ".join([f"{k}={v}" for k, v in choices.items()])
        ans = input(f"{question} ({items}) [default={default}]: ").strip().lower()
        if not ans:
            return default
        return ans if ans in choices else default
