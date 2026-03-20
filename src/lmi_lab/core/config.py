from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class RunConfig:
    before: Path
    after: Path
    outdir: Path
    engines: list[str] = field(default_factory=lambda: ["duckdb"])
    strictness: str = "A"
    primary_keys: list[str] = field(default_factory=lambda: ["user_id"])
    list_columns: list[str] = field(default_factory=lambda: ["tags", "levels", "timestamps"])
    delimiter: str = "\t"
    action: str = "compare"
    memray: bool = False

    def validate(self) -> None:
        strict = self.strictness.upper()
        if strict not in {"A", "B"}:
            raise ValueError(f"strictness must be A|B: {self.strictness}")
        self.strictness = strict


@dataclass(slots=True)
class GenerateConfig:
    output: Path
    rows: int = 100
    seed: int = 7
