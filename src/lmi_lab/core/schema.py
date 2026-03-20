from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

REQUIRED_COLUMNS = ["user_id", "tags", "levels", "timestamps", "op_type"]


@dataclass(slots=True)
class CompareResult:
    engine: str
    diff_path: Path
    rows_before: int
    rows_after: int
    rows_diff: int
    elapsed_seconds: float
    peak_rss_mb: float
    success: bool = True
    reason: str = ""
    stats: dict[str, int | float | str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "engine": self.engine,
            "diff_path": str(self.diff_path),
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "rows_diff": self.rows_diff,
            "elapsed_seconds": self.elapsed_seconds,
            "peak_rss_mb": self.peak_rss_mb,
            "success": self.success,
            "reason": self.reason,
            "stats": self.stats,
        }
