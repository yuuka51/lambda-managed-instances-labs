from __future__ import annotations

import resource
import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class Timer:
    start: float = field(default_factory=time.perf_counter)

    def elapsed(self) -> float:
        return time.perf_counter() - self.start


def peak_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
