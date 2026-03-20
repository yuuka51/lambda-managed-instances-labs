from __future__ import annotations

from lmi_lab.runners.aws_runner import run_event


def handler(event: dict, _context) -> dict:
    return run_event(event)
