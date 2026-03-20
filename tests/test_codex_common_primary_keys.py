from __future__ import annotations

import pytest

from lmi_lab.impls.codex.engines.common import diff_rows


def test_diff_rows_rejects_missing_primary_key_columns() -> None:
    before_rows = [{"name": "alice"}]
    after_rows = [{"name": "alice"}]

    with pytest.raises(ValueError, match="missing primary-key columns: user_id"):
        diff_rows(before_rows, after_rows, ["user_id"])
