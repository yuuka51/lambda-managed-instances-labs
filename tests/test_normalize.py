from lmi_lab.core.normalize import normalize_row, should_exclude_row


def test_strictness_a_timestamp_format_is_not_equal() -> None:
    r1 = normalize_row({"timestamps": "2026-01-07 00:00:00", "tags": "", "levels": "", "op_type": "U"}, "A")
    r2 = normalize_row({"timestamps": "2026-01-07T00:00:00", "tags": "", "levels": "", "op_type": "U"}, "A")
    assert r1["timestamps"] != r2["timestamps"]


def test_strictness_b_timestamp_format_is_not_equal() -> None:
    r1 = normalize_row({"timestamps": "2026-01-07 00:00:00", "tags": "", "levels": "", "op_type": "U"}, "B")
    r2 = normalize_row({"timestamps": "2026-01-07T00:00:00", "tags": "", "levels": "", "op_type": "U"}, "B")
    assert r1["timestamps"] != r2["timestamps"]


def test_op_type_insert_is_normalized_to_update() -> None:
    row = normalize_row({"op_type": "I", "tags": "", "levels": "", "timestamps": ""}, "B")
    assert row["op_type"] == "U"


def test_op_type_delete_is_excluded() -> None:
    assert should_exclude_row({"op_type": "D"}) is True
