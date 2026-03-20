"""Tests for Codex common module functions."""

from pathlib import Path

import pytest

from lmi_lab.impls.codex.engines.common import normalize_rows, rows_from_tsv


def test_rows_from_tsv_basic():
    """Test that rows_from_tsv can read a TSV file and return a list of dictionaries."""
    path = Path("tests/fixtures/before_small.tsv")
    rows = rows_from_tsv(path)
    
    # Should return a list
    assert isinstance(rows, list)
    
    # Should have at least one row
    assert len(rows) > 0
    
    # Each row should be a dictionary
    assert all(isinstance(row, dict) for row in rows)
    
    # Each value should be a string
    for row in rows:
        assert all(isinstance(v, str) for v in row.values())


def test_rows_from_tsv_with_custom_delimiter():
    """Test that rows_from_tsv can use a custom delimiter."""
    # Create a temporary CSV file with comma delimiter
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        f.write("col1,col2,col3\n")
        f.write("a,b,c\n")
        f.write("d,e,f\n")
        temp_path = Path(f.name)
    
    try:
        rows = rows_from_tsv(temp_path, delimiter=",")
        assert len(rows) == 2
        assert rows[0] == {"col1": "a", "col2": "b", "col3": "c"}
        assert rows[1] == {"col1": "d", "col2": "e", "col3": "f"}
    finally:
        temp_path.unlink()


def test_rows_from_tsv_empty_file():
    """Test that rows_from_tsv handles empty files correctly."""
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False, newline='') as f:
        f.write("col1\tcol2\tcol3\n")
        temp_path = Path(f.name)
    
    try:
        rows = rows_from_tsv(temp_path)
        assert rows == []
    finally:
        temp_path.unlink()


def test_normalize_rows_excludes_deleted():
    """Test that normalize_rows excludes rows with op_type='D'."""
    rows = [
        {"user_id": "user1", "tags": "a|b", "op_type": "I"},
        {"user_id": "user2", "tags": "c|d", "op_type": "D"},
        {"user_id": "user3", "tags": "e|f", "op_type": "U"},
    ]
    
    normalized = normalize_rows(rows, "A")
    
    # Should exclude the row with op_type='D'
    assert len(normalized) == 2
    assert all(row["user_id"] != "user2" for row in normalized)


def test_normalize_rows_strictness_a():
    """Test that normalize_rows applies strictness mode A correctly."""
    rows = [
        {"user_id": "user1", "tags": "b|a", "levels": "2|1", "timestamps": "t2|t1", "op_type": "I"},
    ]
    
    normalized = normalize_rows(rows, "A")
    
    # Strictness A should sort tags, levels, and timestamps alphabetically
    assert len(normalized) == 1
    assert normalized[0]["tags"] == "a|b"
    assert normalized[0]["levels"] == "1|2"
    assert normalized[0]["timestamps"] == "t1|t2"
    # op_type 'I' should be normalized to 'U'
    assert normalized[0]["op_type"] == "U"


def test_normalize_rows_strictness_b():
    """Test that normalize_rows applies strictness mode B correctly."""
    rows = [
        {"user_id": "user1", "tags": "2|a|1", "levels": "20|10", "timestamps": "t2|t1", "op_type": "U"},
    ]
    
    normalized = normalize_rows(rows, "B")
    
    # Strictness B should sort with integers first, then strings
    assert len(normalized) == 1
    assert normalized[0]["tags"] == "1|2|a"
    assert normalized[0]["levels"] == "10|20"
    assert normalized[0]["timestamps"] == "t1|t2"
    assert normalized[0]["op_type"] == "U"


def test_normalize_rows_empty_list():
    """Test that normalize_rows handles empty list correctly."""
    rows = []
    normalized = normalize_rows(rows, "A")
    assert normalized == []


def test_normalize_rows_all_deleted():
    """Test that normalize_rows returns empty list when all rows are deleted."""
    rows = [
        {"user_id": "user1", "tags": "a|b", "op_type": "D"},
        {"user_id": "user2", "tags": "c|d", "op_type": "D"},
    ]
    
    normalized = normalize_rows(rows, "A")
    assert normalized == []


def test_build_key_basic():
    """Test that build_key constructs composite keys correctly."""
    from lmi_lab.impls.codex.engines.common import build_key
    
    row = {"user_id": "user1", "tags": "a|b", "levels": "1|2"}
    primary_keys = ["user_id"]
    
    key = build_key(row, primary_keys)
    assert key == "user1"


def test_build_key_multiple_keys():
    """Test that build_key handles multiple primary keys."""
    from lmi_lab.impls.codex.engines.common import build_key
    
    row = {"user_id": "user1", "region": "us-east", "tags": "a|b"}
    primary_keys = ["user_id", "region"]
    
    key = build_key(row, primary_keys)
    assert key == "user1||us-east"


def test_build_key_missing_key():
    """Test that build_key raises ValueError when primary key is missing."""
    from lmi_lab.impls.codex.engines.common import build_key
    
    row = {"user_id": "user1", "tags": "a|b"}
    primary_keys = ["user_id", "region"]
    
    with pytest.raises(ValueError, match="Row is missing primary-key columns: region"):
        build_key(row, primary_keys)


def test_build_key_empty_values():
    """Test that build_key handles empty string values correctly."""
    from lmi_lab.impls.codex.engines.common import build_key
    
    row = {"user_id": "", "region": "us-east"}
    primary_keys = ["user_id", "region"]
    
    key = build_key(row, primary_keys)
    assert key == "||us-east"


def test_build_key_special_characters():
    """Test that build_key handles special characters in values."""
    from lmi_lab.impls.codex.engines.common import build_key
    
    row = {"user_id": "user|1", "region": "us||east"}
    primary_keys = ["user_id", "region"]
    
    key = build_key(row, primary_keys)
    assert key == "user|1||us||east"


def test_diff_rows_insert():
    """Test that diff_rows detects INSERT status for keys only in after."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b"},
    ]
    after_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have one INSERT for user2
    assert len(diffs) == 1
    assert diffs[0]["key"] == "user2"
    assert diffs[0]["status"] == "INSERT"
    assert diffs[0]["changed_columns"] == ""


def test_diff_rows_delete():
    """Test that diff_rows detects DELETE status for keys only in before."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
    ]
    after_rows = [
        {"user_id": "user1", "tags": "a|b"},
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have one DELETE for user2
    assert len(diffs) == 1
    assert diffs[0]["key"] == "user2"
    assert diffs[0]["status"] == "DELETE"
    assert diffs[0]["changed_columns"] == ""


def test_diff_rows_update():
    """Test that diff_rows detects UPDATE status and changed columns."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b", "levels": "1|2"},
    ]
    after_rows = [
        {"user_id": "user1", "tags": "c|d", "levels": "1|2"},
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have one UPDATE for user1 with tags changed
    assert len(diffs) == 1
    assert diffs[0]["key"] == "user1"
    assert diffs[0]["status"] == "UPDATE"
    assert diffs[0]["changed_columns"] == "tags"


def test_diff_rows_update_multiple_columns():
    """Test that diff_rows records multiple changed columns separated by pipe."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b", "levels": "1|2", "timestamps": "t1|t2"},
    ]
    after_rows = [
        {"user_id": "user1", "tags": "c|d", "levels": "3|4", "timestamps": "t1|t2"},
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have one UPDATE with tags and levels changed
    assert len(diffs) == 1
    assert diffs[0]["key"] == "user1"
    assert diffs[0]["status"] == "UPDATE"
    # Changed columns should be sorted and pipe-separated
    assert diffs[0]["changed_columns"] == "levels|tags"


def test_diff_rows_no_changes():
    """Test that diff_rows returns empty list when rows are identical."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
    ]
    after_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have no differences
    assert diffs == []


def test_diff_rows_mixed_operations():
    """Test that diff_rows handles INSERT, DELETE, and UPDATE together."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
        {"user_id": "user3", "tags": "e|f"},
    ]
    after_rows = [
        {"user_id": "user1", "tags": "a|b"},  # No change
        {"user_id": "user3", "tags": "g|h"},  # Update
        {"user_id": "user4", "tags": "i|j"},  # Insert
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have 3 differences: DELETE user2, UPDATE user3, INSERT user4
    # Results should be sorted by key
    assert len(diffs) == 3
    assert diffs[0]["key"] == "user2"
    assert diffs[0]["status"] == "DELETE"
    assert diffs[1]["key"] == "user3"
    assert diffs[1]["status"] == "UPDATE"
    assert diffs[1]["changed_columns"] == "tags"
    assert diffs[2]["key"] == "user4"
    assert diffs[2]["status"] == "INSERT"


def test_diff_rows_empty_before():
    """Test that diff_rows handles empty before_rows (all inserts)."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = []
    after_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
    ]
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have two INSERTs
    assert len(diffs) == 2
    assert all(d["status"] == "INSERT" for d in diffs)


def test_diff_rows_empty_after():
    """Test that diff_rows handles empty after_rows (all deletes)."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "tags": "a|b"},
        {"user_id": "user2", "tags": "c|d"},
    ]
    after_rows = []
    primary_keys = ["user_id"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have two DELETEs
    assert len(diffs) == 2
    assert all(d["status"] == "DELETE" for d in diffs)


def test_diff_rows_composite_key():
    """Test that diff_rows works with composite primary keys."""
    from lmi_lab.impls.codex.engines.common import diff_rows
    
    before_rows = [
        {"user_id": "user1", "region": "us-east", "tags": "a|b"},
    ]
    after_rows = [
        {"user_id": "user1", "region": "us-east", "tags": "c|d"},
        {"user_id": "user1", "region": "us-west", "tags": "e|f"},
    ]
    primary_keys = ["user_id", "region"]
    
    diffs = diff_rows(before_rows, after_rows, primary_keys)
    
    # Should have one UPDATE and one INSERT
    assert len(diffs) == 2
    assert diffs[0]["key"] == "user1||us-east"
    assert diffs[0]["status"] == "UPDATE"
    assert diffs[1]["key"] == "user1||us-west"
    assert diffs[1]["status"] == "INSERT"
