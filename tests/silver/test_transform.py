import pytest
import pandas as pd
from unittest.mock import MagicMock

from dagster_lol.assets.silver.transform import (
    convert_to_datetime,
    deduplicate,
    standardize_types,
    complete_missing_values,
)


@pytest.fixture
def ctx():
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


# ---------------------------------------------------------------------------
# convert_to_datetime
# ---------------------------------------------------------------------------

class TestConvertToDatetime:
    def test_iso_valid(self, ctx):
        assert convert_to_datetime(ctx, "2024-01-15T10:30:00", "iso") == "2024-01-15T10:30:00"

    def test_iso_with_z_suffix(self, ctx):
        result = convert_to_datetime(ctx, "2024-01-15T10:30:00Z", "iso")
        assert result is not None

    def test_returns_none_for_empty_string(self, ctx):
        assert convert_to_datetime(ctx, "", "iso") is None

    def test_returns_none_for_none(self, ctx):
        assert convert_to_datetime(ctx, None, "iso") is None

    def test_invalid_value_returns_none_and_warns(self, ctx):
        result = convert_to_datetime(ctx, "not-a-date", "iso")
        assert result is None
        ctx.log.warning.assert_called_once()

    def test_unknown_format_returns_none(self, ctx):
        assert convert_to_datetime(ctx, "2024-01-15", "auto") is None


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_removes_duplicates_keeps_first(self, ctx):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 20, 30]})
        result = deduplicate(ctx, df, ["a"])
        assert len(result) == 2
        assert result["b"].tolist() == [10, 30]

    def test_no_duplicates_unchanged(self, ctx):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = deduplicate(ctx, df, ["a"])
        assert len(result) == 3

    def test_multi_column_dedup(self, ctx):
        df = pd.DataFrame({"a": [1, 1, 1], "b": [1, 1, 2]})
        result = deduplicate(ctx, df, ["a", "b"])
        assert len(result) == 2

    def test_missing_column_skips_and_warns(self, ctx):
        df = pd.DataFrame({"a": [1, 1]})
        result = deduplicate(ctx, df, ["nonexistent"])
        assert len(result) == 2
        ctx.log.warning.assert_called_once()

    def test_resets_index(self, ctx):
        df = pd.DataFrame({"a": [1, 1, 2]})
        result = deduplicate(ctx, df, ["a"])
        assert result.index.tolist() == [0, 1]


# ---------------------------------------------------------------------------
# standardize_types
# ---------------------------------------------------------------------------

class TestStandardizeTypes:
    def test_str_type(self, ctx):
        df = pd.DataFrame({"col": [1, 2]})
        result = standardize_types(ctx, df, {"col": {"type": "str", "missing_value": None}})
        assert pd.api.types.is_string_dtype(result["col"])

    def test_int_valid(self, ctx):
        df = pd.DataFrame({"col": ["1", "2"]})
        result = standardize_types(ctx, df, {"col": {"type": "int", "missing_value": 0}})
        assert result["col"][0] == 1
        assert result["col"][1] == 2

    def test_int_invalid_becomes_na(self, ctx):
        df = pd.DataFrame({"col": ["abc"]})
        result = standardize_types(ctx, df, {"col": {"type": "int", "missing_value": 0}})
        assert pd.isna(result["col"][0])

    def test_bool_truthy_values(self, ctx):
        df = pd.DataFrame({"col": ["1", "true", "True", "yes"]})
        result = standardize_types(ctx, df, {"col": {"type": "bool", "missing_value": False}})
        assert all(result["col"] == True)

    def test_bool_falsy_values(self, ctx):
        df = pd.DataFrame({"col": ["0", "false", "False", "no"]})
        result = standardize_types(ctx, df, {"col": {"type": "bool", "missing_value": False}})
        assert all(result["col"] == False)

    def test_bool_unknown_becomes_na(self, ctx):
        df = pd.DataFrame({"col": ["maybe"]})
        result = standardize_types(ctx, df, {"col": {"type": "bool", "missing_value": False}})
        assert pd.isna(result["col"][0])

    def test_date_valid(self, ctx):
        from datetime import date
        df = pd.DataFrame({"col": ["2024-01-15"]})
        result = standardize_types(ctx, df, {"col": {"type": "date", "missing_value": None}})
        assert result["col"][0] == date(2024, 1, 15)

    def test_date_invalid_becomes_nat(self, ctx):
        df = pd.DataFrame({"col": ["not-a-date"]})
        result = standardize_types(ctx, df, {"col": {"type": "date", "missing_value": None}})
        assert pd.isna(result["col"][0])

    def test_missing_column_warns(self, ctx):
        df = pd.DataFrame({"col": [1]})
        standardize_types(ctx, df, {"nonexistent": {"type": "str", "missing_value": None}})
        ctx.log.warning.assert_called_once()

    def test_does_not_mutate_input(self, ctx):
        df = pd.DataFrame({"col": [1, 2]})
        original = df.copy()
        standardize_types(ctx, df, {"col": {"type": "str", "missing_value": None}})
        pd.testing.assert_frame_equal(df, original)


# ---------------------------------------------------------------------------
# complete_missing_values
# ---------------------------------------------------------------------------

class TestCompleteMissingValues:
    def test_fills_non_none_missing_value(self, ctx):
        df = pd.DataFrame({"col": [1.0, None, 3.0]})
        result = complete_missing_values(ctx, df, {"col": {"type": "int", "missing_value": 0}})
        assert result["col"][1] == 0

    def test_does_not_fill_when_missing_value_is_none(self, ctx):
        df = pd.DataFrame({"col": [1.0, None, 3.0]})
        result = complete_missing_values(ctx, df, {"col": {"type": "str", "missing_value": None}})
        assert pd.isna(result["col"][1])

    def test_fills_bool_na(self, ctx):
        df = pd.DataFrame({"col": pd.array([True, pd.NA, False], dtype="boolean")})
        result = complete_missing_values(ctx, df, {"col": {"type": "bool", "missing_value": False}})
        assert result["col"][1] == False

    def test_missing_column_warns(self, ctx):
        df = pd.DataFrame({"col": [1]})
        complete_missing_values(ctx, df, {"nonexistent": {"type": "str", "missing_value": "N/A"}})
        ctx.log.warning.assert_called_once()

    def test_does_not_mutate_input(self, ctx):
        df = pd.DataFrame({"col": [1.0, None]})
        original = df.copy()
        complete_missing_values(ctx, df, {"col": {"type": "int", "missing_value": 0}})
        pd.testing.assert_frame_equal(df, original)
