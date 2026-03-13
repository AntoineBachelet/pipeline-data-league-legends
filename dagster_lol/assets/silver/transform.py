"""Module de nettoyage et transformation de données pour les DataFrames."""

from datetime import datetime
from typing import Optional, Union
from dagster import AssetExecutionContext

import pandas as pd


def convert_to_datetime(
    context: AssetExecutionContext,
    value: Union[str, int, float],
    format_type: str = "auto",
    custom_format: Optional[str] = None,
) -> Optional[str]:
    """
    Convertit une valeur en datetime ISO string.

    Args:
        value: Valeur à convertir (string, timestamp, etc.)
        format_type: 'iso', 'timestamp', 'custom', 'auto'
        custom_format: Format strptime si format_type='custom'

    Returns:
        String ISO 8601 ou None si échec
    """
    if value is None or value == "":
        return None

    try:
        if format_type == "iso":
            value_str = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(value_str)
            return dt.isoformat()

        if format_type == "timestamp":
            dt = datetime.fromtimestamp(float(value))
            return dt.isoformat()

        if format_type == "custom":
            if not custom_format:
                raise ValueError("custom_format required when format_type='custom'")
            dt = datetime.strptime(str(value), custom_format)
            return dt.isoformat()

        return None

    except (ValueError, TypeError, OSError) as e:
        context.log.warning("Failed to convert '%s' to datetime: %s", value, e)
        return None


def standardize_types(context, df, column_types):
    """
    Standardize types from a DataFrame based on the types passed in column_types.

    Args:
        df: DataFrame containing data to be standardized
        column_types: Dict mapping column names to type configurations

    Returns:
        df: DataFrame with types standardized
    """
    df = df.copy()

    for column_name, config in column_types.items():
        if column_name not in df.columns:
            context.log.warning("Column '%s' not found in DataFrame", column_name)
            continue

        col_type = config["type"]

        if col_type == "str":
            df[column_name] = df[column_name].astype(str)

        elif col_type == "int":
            df[column_name] = (
                pd.to_numeric(df[column_name], errors="coerce").astype("Int64")
            )

        elif col_type == "bool":
            truthy = {"true", "1", "yes", "True"}
            falsy = {"false", "0", "no", "No"}
            df[column_name] = df[column_name].apply(
                lambda x: True if str(x).strip().lower() in truthy
                else False if str(x).strip().lower() in falsy
                else pd.NA
            ).astype("boolean")

        elif col_type == "datetime":
            fmt_type = config.get("format", "iso")
            fmt_custom = config.get("custom_format", None)

            df[column_name] = df[column_name].apply(
                lambda x, ft=fmt_type, cf=fmt_custom: convert_to_datetime(context, x, ft, cf)
            )

        elif col_type == "date":
            df[column_name] = pd.to_datetime(df[column_name], errors="coerce").dt.date

    return df


def deduplicate(context, df, columns):
    """
    Deduplicate a DataFrame based on one or more columns, keeping the first occurrence.

    Args:
        df: DataFrame containing data
        columns: Column name (str) or list of column names to use for deduplication

    Returns:
        df: DataFrame without duplicates
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        context.log.warning("Column(s) %s not found in DataFrame, skipping deduplication", missing)
        return df

    n_before = len(df)
    df = df.drop_duplicates(subset=columns, keep="first").reset_index(drop=True)
    n_removed = n_before - len(df)

    if n_removed:
        context.log.info(
            "Removed %d duplicate(s) based on column(s) %s",
            n_removed,
            columns,
        )

    return df


def complete_missing_values(context, df, columns):
    """
    Complete missing values in a Dataframe based on the schema passed in columns.

    Args:
        df: DataFrame containing data
        columns: Dict mapping column names to type configurations

    Returns:
        df: DataFrame with missing_values completed
    """
    df = df.copy()

    for column_name, config in columns.items():
        if column_name not in df.columns:
            context.log.warning("Column '%s' not found in DataFrame", column_name)
            continue
        if config["missing_value"] is not None:
            df[column_name] = df[column_name].fillna(config["missing_value"])

    return df