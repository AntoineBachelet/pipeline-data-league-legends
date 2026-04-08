"""Module de nettoyage et transformation de données pour les DataFrames."""

import re
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


_REGION_MARKER = re.compile(r"'''([A-Z0-9]+)(?::'''|''':)", re.IGNORECASE)
_PER_ACCOUNT_REGION = re.compile(r"^(.+?)\s*\(([A-Z0-9]+)\)\s*$", re.IGNORECASE)


def _parse_account_token(token: str, region: str) -> Optional[dict]:
    """Parse a single account token into a structured dict.

    Handles:
    - Per-account region suffix: "Alderiate (EUW)"
    - Riot tagline: "KC Caliste#0001"
    - Plain name: "KuroiHoshi"
    """
    token = token.strip()
    if not token:
        return None

    per_account_match = _PER_ACCOUNT_REGION.match(token)
    if per_account_match:
        name_part = per_account_match.group(1).strip()
        region = per_account_match.group(2).upper()
    else:
        name_part = token

    if "#" in name_part:
        game_name, tag_line = name_part.split("#", 1)
        game_name = game_name.strip()
        tag_line = tag_line.strip() or None
    else:
        game_name = name_part.strip()
        tag_line = None

    if not game_name:
        return None

    return {
        "region": region,
        "game_name": game_name,
        "tag_line": tag_line,
        "full_id": f"{game_name}#{tag_line}" if tag_line else game_name,
    }


def parse_soloqueue_ids(raw: str, default_region: str = "EUW", lolpros_account: Optional[dict] = None) -> list[dict]:
    """Parse a raw soloqueue_ids string into a list of structured account dicts.

    Handles the following input formats from Leaguepedia:
    - Region headers:  '''EUW:''' KC NEXT ADKING#EUW <br> '''KR:''' KC Caliste#0001
    - Region in parens: Alderiate (EUW), Adreitael (EUW)
    - Mixed comma/br:  '''EUW:''' Supmass 113, lpl aggression7
    - No region:       KuroiHoshi, XIII Ghost  (defaults to EUW)

    Returns a list of dicts with keys: region, game_name, tag_line, full_id.
    """
    if (not raw or not raw.strip()) and not lolpros_account:
        return []
    text = re.sub(r"\s*<br\s*/?>\s*", ",", raw or "", flags=re.IGNORECASE)

    parts = _REGION_MARKER.split(text)
    # parts[0]           = text before first region marker (may be empty)
    # parts[1], parts[2] = region, accounts chunk
    # parts[3], parts[4] = next region, accounts chunk, …

    accounts = []

    if len(parts) == 1:
        # No region markers — parse whole string with default region
        for token in parts[0].split(","):
            account = _parse_account_token(token, default_region)
            if account:
                accounts.append(account)
    else:
        # Text before first marker (rare but possible)
        if parts[0].strip().strip(","):
            for token in parts[0].split(","):
                account = _parse_account_token(token, default_region)
                if account:
                    accounts.append(account)

        for i in range(1, len(parts), 2):
            region = parts[i].upper()
            chunk = parts[i + 1] if i + 1 < len(parts) else ""
            for token in chunk.split(","):
                account = _parse_account_token(token, region)
                if account:
                    accounts.append(account)

    if lolpros_account:
        summoner_name = lolpros_account.get("summoner_name", "")
        if "#" in summoner_name:
            game_name, tag_line = summoner_name.split("#", 1)
        else:
            game_name, tag_line = summoner_name, None

        already_present = any(
            a["game_name"] == game_name and a["tag_line"] == tag_line
            for a in accounts
        )
        if not already_present:
            accounts.append({
                "region": lolpros_account.get("server", default_region).upper(),
                "game_name": game_name,
                "tag_line": tag_line,
                "full_id": f"{game_name}#{tag_line}" if tag_line else game_name,
            })

    return accounts


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
