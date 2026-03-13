from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

from ...ressources.snowflake import SnowflakeResource
from .leaguepedia import (
    TOURNAMENTS_SNOWFLAKE_TABLE,
    PLAYERS_SNOWFLAKE_TABLE,
    TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE,
)
from .schemas import TournamentsSchema, PlayersSchema, TournamentRostersSchema


def _count_duplicates(snowflake: SnowflakeResource, table: str, keys: list[str]) -> int:
    cols = ", ".join(keys)
    sql = f"""
        SELECT COUNT(*) FROM (
            SELECT {cols}, COUNT(*) AS cnt
            FROM {table}
            GROUP BY {cols}
            HAVING cnt > 1
        )
    """
    with snowflake.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]


def _count_unexpected_nulls(
    snowflake: SnowflakeResource, table: str, schema: dict
) -> tuple[int, list[str]]:
    non_nullable = [col for col, meta in schema.items() if meta["missing_value"] is not None]
    if not non_nullable:
        return 0, []
    conditions = " OR ".join(f"{col} IS NULL" for col in non_nullable)
    sql = f"SELECT COUNT(*) FROM {table} WHERE {conditions}"
    with snowflake.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0], non_nullable


# --- Tournaments ---

@asset_check(asset="tournaments_silver", description="No duplicate overview_page in tournaments_silver")
def tournaments_no_duplicates(snowflake: SnowflakeResource) -> AssetCheckResult:
    count = _count_duplicates(snowflake, TOURNAMENTS_SNOWFLAKE_TABLE, ["overview_page"])
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_groups": count},
    )


@asset_check(asset="tournaments_silver", description="Non-nullable columns have no NULLs in tournaments_silver")
def tournaments_no_nulls(snowflake: SnowflakeResource) -> AssetCheckResult:
    count, cols = _count_unexpected_nulls(
        snowflake, TOURNAMENTS_SNOWFLAKE_TABLE, TournamentsSchema.TOURNAMENTS_SCHEMA
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_rows": count, "checked_columns": str(cols)},
    )


# --- Players ---

@asset_check(asset="players_silver", description="No duplicate overview_page in players_silver")
def players_no_duplicates(snowflake: SnowflakeResource) -> AssetCheckResult:
    count = _count_duplicates(snowflake, PLAYERS_SNOWFLAKE_TABLE, ["overview_page"])
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_groups": count},
    )


@asset_check(asset="players_silver", description="Non-nullable columns have no NULLs in players_silver")
def players_no_nulls(snowflake: SnowflakeResource) -> AssetCheckResult:
    count, cols = _count_unexpected_nulls(
        snowflake, PLAYERS_SNOWFLAKE_TABLE, PlayersSchema.PLAYERS_SCHEMA
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_rows": count, "checked_columns": str(cols)},
    )


# --- Tournament Rosters ---

@asset_check(
    asset="tournament_rosters_silver",
    description="No duplicate (team, overview_page, player_link) in tournament_rosters_silver",
)
def tournament_rosters_no_duplicates(snowflake: SnowflakeResource) -> AssetCheckResult:
    count = _count_duplicates(
        snowflake, TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE, ["team", "overview_page", "player_link"]
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_groups": count},
    )


@asset_check(
    asset="tournament_rosters_silver",
    description="Non-nullable columns have no NULLs in tournament_rosters_silver",
)
def tournament_rosters_no_nulls(snowflake: SnowflakeResource) -> AssetCheckResult:
    count, cols = _count_unexpected_nulls(
        snowflake,
        TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE,
        TournamentRostersSchema.TOURNAMENT_ROSTERS_SCHEMA,
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_rows": count, "checked_columns": str(cols)},
    )


silver_checks = [
    tournaments_no_duplicates,
    tournaments_no_nulls,
    players_no_duplicates,
    players_no_nulls,
    tournament_rosters_no_duplicates,
    tournament_rosters_no_nulls,
]
