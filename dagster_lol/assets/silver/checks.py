import os
import yaml
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity, AssetCheckExecutionContext, AssetKey, TableSchemaMetadataValue

from ...ressources.snowflake import SnowflakeResource
from .leaguepedia import (
    TOURNAMENTS_SNOWFLAKE_TABLE,
    PLAYERS_SNOWFLAKE_TABLE,
    TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE,
    PLAYER_SOLOQUEUE_ACCOUNTS_SNOWFLAKE_TABLE,
)
from .schemas import TournamentsSchema, PlayersSchema, TournamentRostersSchema, PlayerSoloqueueAccountsSchema

_CONTRACTS_DIR = os.path.join(os.path.dirname(__file__), "contracts")

# Dagster/pandas type names → datacontract.com type names
_DAGSTER_TO_CONTRACT_TYPE: dict[str, str] = {
    "str": "string",
    "object": "date",       # pandas stores datetime.date values as dtype=object
    "int64": "integer",
    "Int64": "integer",
    "float64": "number",
    "Float64": "number",
    "bool": "boolean",
    "boolean": "boolean",
    "datetime64[ns]": "timestamp",
}


def _normalize_type(dagster_type: str) -> str:
    return _DAGSTER_TO_CONTRACT_TYPE.get(dagster_type, dagster_type)


def _check_schema_against_contract(
    context: AssetCheckExecutionContext,
    asset_key: str,
    contract_file: str,
    model_name: str,
) -> AssetCheckResult:
    """Compare dagster/column_schema metadata from the latest materialization against a DataContract YAML.

    Args:
        context: Asset check execution context.
        asset_key: Dagster asset key string (e.g. "tournaments_silver").
        contract_file: Filename of the contract YAML in the contracts/ directory.
        model_name: Key under `models:` in the YAML (e.g. "leaguepedia_tournaments").
    """
    contract_path = os.path.join(_CONTRACTS_DIR, contract_file)
    try:
        with open(contract_path) as f:
            contract = yaml.safe_load(f)
    except FileNotFoundError:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": f"Data contract not found: {contract_path}"},
        )

    latest = context.instance.get_latest_materialization_event(AssetKey([asset_key]))
    if not latest:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": "No materialization found for asset"},
        )

    schema_meta = (
        latest.asset_materialization.metadata.get("dagster/column_schema")
        if latest.asset_materialization
        else None
    )
    if not schema_meta or not isinstance(schema_meta, TableSchemaMetadataValue):
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": "No dagster/column_schema metadata on latest materialization"},
        )

    actual = {col.name: _normalize_type(col.type) for col in schema_meta.value.columns}
    expected = {
        col_name: col_info.get("type", "unknown")
        for col_name, col_info in contract["models"][model_name]["fields"].items()
    }

    missing = [c for c in expected if c not in actual]
    extra = [c for c in actual if c not in expected]
    mismatches = [
        {"column": c, "expected": expected[c], "actual": actual[c]}
        for c in expected
        if c in actual and actual[c] != expected[c]
    ]

    passed = not missing and not extra and not mismatches
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "mismatches": str(mismatches),
            "missing_columns": str(missing),
            "extra_columns": str(extra),
        },
    )


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


# --- Player Soloqueue Accounts ---

@asset_check(
    asset="player_soloqueue_accounts_silver",
    description="No duplicate (player_overview_page, region, full_id) in player_soloqueue_accounts_silver",
)
def player_soloqueue_accounts_no_duplicates(snowflake: SnowflakeResource) -> AssetCheckResult:
    count = _count_duplicates(
        snowflake,
        PLAYER_SOLOQUEUE_ACCOUNTS_SNOWFLAKE_TABLE,
        ["player_overview_page", "region", "full_id"],
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_groups": count},
    )


@asset_check(
    asset="player_soloqueue_accounts_silver",
    description="Non-nullable columns have no NULLs in player_soloqueue_accounts_silver",
)
def player_soloqueue_accounts_no_nulls(snowflake: SnowflakeResource) -> AssetCheckResult:
    count, cols = _count_unexpected_nulls(
        snowflake,
        PLAYER_SOLOQUEUE_ACCOUNTS_SNOWFLAKE_TABLE,
        PlayerSoloqueueAccountsSchema.PLAYER_SOLOQUEUE_ACCOUNTS_SCHEMA,
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_rows": count, "checked_columns": str(cols)},
    )


@asset_check(asset="tournaments_silver", description="Schema matches data contract")
def tournaments_schema_contract(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _check_schema_against_contract(
        context,
        asset_key="tournaments_silver",
        contract_file="tournaments.yaml",
        model_name="leaguepedia_tournaments",
    )


@asset_check(asset="players_silver", description="Schema matches data contract")
def players_schema_contract(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _check_schema_against_contract(
        context,
        asset_key="players_silver",
        contract_file="players.yaml",
        model_name="leaguepedia_players",
    )


@asset_check(asset="tournament_rosters_silver", description="Schema matches data contract")
def tournament_rosters_schema_contract(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _check_schema_against_contract(
        context,
        asset_key="tournament_rosters_silver",
        contract_file="tournament_rosters.yaml",
        model_name="leaguepedia_tournament_rosters",
    )


@asset_check(asset="player_soloqueue_accounts_silver", description="Schema matches data contract")
def player_soloqueue_accounts_schema_contract(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _check_schema_against_contract(
        context,
        asset_key="player_soloqueue_accounts_silver",
        contract_file="player_soloqueue_accounts.yaml",
        model_name="leaguepedia_player_soloqueue_accounts",
    )


silver_checks = [
    tournaments_no_duplicates,
    tournaments_no_nulls,
    tournaments_schema_contract,
    players_no_duplicates,
    players_no_nulls,
    players_schema_contract,
    tournament_rosters_no_duplicates,
    tournament_rosters_no_nulls,
    tournament_rosters_schema_contract,
    player_soloqueue_accounts_no_duplicates,
    player_soloqueue_accounts_no_nulls,
    player_soloqueue_accounts_schema_contract,
]
