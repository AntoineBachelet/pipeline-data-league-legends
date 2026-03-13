from datetime import date
import pandas as pd
from dagster import asset, AssetExecutionContext, MaterializeResult, AssetDep
from ...ressources.s3 import S3Resource
from ...ressources.snowflake import SnowflakeResource
from .transform import standardize_types, complete_missing_values, deduplicate
from .schemas import TournamentsSchema, PlayersSchema, TournamentRostersSchema

TOURNAMENTS_BRONZE_PREFIX = "bronze/leaguepedia/tournaments/"
PLAYERS_BRONZE_PREFIX = "bronze/leaguepedia/players/"
TOURNAMENT_ROSTERS_BRONZE_PREFIX = "bronze/leaguepedia/tournamentrosters/"

TOURNAMENTS_SNOWFLAKE_TABLE = "silver.leaguepedia_tournaments"
PLAYERS_SNOWFLAKE_TABLE = "silver.leaguepedia_players"
TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE = "silver.leaguepedia_tournament_rosters"

PLAYERS_COLUMN_MAPPING = {
    "ID": "id",
    "OverviewPage": "overview_page",
    "Player": "player",
    "Image": "image",
    "Name": "name",
    "NativeName": "native_name",
    "NameAlphabet": "name_alphabet",
    "NameFull": "name_full",
    "Country": "country",
    "Nationality": "nationality",
    "NationalityPrimary": "nationality_primary",
    "Age": "age",
    "Birthdate": "birthdate",
    "Deathdate": "deathdate",
    "ResidencyFormer": "residency_former",
    "Team": "team",
    "Team2": "team2",
    "CurrentTeams": "current_teams",
    "TeamSystem": "team_system",
    "Team2System": "team2_system",
    "Residency": "residency",
    "Role": "role",
    "Contract": "contract",
    "ContractText": "contract_text",
    "FavChamps": "fav_champs",
    "SoloqueueIds": "soloqueue_ids",
    "Askfm": "askfm",
    "Bluesky": "bluesky",
    "Discord": "discord",
    "Facebook": "facebook",
    "Instagram": "instagram",
    "Lolpros": "lolpros",
    "DPMLOL": "dpmlol",
    "Reddit": "reddit",
    "Snapchat": "snapchat",
    "Stream": "stream",
    "KICK": "kick",
    "Twitter": "twitter",
    "Threads": "threads",
    "LinkedIn": "linkedin",
    "Vk": "vk",
    "Website": "website",
    "Weibo": "weibo",
    "Youtube": "youtube",
    "TeamLast": "team_last",
    "RoleLast": "role_last",
    "IsRetired": "is_retired",
    "ToWildrift": "to_wildrift",
    "ToValorant": "to_valorant",
    "ToTFT": "to_tft",
    "ToLegendsOfRuneterra": "to_legends_of_runeterra",
    "To2XKO": "to_2xko",
    "IsPersonality": "is_personality",
    "IsSubstitute": "is_substitute",
    "IsTrainee": "is_trainee",
    "IsLowercase": "is_lowercase",
    "IsAutoTeam": "is_auto_team",
    "IsLowContent": "is_low_content",
}

TOURNAMENT_ROSTERS_COLUMN_MAPPING = {
    "Team": "team",
    "OverviewPage": "overview_page",
    "RosterLinks": "player_link",
    "Roles": "role",
    "Region": "region",
    "Footnotes": "footnotes",
    "IsUsed": "isused",
    "IsComplete": "iscomplete",
    "PageAndTeam": "pageandteam"
}

TOURNAMENTS_COLUMN_MAPPING = {
    "Name": "name",
    "OverviewPage": "overview_page",
    "DateStart": "date_start",
    "League": "league",
    "Region": "region",
    "Prizepool": "prizepool",
    "Currency": "currency",
    "Country": "country",
    "Rulebook": "rulebook",
    "EventType": "event_type",
    "Links": "links",
    "Sponsors": "sponsors",
    "Organizer": "organizer",
    "Organizers": "organizers",
    "Split": "split",
    "IsQualifier": "is_qualifier",
    "IsPlayoffs": "is_playoffs",
    "IsOfficial": "is_official",
    "Year": "year",
}


@asset(
    description="Tournois nettoyés depuis le bronze S3 vers Snowflake",
    deps=[AssetDep("tournaments_bronze")],
)
def tournaments_silver(
    context: AssetExecutionContext,
    s3: S3Resource,
    snowflake: SnowflakeResource,
) -> MaterializeResult:
    latest_key = s3.get_latest_key(TOURNAMENTS_BRONZE_PREFIX)
    context.log.info(f"Reading latest bronze file: s3://{s3.bucket_name}/{latest_key}")

    raw_data = s3.download_json(latest_key)
    context.log.info(f"  → {len(raw_data)} total tournaments loaded from bronze")

    df = pd.DataFrame(raw_data)
    df = df[list(TOURNAMENTS_COLUMN_MAPPING.keys())].rename(columns=TOURNAMENTS_COLUMN_MAPPING)

    df = deduplicate(context, df, ["overview_page"])
    df = standardize_types(context, df, TournamentsSchema.TOURNAMENTS_SCHEMA)
    df = complete_missing_values(context, df, TournamentsSchema.TOURNAMENTS_SCHEMA)

    silver_rows = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]

    snowflake.truncate_and_insert(TOURNAMENTS_SNOWFLAKE_TABLE, silver_rows, list(TOURNAMENTS_COLUMN_MAPPING.values()))
    context.log.info(f"✅ {len(silver_rows)} rows written to Snowflake → {TOURNAMENTS_SNOWFLAKE_TABLE}")

    today = date.today().isoformat()
    return MaterializeResult(
        metadata={
            "snowflake_table": TOURNAMENTS_SNOWFLAKE_TABLE,
            "row_count": len(silver_rows),
            "source_key": latest_key,
            "ingestion_date": today,
        }
    )

@asset(
    description="Joueurs nettoyés depuis le bronze S3 vers Snowflake",
    deps=[AssetDep("players_bronze")],
)
def players_silver(
    context: AssetExecutionContext,
    s3: S3Resource,
    snowflake: SnowflakeResource,
) -> MaterializeResult:
    latest_key = s3.get_latest_key(PLAYERS_BRONZE_PREFIX)
    context.log.info(f"Reading latest bronze file: s3://{s3.bucket_name}/{latest_key}")

    raw_data = s3.download_json(latest_key)
    context.log.info(f"  → {len(raw_data)} total players loaded from bronze")

    df = pd.DataFrame(raw_data)
    df = df[list(PLAYERS_COLUMN_MAPPING.keys())].rename(columns=PLAYERS_COLUMN_MAPPING)

    df = deduplicate(context, df, ["overview_page"])
    df = standardize_types(context, df, PlayersSchema.PLAYERS_SCHEMA)
    df = complete_missing_values(context, df, PlayersSchema.PLAYERS_SCHEMA)

    silver_rows = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]

    snowflake.truncate_and_insert(PLAYERS_SNOWFLAKE_TABLE, silver_rows, list(PLAYERS_COLUMN_MAPPING.values()))
    context.log.info(f"✅ {len(silver_rows)} rows written to Snowflake → {PLAYERS_SNOWFLAKE_TABLE}")

    today = date.today().isoformat()
    return MaterializeResult(
        metadata={
            "snowflake_table": PLAYERS_SNOWFLAKE_TABLE,
            "row_count": len(silver_rows),
            "source_key": latest_key,
            "ingestion_date": today,
        }
    )


def _align_roles(row):
    """Align role list length to player_link list length.

    Pad with None when roles are missing; truncate when there are more roles than players.
    """
    players = [p for p in row["player_link"] if p != ""]
    roles = [r for r in row["role"] if r != ""]
    roles += [None] * (len(players) - len(roles))
    return roles[: len(players)]

@asset(
    description="Rosters de tournois nettoyés depuis le bronze S3 vers Snowflake",
    deps=[AssetDep("tournament_rosters_bronze")],
)
def tournament_rosters_silver(
    context: AssetExecutionContext,
    s3: S3Resource,
    snowflake: SnowflakeResource,
) -> MaterializeResult:
    latest_key = s3.get_latest_key(TOURNAMENT_ROSTERS_BRONZE_PREFIX)
    context.log.info(f"Reading latest bronze file: s3://{s3.bucket_name}/{latest_key}")

    raw_data = s3.download_json(latest_key)
    context.log.info(f"  → {len(raw_data)} total roster entries loaded from bronze")

    df = pd.DataFrame(raw_data)
    df = df[list(TOURNAMENT_ROSTERS_COLUMN_MAPPING.keys())].rename(columns=TOURNAMENT_ROSTERS_COLUMN_MAPPING)

    df["player_link"] = df["player_link"].str.split(";;")
    df["role"] = df["role"].str.split(";;")

    # Drop rows with no player data — unusable even if roles are filled in.
    empty_roster_mask = df["player_link"].apply(lambda x: x == [""] or x == [])
    n_empty = empty_roster_mask.sum()
    if n_empty:
        context.log.warning("  ⚠ Dropping %d rows with empty RosterLinks", n_empty)
    df = df[~empty_roster_mask].reset_index(drop=True)

    df["player_link"] = df["player_link"].apply(lambda x: [p for p in x if p != ""])
    df["role"] = df.apply(_align_roles, axis=1)

    df = df.explode(["player_link", "role"]).reset_index(drop=True)

    df = deduplicate(context, df, ["team", "overview_page", "player_link"])
    df = standardize_types(context, df, TournamentRostersSchema.TOURNAMENT_ROSTERS_SCHEMA)
    df = complete_missing_values(context, df, TournamentRostersSchema.TOURNAMENT_ROSTERS_SCHEMA)
    context.log.info(f"  → {len(df)} rows after explode and deduplication")

    silver_rows = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]

    snowflake.truncate_and_insert(
        TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE,
        silver_rows,
        list(TOURNAMENT_ROSTERS_COLUMN_MAPPING.values()),
    )
    context.log.info(f"✅ {len(silver_rows)} rows written to Snowflake → {TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE}")

    today = date.today().isoformat()
    return MaterializeResult(
        metadata={
            "snowflake_table": TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE,
            "row_count": len(silver_rows),
            "source_key": latest_key,
            "ingestion_date": today,
        }
    )