from datetime import date
import pandas as pd
from dagster import asset, AssetExecutionContext, AssetDep, MaterializeResult
from ...ressources import RiotResource, S3Resource, SnowflakeResource
from .utils import safe_name

LEAGUES = ["La Ligue Française", "LoL EMEA Championship"]

SOLOQ_ACCOUNTS_SQL = """
    SELECT puuid, player FROM LEAGUEPEDIA_PLAYER_SOLOQUEUE_ACCOUNTS
    INNER JOIN LEAGUEPEDIA_PLAYERS ON LEAGUEPEDIA_PLAYER_SOLOQUEUE_ACCOUNTS.PLAYER_OVERVIEW_PAGE = LEAGUEPEDIA_PLAYERS.OVERVIEW_PAGE
    INNER JOIN GOLD.LEC_LAST_ROSTERS ON PLAYER_LINK = LEAGUEPEDIA_PLAYERS.PLAYER
    WHERE REGION = 'EUW' AND puuid IS NOT NULL;
"""

MATCH_BRONZE_PREFIX = "bronze/riot/match_data"


@asset(
    description="Récupération des match IDs Soloq depuis l'API Riot et stockage Parquet sur S3",
)
def matchs_ids_bronze(
    context: AssetExecutionContext,
    riot: RiotResource,
    snowflake: SnowflakeResource,
    s3: S3Resource,
) -> MaterializeResult:
    soloq_accounts = snowflake.fetch(SOLOQ_ACCOUNTS_SQL)
    context.log.info("  → %d accounts to process", len(soloq_accounts))

    today = date.today().isoformat()
    total_match_ids = 0
    uploaded_keys = []

    for _, account_row in soloq_accounts.iterrows():
        puuid = account_row["puuid"]
        player = account_row["player"]

        context.log.info("Fetching match ids for %s (puuid=%s…)", player, puuid[:8])
        match_ids = riot.get_match_ids(puuid, "EUW", context.log)
        context.log.info("  → %d match ids retrieved for %s", len(match_ids), player)

        if not match_ids:
            continue

        df = pd.DataFrame({"match_id": match_ids})
        df["puuid"] = puuid
        df["player"] = player

        safe_player = safe_name(player)
        s3_key = f"{MATCH_BRONZE_PREFIX}/{safe_player}/{puuid}/match_ids/{today}.parquet"
        s3.upload_parquet(df.to_dict("records"), s3_key)
        context.log.info("  → Uploaded to s3://%s/%s", s3.bucket_name, s3_key)

        total_match_ids += len(match_ids)
        uploaded_keys.append(s3_key)

    return MaterializeResult(
        metadata={
            "account_count": len(soloq_accounts),
            "total_match_ids": total_match_ids,
            "files_uploaded": len(uploaded_keys),
            "s3_prefix": f"{MATCH_BRONZE_PREFIX}/{today}/",
            "ingestion_date": today,
        }
    )


@asset(
    description="Récupération des détails de matchs Soloq depuis l'API Riot et stockage Parquet sur S3",
    deps=[AssetDep("matchs_ids_bronze")],
)
def matchs_details_bronze(
    context: AssetExecutionContext,
    riot: RiotResource,
    snowflake: SnowflakeResource,
    s3: S3Resource,
) -> MaterializeResult:
    soloq_accounts = snowflake.fetch(SOLOQ_ACCOUNTS_SQL)
    context.log.info("  → %d accounts to process", len(soloq_accounts))

    total_fetched = 0
    total_skipped = 0
    total_uploaded = 0

    for _, account_row in soloq_accounts.iterrows():
        puuid = account_row["puuid"]
        player = account_row["player"]
        safe_player = safe_name(player)

        match_ids_prefix = f"{MATCH_BRONZE_PREFIX}/{safe_player}/{puuid}/match_ids/"
        try:
            latest_key = s3.get_latest_key(match_ids_prefix, extension=".parquet")
        except FileNotFoundError:
            context.log.warning("No match ids file found for %s — skipping", player)
            continue

        df_ids = s3.download_parquet(latest_key)
        match_ids = df_ids["match_id"].tolist()
        context.log.info("  → %d match ids loaded for %s", len(match_ids), player)

        details_prefix = f"{MATCH_BRONZE_PREFIX}/{safe_player}/{puuid}/match_details/"
        existing_objects = s3.get_client().list_objects_v2(
            Bucket=s3.bucket_name, Prefix=details_prefix
        )
        existing_ids = {
            obj["Key"].split("/")[-1].replace(".parquet", "")
            for obj in existing_objects.get("Contents", [])
        }

        new_ids = [mid for mid in match_ids if mid not in existing_ids]
        context.log.info(
            "  → %d new / %d already stored for %s",
            len(new_ids), len(existing_ids), player,
        )
        total_skipped += len(existing_ids)

        for match_id in new_ids:
            match_data = riot.get_match(match_id, context.log)
            if match_data is None:
                continue

            s3_key = f"{details_prefix}{match_id}.parquet"
            s3.upload_parquet([match_data], s3_key)
            context.log.debug("  → Uploaded %s", s3_key)

            total_fetched += 1
            total_uploaded += 1

    return MaterializeResult(
        metadata={
            "account_count": len(soloq_accounts),
            "matches_fetched": total_fetched,
            "matches_skipped": total_skipped,
            "files_uploaded": total_uploaded,
            "ingestion_date": date.today().isoformat(),
        }
    )