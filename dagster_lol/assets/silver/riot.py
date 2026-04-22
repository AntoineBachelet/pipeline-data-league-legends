from datetime import date
from typing import Optional

import pandas as pd
from dagster import asset, AssetExecutionContext, AssetDep, MaterializeResult

from ...ressources.s3 import S3Resource
from ...ressources.snowflake import SnowflakeResource
from .transform import standardize_types, complete_missing_values
from .schemas import RiotMatchSchema, RiotMatchParticipantsSchema, RiotTeamsSchema, RiotPlayerRankingsSchema

MATCH_BRONZE_PREFIX = "bronze/riot/match_data"
RANKINGS_BRONZE_PREFIX = "bronze/riot/rankings"

RIOT_PLAYER_RANKINGS_TABLE = "silver.riot_player_rankings"
RANKINGS_COLUMNS = list(RiotPlayerRankingsSchema.RIOT_PLAYER_RANKINGS_SCHEMA.keys())

RIOT_MATCH_TABLE = "silver.riot_match"
RIOT_MATCH_PARTICIPANTS_TABLE = "silver.riot_match_participants"
RIOT_TEAMS_TABLE = "silver.riot_teams"

MATCH_COLUMNS = list(RiotMatchSchema.RIOT_MATCH_SCHEMA.keys())
PARTICIPANTS_COLUMNS = list(RiotMatchParticipantsSchema.RIOT_MATCH_PARTICIPANTS_SCHEMA.keys())
TEAMS_COLUMNS = list(RiotTeamsSchema.RIOT_TEAMS_SCHEMA.keys())

SOLOQ_ACCOUNTS_SQL = """
    SELECT DISTINCT puuid, player FROM SILVER.LEAGUEPEDIA_PLAYER_SOLOQUEUE_ACCOUNTS
    INNER JOIN SILVER.LEAGUEPEDIA_PLAYERS ON LEAGUEPEDIA_PLAYER_SOLOQUEUE_ACCOUNTS.PLAYER_OVERVIEW_PAGE = LEAGUEPEDIA_PLAYERS.OVERVIEW_PAGE
    INNER JOIN GOLD.LEC_LAST_ROSTERS ON PLAYER_LINK = LEAGUEPEDIA_PLAYERS.PLAYER
    WHERE REGION = 'EUW' AND puuid IS NOT NULL;
"""

# A match is considered fully processed only when timeline diffs are already computed.
# Matches already in DB but with NULL diffs will be reprocessed and updated via MERGE.
EXISTING_MATCH_IDS_SQL = """
    SELECT DISTINCT match_id
    FROM silver.riot_match_participants
    WHERE cs_diff_at_15 IS NOT NULL
"""


def _safe_name(name: str) -> str:
    return name.replace(" ", "_").replace("/", "-")


def _compute_at_15_diffs(timeline_data: dict, participants: list[dict]) -> dict[int, dict]:
    """Compute cs_diff_at_15 and gold_diff_at_15 for each participant.

    Uses the timeline frame at exactly 15 minutes (derived from frameInterval).
    Diffs are computed against the lane opponent (same teamPosition, opposite teamId).
    Returns a dict mapping participantId → {cs_diff_at_15, gold_diff_at_15},
    or an empty dict if the game ended before 15 minutes or data is missing.
    """
    info = timeline_data.get("info", {})
    frame_interval_ms = info.get("frameInterval", 60000)
    frames = info.get("frames", [])

    target_index = (15 * 60 * 1000) // frame_interval_ms
    if target_index >= len(frames):
        return {}

    participant_frames = frames[target_index].get("participantFrames", {})

    # Build position → {teamId: participantId} to find lane opponents
    position_map: dict[str, dict[int, int]] = {}
    for p in participants:
        pos = p.get("teamPosition") or p.get("individualPosition")
        if not pos or pos in ("", "Invalid"):
            continue
        position_map.setdefault(pos, {})[p["teamId"]] = p["participantId"]

    diffs: dict[int, dict] = {}
    for p in participants:
        pid = p["participantId"]
        pos = p.get("teamPosition") or p.get("individualPosition")
        team_id = p["teamId"]

        if not pos or pos not in position_map:
            continue

        lane_teams = position_map[pos]
        if len(lane_teams) != 2:
            continue

        opponent_team_id = next(t for t in lane_teams if t != team_id)
        opponent_pid = lane_teams[opponent_team_id]

        my_frame = participant_frames.get(str(pid), {})
        opp_frame = participant_frames.get(str(opponent_pid), {})

        diffs[pid] = {
            "cs_diff_at_15":   my_frame.get("minionsKilled", 0) - opp_frame.get("minionsKilled", 0),
            "gold_diff_at_15": my_frame.get("totalGold", 0) - opp_frame.get("totalGold", 0),
        }

    return diffs


def _extract_match(match_id: str, info: dict) -> dict:
    return {
        "match_id":             match_id,
        "game_id":              info.get("gameId"),
        "platform_id":          info.get("platformId"),
        "game_version":         info.get("gameVersion"),
        "game_mode":            info.get("gameMode"),
        "game_type":            info.get("gameType"),
        "queue_id":             info.get("queueId"),
        "map_id":               info.get("mapId"),
        "game_duration":        info.get("gameDuration"),
        "game_start_timestamp": info.get("gameStartTimestamp"),
        "game_end_timestamp":   info.get("gameEndTimestamp"),
    }


def _extract_participants(
    match_id: str,
    participants: list[dict],
    at_15_diffs: Optional[dict[int, dict]] = None,
) -> list[dict]:
    at_15_diffs = at_15_diffs or {}
    rows = []
    for p in participants:
        pid = int(p["participantId"])
        diffs = at_15_diffs.get(pid, {})
        rows.append({
            "match_id":                        match_id,
            "participant_id":                  pid,
            "puuid":                           p.get("puuid"),
            "team_id":                         p.get("teamId"),
            "riot_id_game_name":               p.get("riotIdGameName"),
            "riot_id_tagline":                 p.get("riotIdTagline"),
            "champion_id":                     p.get("championId"),
            "champion_name":                   p.get("championName"),
            "team_position":                   p.get("teamPosition"),
            "individual_position":             p.get("individualPosition"),
            "win":                             p.get("win"),
            "kills":                           p.get("kills"),
            "deaths":                          p.get("deaths"),
            "assists":                         p.get("assists"),
            "gold_earned":                     p.get("goldEarned"),
            "total_damage_dealt_to_champions": p.get("totalDamageDealtToChampions"),
            "total_minions_killed":            p.get("totalMinionsKilled"),
            "neutral_minions_killed":          p.get("neutralMinionsKilled"),
            "vision_score":                    p.get("visionScore"),
            "wards_placed":                    p.get("wardsPlaced"),
            "wards_killed":                    p.get("wardsKilled"),
            "detector_wards_placed":           p.get("detectorWardsPlaced"),
            "cs_diff_at_15":                   diffs.get("cs_diff_at_15"),
            "gold_diff_at_15":                 diffs.get("gold_diff_at_15"),
        })
    return rows


def _extract_teams(match_id: str, teams: list[dict]) -> list[dict]:
    rows = []
    for team in teams:
        bans = sorted(team.get("bans", []), key=lambda b: b.get("pickTurn", 0))
        ban_ids = [b.get("championId") for b in bans]
        ban_ids += [None] * (5 - len(ban_ids))

        objectives = team.get("objectives", {})
        rows.append({
            "match_id":                     match_id,
            "team_id":                      team.get("teamId"),
            "win":                          team.get("win"),
            "ban_1_champion_id":            ban_ids[0],
            "ban_2_champion_id":            ban_ids[1],
            "ban_3_champion_id":            ban_ids[2],
            "ban_4_champion_id":            ban_ids[3],
            "ban_5_champion_id":            ban_ids[4],
            "objectives_baron_kills":       objectives.get("baron", {}).get("kills"),
            "objectives_dragon_kills":      objectives.get("dragon", {}).get("kills"),
            "objectives_tower_kills":       objectives.get("tower", {}).get("kills"),
            "objectives_inhibitor_kills":   objectives.get("inhibitor", {}).get("kills"),
            "objectives_rift_herald_kills": objectives.get("riftHerald", {}).get("kills"),
            "objectives_champion_kills":    objectives.get("champion", {}).get("kills"),
        })
    return rows


@asset(
    description="Matchs Soloq normalisés depuis le bronze S3 vers Snowflake (3 tables : match, participants, teams)",
    deps=[AssetDep("matchs_details_bronze"), AssetDep("matchs_timeline_bronze")],
)
def riot_matches_silver(
    context: AssetExecutionContext,
    s3: S3Resource,
    snowflake: SnowflakeResource,
) -> MaterializeResult:
    df_existing = snowflake.fetch(EXISTING_MATCH_IDS_SQL)
    existing_match_ids: set[str] = set(df_existing["match_id"].tolist())
    context.log.info("  → %d match_ids already in Snowflake", len(existing_match_ids))

    soloq_accounts = snowflake.fetch(SOLOQ_ACCOUNTS_SQL)
    context.log.info("  → %d accounts to process", len(soloq_accounts))

    match_rows: list[dict] = []
    participant_rows: list[dict] = []
    team_rows: list[dict] = []
    seen_match_ids: set[str] = set()
    files_read = 0
    files_skipped = 0
    timelines_found = 0

    for _, account_row in soloq_accounts.iterrows():
        puuid = account_row["puuid"]
        player = account_row["player"]
        safe_player = _safe_name(player)

        details_prefix = f"{MATCH_BRONZE_PREFIX}/{safe_player}/{puuid}/match_details/"
        response = s3.get_client().list_objects_v2(Bucket=s3.bucket_name, Prefix=details_prefix)
        objects = response.get("Contents", [])

        if not objects:
            context.log.debug("No match details found for %s", player)
            continue

        context.log.info("  → %d match files found for %s", len(objects), player)

        for obj in objects:
            match_id = obj["Key"].split("/")[-1].replace(".parquet", "")

            if match_id in existing_match_ids or match_id in seen_match_ids:
                files_skipped += 1
                context.log.debug("  → Skipping %s", match_id)
                continue

            df = s3.download_parquet(obj["Key"])
            if df.empty:
                continue

            raw = df.to_dict("records")[0]
            info = raw.get("info", {})
            match_id_from_meta = raw.get("metadata", {}).get("matchId") or match_id

            # Try to load the corresponding timeline for diff computation
            at_15_diffs: dict[int, dict] = {}
            timeline_key = f"{MATCH_BRONZE_PREFIX}/{safe_player}/{puuid}/match_timeline/{match_id}.parquet"
            try:
                df_timeline = s3.download_parquet(timeline_key)
                if not df_timeline.empty:
                    timeline_raw = df_timeline.to_dict("records")[0]
                    at_15_diffs = _compute_at_15_diffs(timeline_raw, info.get("participants", []))
                    timelines_found += 1
                    context.log.debug("  → Timeline loaded for %s (%d diffs computed)", match_id, len(at_15_diffs))
            except Exception:
                context.log.debug("  → No timeline available for %s", match_id)

            match_rows.append(_extract_match(match_id_from_meta, info))
            participant_rows.extend(_extract_participants(match_id_from_meta, info.get("participants", []), at_15_diffs))
            team_rows.extend(_extract_teams(match_id_from_meta, info.get("teams", [])))

            seen_match_ids.add(match_id)
            files_read += 1

    context.log.info(
        "  → %d new files read, %d skipped, %d timelines loaded → %d matches, %d participants, %d team records",
        files_read, files_skipped, timelines_found, len(match_rows), len(participant_rows), len(team_rows),
    )

    today = date.today().isoformat()

    if not match_rows:
        context.log.info("No new match data to store")
        return MaterializeResult(
            metadata={
                "files_read": 0,
                "files_skipped": files_skipped,
                "ingestion_date": today,
            }
        )

    # --- RIOT_MATCH ---
    df_match = pd.DataFrame(match_rows, columns=MATCH_COLUMNS)
    df_match = standardize_types(context, df_match, RiotMatchSchema.RIOT_MATCH_SCHEMA)
    df_match = complete_missing_values(context, df_match, RiotMatchSchema.RIOT_MATCH_SCHEMA)
    silver_matches = [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in df_match.to_dict("records")]

    result_match = snowflake.merge(
        RIOT_MATCH_TABLE, silver_matches, MATCH_COLUMNS, key_columns=["match_id"]
    )
    context.log.info(
        "✅ riot_match: %d new, %d updated → %s",
        result_match["new_count"], result_match["updated_count"], RIOT_MATCH_TABLE,
    )

    # --- RIOT_MATCH_PARTICIPANTS ---
    df_participants = pd.DataFrame(participant_rows, columns=PARTICIPANTS_COLUMNS)
    df_participants = standardize_types(context, df_participants, RiotMatchParticipantsSchema.RIOT_MATCH_PARTICIPANTS_SCHEMA)
    df_participants = complete_missing_values(context, df_participants, RiotMatchParticipantsSchema.RIOT_MATCH_PARTICIPANTS_SCHEMA)
    silver_participants = [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in df_participants.to_dict("records")]

    result_participants = snowflake.merge(
        RIOT_MATCH_PARTICIPANTS_TABLE, silver_participants, PARTICIPANTS_COLUMNS,
        key_columns=["match_id", "participant_id"]
    )
    context.log.info(
        "✅ riot_match_participants: %d new, %d updated → %s",
        result_participants["new_count"], result_participants["updated_count"], RIOT_MATCH_PARTICIPANTS_TABLE,
    )

    # --- RIOT_TEAMS ---
    df_teams = pd.DataFrame(team_rows, columns=TEAMS_COLUMNS)
    df_teams = standardize_types(context, df_teams, RiotTeamsSchema.RIOT_TEAMS_SCHEMA)
    df_teams = complete_missing_values(context, df_teams, RiotTeamsSchema.RIOT_TEAMS_SCHEMA)
    silver_teams = [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in df_teams.to_dict("records")]

    result_teams = snowflake.merge(
        RIOT_TEAMS_TABLE, silver_teams, TEAMS_COLUMNS, key_columns=["match_id", "team_id"]
    )
    context.log.info(
        "✅ riot_teams: %d new, %d updated → %s",
        result_teams["new_count"], result_teams["updated_count"], RIOT_TEAMS_TABLE,
    )

    return MaterializeResult(
        metadata={
            "files_read": files_read,
            "files_skipped": files_skipped,
            "timelines_loaded": timelines_found,
            "match_count": len(silver_matches),
            "participant_count": len(silver_participants),
            "team_count": len(silver_teams),
            "match_new": result_match["new_count"],
            "match_updated": result_match["updated_count"],
            "ingestion_date": today,
        }
    )


@asset(
    description="Classement ranked des joueurs normalisé depuis le bronze S3 vers Snowflake (snapshot quotidien par joueur et type de file)",
    deps=[AssetDep("player_rankings_bronze")],
)
def player_rankings_silver(
    context: AssetExecutionContext,
    s3: S3Resource,
    snowflake: SnowflakeResource,
) -> MaterializeResult:
    soloq_accounts = snowflake.fetch(SOLOQ_ACCOUNTS_SQL)
    context.log.info("  → %d accounts to process", len(soloq_accounts))

    today = date.today().isoformat()
    ranking_rows: list[dict] = []
    files_read = 0
    files_skipped = 0

    for _, account_row in soloq_accounts.iterrows():
        puuid = account_row["puuid"]
        player = account_row["player"]
        safe_player = _safe_name(player)

        snapshot_prefix = f"{RANKINGS_BRONZE_PREFIX}/{safe_player}/{puuid}/"
        try:
            latest_key = s3.get_latest_key(snapshot_prefix, extension=".parquet")
            df = s3.download_parquet(latest_key)
        except FileNotFoundError:
            context.log.warning("No ranking snapshot found for %s — skipping", player)
            files_skipped += 1
            continue

        if df.empty:
            files_skipped += 1
            continue

        for row in df.to_dict("records"):
            row["snapshot_date"] = today
            ranking_rows.append(row)

        files_read += 1
        context.log.info("  → %d queue entries loaded for %s", len(df), player)

    context.log.info("  → %d files read, %d skipped → %d total entries", files_read, files_skipped, len(ranking_rows))

    if not ranking_rows:
        context.log.info("No ranking data to store")
        return MaterializeResult(
            metadata={"files_read": 0, "files_skipped": files_skipped, "ingestion_date": today}
        )

    df_rankings = pd.DataFrame(ranking_rows, columns=RANKINGS_COLUMNS)
    df_rankings = standardize_types(context, df_rankings, RiotPlayerRankingsSchema.RIOT_PLAYER_RANKINGS_SCHEMA)
    df_rankings = complete_missing_values(context, df_rankings, RiotPlayerRankingsSchema.RIOT_PLAYER_RANKINGS_SCHEMA)
    silver_rankings = [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in df_rankings.to_dict("records")]

    result = snowflake.merge(
        RIOT_PLAYER_RANKINGS_TABLE, silver_rankings, RANKINGS_COLUMNS,
        key_columns=["puuid", "queue_type", "snapshot_date"]
    )
    context.log.info(
        "✅ riot_player_rankings: %d new, %d updated → %s",
        result["new_count"], result["updated_count"], RIOT_PLAYER_RANKINGS_TABLE,
    )

    return MaterializeResult(
        metadata={
            "files_read": files_read,
            "files_skipped": files_skipped,
            "entry_count": len(silver_rankings),
            "new_count": result["new_count"],
            "updated_count": result["updated_count"],
            "ingestion_date": today,
        }
    )
