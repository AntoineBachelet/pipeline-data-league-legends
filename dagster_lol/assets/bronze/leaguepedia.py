import time
from datetime import date
from dagster import asset, AssetExecutionContext, MaterializeResult
from mwrogue.esports_client import EsportsClient
from mwrogue.auth_credentials import AuthCredentials
from ...ressources import LeaguepediaResource, S3Resource

PAGE_SIZE = 500
RATE_LIMIT_DELAY = 60  # seconds between pages

ROLES = ("Top", "Jungle", "Mid", "Bot", "Support")

LEAGUES = ["La Ligue Française", "LoL EMEA Championship"]

@asset(
    description="Récupération des tournois bruts dans leaguepedia",
)
def tournaments_bronze(
    context: AssetExecutionContext,
    leaguepedia: LeaguepediaResource,
    s3: S3Resource,
) -> MaterializeResult:
    client = leaguepedia.get_client()

    FIELDS = """
        T.Name,
        T.OverviewPage,
        T.DateStart,
        T.Date,
        T.DateStartFuzzy,
        T.League,
        T.Region,
        T.Prizepool,
        T.Currency,
        T.Country,
        T.ClosestTimezone,
        T.Rulebook,
        T.EventType,
        T.Links,
        T.Sponsors,
        T.Organizer,
        T.Organizers,
        T.StandardName,
        T.StandardName_Redirect,
        T.BasePage,
        T.Split,
        T.SplitNumber,
        T.SplitMainPage,
        T.TournamentLevel,
        T.IsQualifier,
        T.IsPlayoffs,
        T.IsOfficial,
        T.Year,
        T.LeagueIconKey,
        T.AlternativeNames,
        T.ScrapeLink,
        T.Tags,
        T.SuppressTopSchedule
    """.strip()

    all_results = []
    offset = 0

    while True:
        context.log.info(f"Fetching page offset={offset}...")
        page = client.cargo_client.query(
            tables="Tournaments=T",
            fields=FIELDS,
            order_by="T.DateStart DESC",
            limit=PAGE_SIZE,
            offset=offset,
        )

        all_results.extend(page)
        context.log.info(f"  → {len(page)} rows fetched (total: {len(all_results)})")

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        context.log.info(f"Rate limit pause: waiting {RATE_LIMIT_DELAY}s before next page...")
        time.sleep(RATE_LIMIT_DELAY)

    today = date.today().isoformat()
    # s3_key = f"bronze/leaguepedia/tournaments/{today}/tournaments.json"
    s3_key = f"bronze/leaguepedia/tournaments/{today}/tournaments.parquet"
    # s3_path = s3.upload_json(all_results, s3_key)
    s3_path = s3.upload_parquet(all_results, s3_key)
    context.log.info(f"✅ {len(all_results)} tournois sauvegardés → {s3_path}")
    # if result:
    #     tournaments.append(result[0])

    return MaterializeResult(
        metadata={
            "s3_path": s3_path,
            "row_count": len(all_results),
            "ingestion_date": today,
        }
    )

@asset(
    description="Récupération des rosters de tournoi leaguepedia",
)
def tournament_rosters_bronze(
    context: AssetExecutionContext,
    leaguepedia: LeaguepediaResource,
    s3: S3Resource,
) -> MaterializeResult:
    client = leaguepedia.get_client()

    FIELDS = """
        TR.Team,
        TR.OverviewPage,
        TR.Region,
        TR.RosterLinks,
        TR.Roles,
        TR.Flags,
        TR.Footnotes,
        TR.IsUsed,
        TR.Tournament,
        TR.Short,
        TR.IsComplete,
        TR.PageAndTeam,
        TR.UniqueLine
    """.strip()

    all_results = []
    offset = 0

    while True:
        context.log.info(f"Fetching page offset={offset}...")
        page = client.cargo_client.query(
            tables="TournamentRosters=TR",
            fields=FIELDS,
            limit=PAGE_SIZE,
            offset=offset,
        )

        all_results.extend(page)
        context.log.info(f"  → {len(page)} rows fetched (total: {len(all_results)})")

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        context.log.info(f"Rate limit pause: waiting {RATE_LIMIT_DELAY}s before next page...")
        time.sleep(RATE_LIMIT_DELAY)

    today = date.today().isoformat()
    #s3_key = f"bronze/leaguepedia/tournamentrosters/{today}/tournamentrosters.json"
    s3_key = f"bronze/leaguepedia/tournamentrosters/{today}/tournamentrosters.parquet"
    #s3_path = s3.upload_json(all_results, s3_key)
    s3_path = s3.upload_parquet(all_results, s3_key)

    context.log.info(f"✅ {len(all_results)} tournament rosters sauvegardés → {s3_path}")
    # if result:
    #     tournaments.append(result[0])

    return MaterializeResult(
        metadata={
            "s3_path": s3_path,
            "row_count": len(all_results),
            "ingestion_date": today,
        }
    )


@asset(
    description="Récupération des joueurs bruts depuis leaguepedia",
)
def players_bronze(
    context: AssetExecutionContext,
    leaguepedia: LeaguepediaResource,
    s3: S3Resource,
) -> MaterializeResult:
    client = leaguepedia.get_client()

    FIELDS = """
        P.ID,
        P.OverviewPage,
        P.Player,
        P.Image,
        P.Name,
        P.NativeName,
        P.NameAlphabet,
        P.NameFull,
        P.Country,
        P.Nationality,
        P.NationalityPrimary,
        P.Age,
        P.Birthdate,
        P.Deathdate,
        P.ResidencyFormer,
        P.Team,
        P.Team2,
        P.CurrentTeams,
        P.TeamSystem,
        P.Team2System,
        P.Residency,
        P.Role,
        P.Contract,
        P.ContractText,
        P.FavChamps,
        P.SoloqueueIds,
        P.Askfm,
        P.Bluesky,
        P.Discord,
        P.Facebook,
        P.Instagram,
        P.Lolpros,
        P.DPMLOL,
        P.Reddit,
        P.Snapchat,
        P.Stream,
        P.KICK,
        P.Twitter,
        P.Threads,
        P.LinkedIn,
        P.Vk,
        P.Website,
        P.Weibo,
        P.Youtube,
        P.TeamLast,
        P.RoleLast,
        P.IsRetired,
        P.ToWildrift,
        P.ToValorant,
        P.ToTFT,
        P.ToLegendsOfRuneterra,
        P.To2XKO,
        P.IsPersonality,
        P.IsSubstitute,
        P.IsTrainee,
        P.IsLowercase,
        P.IsAutoTeam,
        P.IsLowContent
    """.strip()

    all_results = []
    offset = 0

    while True:
        context.log.info(f"Fetching page offset={offset}...")
        page = client.cargo_client.query(
            tables="Players=P",
            fields=FIELDS,
            limit=PAGE_SIZE,
            offset=offset,
        )

        all_results.extend(page)
        context.log.info(f"  → {len(page)} rows fetched (total: {len(all_results)})")

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        context.log.info(f"Rate limit pause: waiting {RATE_LIMIT_DELAY}s before next page...")
        time.sleep(RATE_LIMIT_DELAY)

    today = date.today().isoformat()
    # s3_key = f"bronze/leaguepedia/players/{today}/players.json"
    s3_key = f"bronze/leaguepedia/players/{today}/players.parquet"
    # s3_path = s3.upload_json(all_results, s3_key)
    s3_path = s3.upload_parquet(all_results, s3_key)
    context.log.info(f"✅ {len(all_results)} joueurs sauvegardés → {s3_path}")

    return MaterializeResult(
        metadata={
            "s3_path": s3_path,
            "row_count": len(all_results),
            "ingestion_date": today,
        }
    )


# @asset(
#     group_name="leaguepedia",
#     description="Toutes les équipes participant aux derniers splits LFL et LEC avec leurs rosters",
# )
# def teams_in_latest_splits(
#     context: AssetExecutionContext,
#     leaguepedia: LeaguepediaResource,
#     latest_regular_splits: list[dict],
# ) -> list[dict]:
#     client = leaguepedia.get_client()
#     overview_pages = [s["OverviewPage"] for s in latest_regular_splits]
#     pages_filter = ", ".join(repr(p) for p in overview_pages)

#     results = client.cargo_client.query(
#         tables="TournamentRosters=TR",
#         fields="TR.Team, TR.RosterLinks",
#         where=f"TR.OverviewPage IN ({pages_filter})",
#         limit=100,
#     )

#     rosters = [r for r in results if r.get("Team")]
#     context.log.info(f"→ {len(rosters)} entrées de roster récupérées pour {len(overview_pages)} splits en 1 appel")
#     return rosters

# @asset(
#     group_name="leaguepedia",
#     description="Roster actuel des équipes LFL et LEC",
# )
# def pro_players_roster(
#     context: AssetExecutionContext,
#     leaguepedia: LeaguepediaResource,
#     teams_in_latest_splits: list[dict],
# ) -> list[dict]:
#     client = leaguepedia.get_client()

#     all_player_pages = list({
#         page.strip()
#         for roster in teams_in_latest_splits
#         for page in (roster.get("RosterLinks") or "").split(";;")
#         if page.strip()
#     })

#     if not all_player_pages:
#         context.log.warning("⚠️ Aucune page joueur trouvée dans les rosters")
#         return []

#     pages_filter = ", ".join(repr(p) for p in all_player_pages)
#     players = client.cargo_client.query(
#         tables="Players=P",
#         fields="P.Player, P.OverviewPage, P.Role, P.Team, P.Country",
#         where=f"P.OverviewPage IN ({pages_filter}) AND P.Role IN {ROLES}",
#         limit=500,
#     )

#     context.log.info(f"→ {len(players)} joueurs récupérés en 1 appel")
#     return players