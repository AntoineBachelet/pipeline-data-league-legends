from datetime import date
import requests
from dagster import asset, AssetExecutionContext, MaterializeResult
from ...ressources import S3Resource

API_BASE_URL = "https://api.lolpros.gg/es/ladder"
PAGE_SIZE = 1000


@asset(
    description="Récupération du ladder lolpros.gg (classement des joueurs pro en soloqueue)",
)
def lolpros_ladder_bronze(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> MaterializeResult:
    all_results = []
    page = 1

    while True:
        context.log.info(f"Fetching page={page}...")
        response = requests.get(
            API_BASE_URL,
            params={
                "page": page,
                "page_size": PAGE_SIZE,
                "sort": "rank",
                "order": "desc",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        all_results.extend(data)
        context.log.info(f"  → {len(data)} rows fetched (total: {len(all_results)})")

        if len(data) < PAGE_SIZE:
            break

        page += 1

    today = date.today().isoformat()
    s3_key = f"bronze/lolpros/ladder/{today}/ladder.parquet"
    s3_path = s3.upload_parquet(all_results, s3_key)
    context.log.info(f"✅ {len(all_results)} entrées ladder sauvegardées → {s3_path}")

    return MaterializeResult(
        metadata={
            "s3_path": s3_path,
            "row_count": len(all_results),
            "ingestion_date": today,
        }
    )
