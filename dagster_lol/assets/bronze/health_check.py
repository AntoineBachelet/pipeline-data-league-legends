from dagster import asset, AssetExecutionContext




@asset(
    description="Asset de test — vérifie que le pipeline tourne correctement"
)
def ping(context: AssetExecutionContext) -> str:
    context.log.info("✅ Dagster opérationnel !")
    return "pong"

