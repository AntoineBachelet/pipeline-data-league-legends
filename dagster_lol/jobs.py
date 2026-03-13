from dagster import define_asset_job, AssetSelection

leaguepedia_job = define_asset_job(
    name="leaguepedia_job",
    selection=AssetSelection.groups("leaguepedia"),  # tous les assets du groupe
)

bronze_job = define_asset_job(
    name="bronze_job",
    selection=AssetSelection.groups("bronze"),  # tous les assets du groupe
)