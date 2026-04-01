from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_gold_layer"

@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def lol_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()