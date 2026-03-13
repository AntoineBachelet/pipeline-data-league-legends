from dagster import load_assets_from_package_module
from . import bronze, silver
from .silver import silver_checks

bronze_assets = load_assets_from_package_module(bronze, group_name="bronze")
silver_assets = load_assets_from_package_module(silver, group_name="silver")