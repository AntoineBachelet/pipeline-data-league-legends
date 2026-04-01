from dagster import Definitions
from dagster import EnvVar
from dagster_dbt import DbtCliResource
from .ressources import S3Resource, LeaguepediaResource, SnowflakeResource
from .assets import bronze_assets, silver_assets, silver_checks, lol_dbt_assets, DBT_PROJECT_DIR
from .jobs import leaguepedia_job, bronze_job

defs = Definitions(
    assets=[*bronze_assets, *silver_assets, lol_dbt_assets],
    asset_checks=silver_checks,
    jobs=[leaguepedia_job, bronze_job],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
        "leaguepedia": LeaguepediaResource(
            username=EnvVar("LEAGUEPEDIA_USERNAME"),
            password=EnvVar("LEAGUEPEDIA_PASSWORD"),
        ),
        "s3": S3Resource(
            bucket_name=EnvVar("S3_BUCKET_NAME"),
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            region_name=EnvVar("AWS_REGION"),
        ),
        "snowflake": SnowflakeResource(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            database=EnvVar("SNOWFLAKE_DATABASE"),
            schema_name=EnvVar("SNOWFLAKE_SCHEMA_NAME"),
            warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
        ),
    }
)
