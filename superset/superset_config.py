import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ['SUPERSET_POSTGRES_USER']}:"
    f"{os.environ['SUPERSET_POSTGRES_PASSWORD']}"
    f"@superset_postgresql:5432/"
    f"{os.environ['SUPERSET_POSTGRES_DB']}"
)

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://superset_redis:6379/0",
}


class CeleryConfig:
    broker_url = "redis://superset_redis:6379/0"
    result_backend = "redis://superset_redis:6379/1"


CELERY_CONFIG = CeleryConfig

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
