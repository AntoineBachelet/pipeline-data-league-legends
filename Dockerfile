FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dagster_lol/ ./dagster_lol/
COPY pyproject.toml .

RUN pip install --no-cache-dir -e .

COPY dbt_gold_layer/ ./dbt_gold_layer/
RUN cd dbt_gold_layer && \
    SNOWFLAKE_ACCOUNT=dummy \
    SNOWFLAKE_USER=dummy \
    SNOWFLAKE_PASSWORD=dummy \
    SNOWFLAKE_DATABASE=dummy \
    SNOWFLAKE_WAREHOUSE=dummy \
    SNOWFLAKE_ROLE=dummy \
    dbt parse --profiles-dir .

EXPOSE 4000

CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_lol"]