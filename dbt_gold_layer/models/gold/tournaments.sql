WITH source AS (
    SELECT *
    FROM {{ source('silver', 'LEAGUEPEDIA_TOURNAMENTS') }}
),

filtered AS (
    SELECT
        OVERVIEW_PAGE,
        DATE_START,
        LEAGUE
    FROM source
    WHERE LEAGUE = 'LoL EMEA Championship'
)

SELECT * FROM filtered