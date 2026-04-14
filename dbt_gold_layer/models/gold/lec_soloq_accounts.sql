WITH lec_last_rosters AS (
    SELECT PLAYER_LINK, PLAYER_NAME, ROLE, TEAM
    FROM {{ ref('lec_last_rosters') }}
),

player_soloqueue_accounts AS (
    SELECT player_overview_page, region, game_name, tag_line, full_id, puuid
    FROM {{ source('silver', 'LEAGUEPEDIA_PLAYER_SOLOQUEUE_ACCOUNTS') }}
)

SELECT
    r.PLAYER_LINK,
    r.PLAYER_NAME,
    r.ROLE,
    r.TEAM,
    a.region,
    a.game_name,
    a.tag_line,
    a.full_id,
    a.puuid
FROM lec_last_rosters AS r
INNER JOIN player_soloqueue_accounts AS a ON a.player_overview_page = r.PLAYER_LINK
WHERE region = 'EUW' AND puuid IS NOT NULL
