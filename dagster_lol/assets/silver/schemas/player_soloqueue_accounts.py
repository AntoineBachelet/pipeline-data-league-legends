"""Schema de données pour les comptes soloqueue des joueurs leaguepedia."""


class PlayerSoloqueueAccountsSchema:
    """Schéma avec métadonnées de format pour les comptes soloqueue."""

    PLAYER_SOLOQUEUE_ACCOUNTS_SCHEMA = {
        "player_overview_page": {"type": "str", "missing_value": None},
        "region": {"type": "str", "missing_value": None},
        "game_name": {"type": "str", "missing_value": None},
        "tag_line": {"type": "str", "missing_value": None},
        "full_id": {"type": "str", "missing_value": None},
        "puuid": {"type": "str", "missing_value": None},
    }
