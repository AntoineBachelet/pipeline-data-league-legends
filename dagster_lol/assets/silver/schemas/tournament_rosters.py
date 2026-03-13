"""Schéma de données pour les rosters de tournois leaguepedia."""


class TournamentRostersSchema:
    """Schéma avec métadonnées de format pour les rosters de tournois leaguepedia."""

    TOURNAMENT_ROSTERS_SCHEMA = {
        "team": {"type": "str", "missing_value": None},
        "overview_page": {"type": "str", "missing_value": None},
        "player_link": {"type": "str", "missing_value": None},
        "role": {"type": "str", "missing_value": None},
        "region": {"type": "str", "missing_value": None},
        "footnotes": {"type": "str", "missing_value": None},
        "isused": {"type": "str", "missing_value": None},
        "iscomplete": {"type": "boolean", "missing_value": True},
        "pageandteam": {"type": "str", "missing_value": None}
    }
