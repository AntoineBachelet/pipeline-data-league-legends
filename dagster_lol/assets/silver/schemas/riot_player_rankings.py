"""Schéma de données pour le classement ranked des joueurs (couche silver)."""


class RiotPlayerRankingsSchema:
    RIOT_PLAYER_RANKINGS_SCHEMA = {
        "puuid":           {"type": "str",  "missing_value": None},
        "player":          {"type": "str",  "missing_value": None},
        "queue_type":      {"type": "str",  "missing_value": None},
        "tier":            {"type": "str",  "missing_value": None},
        "rank":            {"type": "str",  "missing_value": None},
        "league_points":   {"type": "int",  "missing_value": None},
        "wins":            {"type": "int",  "missing_value": None},
        "losses":          {"type": "int",  "missing_value": None},
        "veteran":         {"type": "bool", "missing_value": None},
        "inactive":        {"type": "bool", "missing_value": None},
        "fresh_blood":     {"type": "bool", "missing_value": None},
        "hot_streak":      {"type": "bool", "missing_value": None},
        "snapshot_date":   {"type": "date", "missing_value": None},
    }
