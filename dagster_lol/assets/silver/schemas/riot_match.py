"""Schéma de données pour les matchs Riot (couche silver)."""


class RiotMatchSchema:
    RIOT_MATCH_SCHEMA = {
        "match_id":              {"type": "str", "missing_value": None},
        "game_id":               {"type": "int", "missing_value": None},
        "platform_id":           {"type": "str", "missing_value": None},
        "game_version":          {"type": "str", "missing_value": None},
        "game_mode":             {"type": "str", "missing_value": None},
        "game_type":             {"type": "str", "missing_value": None},
        "queue_id":              {"type": "int", "missing_value": None},
        "map_id":                {"type": "int", "missing_value": None},
        "game_duration":         {"type": "int", "missing_value": None},
        "game_start_timestamp":  {"type": "int", "missing_value": None},
        "game_end_timestamp":    {"type": "int", "missing_value": None},
    }
