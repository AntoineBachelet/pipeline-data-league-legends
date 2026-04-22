"""Schéma de données pour les équipes des matchs Riot (couche silver)."""


class RiotTeamsSchema:
    RIOT_TEAMS_SCHEMA = {
        "match_id":                     {"type": "str", "missing_value": None},
        "team_id":                      {"type": "int", "missing_value": None},
        "win":                          {"type": "bool", "missing_value": None},
        "ban_1_champion_id":            {"type": "int", "missing_value": None},
        "ban_2_champion_id":            {"type": "int", "missing_value": None},
        "ban_3_champion_id":            {"type": "int", "missing_value": None},
        "ban_4_champion_id":            {"type": "int", "missing_value": None},
        "ban_5_champion_id":            {"type": "int", "missing_value": None},
        "objectives_baron_kills":       {"type": "int", "missing_value": 0},
        "objectives_dragon_kills":      {"type": "int", "missing_value": 0},
        "objectives_tower_kills":       {"type": "int", "missing_value": 0},
        "objectives_inhibitor_kills":   {"type": "int", "missing_value": 0},
        "objectives_rift_herald_kills": {"type": "int", "missing_value": 0},
        "objectives_champion_kills":    {"type": "int", "missing_value": 0},
    }
