"""Schéma de données pour les participants des matchs Riot (couche silver)."""


class RiotMatchParticipantsSchema:
    RIOT_MATCH_PARTICIPANTS_SCHEMA = {
        "match_id":                          {"type": "str", "missing_value": None},
        "participant_id":                    {"type": "int", "missing_value": None},
        "puuid":                             {"type": "str", "missing_value": None},
        "team_id":                           {"type": "int", "missing_value": None},
        "riot_id_game_name":                 {"type": "str", "missing_value": None},
        "riot_id_tagline":                   {"type": "str", "missing_value": None},
        "champion_id":                       {"type": "int", "missing_value": None},
        "champion_name":                     {"type": "str", "missing_value": None},
        "team_position":                     {"type": "str", "missing_value": None},
        "individual_position":               {"type": "str", "missing_value": None},
        "win":                               {"type": "bool", "missing_value": None},
        "kills":                             {"type": "int", "missing_value": 0},
        "deaths":                            {"type": "int", "missing_value": 0},
        "assists":                           {"type": "int", "missing_value": 0},
        "gold_earned":                       {"type": "int", "missing_value": 0},
        "total_damage_dealt_to_champions":   {"type": "int", "missing_value": 0},
        "total_minions_killed":              {"type": "int", "missing_value": 0},
        "neutral_minions_killed":            {"type": "int", "missing_value": 0},
        "vision_score":                      {"type": "int", "missing_value": 0},
        "wards_placed":                      {"type": "int", "missing_value": 0},
        "wards_killed":                      {"type": "int", "missing_value": 0},
        "detector_wards_placed":             {"type": "int", "missing_value": 0},
    }
