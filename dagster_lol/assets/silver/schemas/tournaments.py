"""Schéma de données pour les tournois leaguepedia."""


class TournamentsSchema:
    """Schéma avec métadonnées de format pour tournois leaguepedia."""

    TOURNAMENTS_SCHEMA = {
        "name": {"type": "str", "missing_value": None},
        "overview_page": {"type": "str", "missing_value": None},
        "date_start": {"type": "date", "missing_value": None},
        "league": {"type": "str", "missing_value": None},
        "region": {"type": "str", "missing_value": None},
        "prizepool": {"type": "str", "missing_value": None},
        "currency": {"type": "str", "missing_value": None},
        "country": {"type": "str", "missing_value": None},
        "rulebook": {"type": "str", "missing_value": None},
        "event_type": {"type": "str", "missing_value": None},
        "links": {"type": "str", "missing_value": None},
        "sponsors": {"type": "str", "missing_value": None},
        "organizer": {"type": "str", "missing_value": None},
        "organizers": {"type": "str", "missing_value": None},
        "split": {"type": "str", "missing_value": None},
        "is_qualifier": {"type": "bool", "missing_value": True},
        "is_playoffs": {"type": "bool", "missing_value": False},
        "is_official": {"type": "bool", "missing_value": False},
        "year": {"type": "int", "missing_value": 0},
    }