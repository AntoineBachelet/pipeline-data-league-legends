import pytest
from unittest.mock import MagicMock
from dagster import build_asset_context

from dagster_lol.assets.silver.leaguepedia import (
    tournaments_silver,
    players_silver,
    tournament_rosters_silver,
    TOURNAMENTS_SNOWFLAKE_TABLE,
    PLAYERS_SNOWFLAKE_TABLE,
    TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE,
)


@pytest.fixture
def ctx():
    return build_asset_context()


# ---------------------------------------------------------------------------
# Factories : données brutes avec les noms de colonnes originaux (pré-rename)
# ---------------------------------------------------------------------------

def _tournament(**overrides) -> dict:
    base = {
        "Name": "LEC Spring 2024",
        "OverviewPage": "LEC/2024 Season/Spring Season",
        "DateStart": "2024-01-15",
        "League": "LEC",
        "Region": "Europe",
        "Prizepool": "",
        "Currency": "",
        "Country": "",
        "Rulebook": "",
        "EventType": "",
        "Links": "",
        "Sponsors": "",
        "Organizer": "",
        "Organizers": "",
        "Split": "Spring",
        "IsQualifier": "0",
        "IsPlayoffs": "0",
        "IsOfficial": "1",
        "Year": "2024",
    }
    return {**base, **overrides}


def _player(**overrides) -> dict:
    base = {
        "ID": "Faker",
        "OverviewPage": "Faker",
        "Player": "Faker",
        "Image": "",
        "Name": "Lee Sang-hyeok",
        "NativeName": "",
        "NameAlphabet": "",
        "NameFull": "",
        "Country": "South Korea",
        "Nationality": "South Korean",
        "NationalityPrimary": "",
        "Age": "28",
        "Birthdate": "1996-05-07",
        "Deathdate": "",
        "ResidencyFormer": "",
        "Team": "T1",
        "Team2": "",
        "CurrentTeams": "",
        "TeamSystem": "",
        "Team2System": "",
        "Residency": "Korea",
        "Role": "Mid",
        "Contract": "",
        "ContractText": "",
        "FavChamps": "",
        "SoloqueueIds": "",
        "Askfm": "",
        "Bluesky": "",
        "Discord": "",
        "Facebook": "",
        "Instagram": "",
        "Lolpros": "",
        "DPMLOL": "",
        "Reddit": "",
        "Snapchat": "",
        "Stream": "",
        "KICK": "",
        "Twitter": "",
        "Threads": "",
        "LinkedIn": "",
        "Vk": "",
        "Website": "",
        "Weibo": "",
        "Youtube": "",
        "TeamLast": "",
        "RoleLast": "",
        "IsRetired": "0",
        "ToWildrift": "0",
        "ToValorant": "0",
        "ToTFT": "0",
        "ToLegendsOfRuneterra": "0",
        "To2XKO": "0",
        "IsPersonality": "0",
        "IsSubstitute": "0",
        "IsTrainee": "0",
        "IsLowercase": "0",
        "IsAutoTeam": "0",
        "IsLowContent": "0",
    }
    return {**base, **overrides}


def _roster(**overrides) -> dict:
    base = {
        "Team": "Fnatic",
        "OverviewPage": "LEC/2024 Season/Spring Season",
        "RosterLinks": "Caps;;Rekkles",
        "Roles": "Mid;;Bot",
        "Region": "Europe",
        "Footnotes": "",
        "IsUsed": "",
        "IsComplete": "1",
        "PageAndTeam": "LEC/2024 Season/Spring Season_Fnatic",
    }
    return {**base, **overrides}


# ---------------------------------------------------------------------------
# tournaments_silver
# ---------------------------------------------------------------------------

class TestTournamentsSilver:
    def test_writes_to_correct_snowflake_table(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_tournament()]

        tournaments_silver(ctx, mock_s3, mock_snowflake)

        table = mock_snowflake.truncate_and_insert.call_args[0][0]
        assert table == TOURNAMENTS_SNOWFLAKE_TABLE

    def test_deduplicates_on_overview_page(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [
            _tournament(),
            _tournament(),  # duplicate overview_page
            _tournament(OverviewPage="LEC/2024 Season/Summer Season"),
        ]

        tournaments_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        assert len(rows) == 2

    def test_metadata_row_count(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [
            _tournament(),
            _tournament(OverviewPage="LEC/2024 Season/Summer Season"),
        ]

        result = tournaments_silver(ctx, mock_s3, mock_snowflake)

        assert result.metadata["row_count"] == 2

    def test_columns_are_renamed(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_tournament()]

        tournaments_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        assert "overview_page" in rows[0]
        assert "OverviewPage" not in rows[0]


# ---------------------------------------------------------------------------
# players_silver
# ---------------------------------------------------------------------------

class TestPlayersSilver:
    def test_writes_to_correct_snowflake_table(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_player()]

        players_silver(ctx, mock_s3, mock_snowflake)

        table = mock_snowflake.truncate_and_insert.call_args[0][0]
        assert table == PLAYERS_SNOWFLAKE_TABLE

    def test_deduplicates_on_overview_page(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [
            _player(),
            _player(),  # duplicate
            _player(OverviewPage="Caps", Player="Caps"),
        ]

        players_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        assert len(rows) == 2

    def test_metadata_row_count(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_player()]

        result = players_silver(ctx, mock_s3, mock_snowflake)

        assert result.metadata["row_count"] == 1


# ---------------------------------------------------------------------------
# tournament_rosters_silver
# ---------------------------------------------------------------------------

class TestTournamentRostersSilver:
    def test_writes_to_correct_snowflake_table(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_roster()]

        tournament_rosters_silver(ctx, mock_s3, mock_snowflake)

        table = mock_snowflake.truncate_and_insert.call_args[0][0]
        assert table == TOURNAMENT_ROSTERS_SNOWFLAKE_TABLE

    def test_explodes_player_links(self, ctx, mock_s3, mock_snowflake):
        """Un roster avec 2 joueurs doit produire 2 lignes après explode."""
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_roster()]  # Caps;;Rekkles → 2 joueurs

        tournament_rosters_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        assert len(rows) == 2

    def test_drops_empty_roster_rows(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [
            _roster(),                       # 2 joueurs → 2 lignes
            _roster(RosterLinks=""),         # vide → doit être ignoré
        ]

        tournament_rosters_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        assert len(rows) == 2

    def test_deduplicates_on_composite_key(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [
            _roster(RosterLinks="Caps", Roles="Mid"),
            _roster(RosterLinks="Caps", Roles="Mid"),  # même (team, overview_page, player_link)
        ]

        tournament_rosters_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        assert len(rows) == 1

    def test_role_aligned_with_player(self, ctx, mock_s3, mock_snowflake):
        """Les rôles doivent être alignés sur les joueurs après explode."""
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [
            _roster(RosterLinks="Caps;;Rekkles", Roles="Mid;;Bot"),
        ]

        tournament_rosters_silver(ctx, mock_s3, mock_snowflake)

        rows = mock_snowflake.truncate_and_insert.call_args[0][1]
        roles = {r["player_link"]: r["role"] for r in rows}
        assert roles["Caps"] == "Mid"
        assert roles["Rekkles"] == "Bot"

    def test_metadata_row_count(self, ctx, mock_s3, mock_snowflake):
        mock_s3.get_latest_key.return_value = "bronze/key"
        mock_s3.download_json.return_value = [_roster()]  # 2 joueurs

        result = tournament_rosters_silver(ctx, mock_s3, mock_snowflake)

        assert result.metadata["row_count"] == 2
