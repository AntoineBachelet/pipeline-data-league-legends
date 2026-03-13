import pytest
from unittest.mock import MagicMock, patch
from dagster import build_asset_context

from dagster_lol.assets.bronze.leaguepedia import (
    tournaments_bronze,
    players_bronze,
    tournament_rosters_bronze,
    PAGE_SIZE,
)


@pytest.fixture
def ctx():
    return build_asset_context()


# ---------------------------------------------------------------------------
# tournaments_bronze
# ---------------------------------------------------------------------------

class TestTournamentsBronze:
    def test_single_page_uploads_to_s3(self, ctx, mock_s3, mock_leaguepedia):
        rows = [{"Name": f"T{i}"} for i in range(10)]
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = rows
        mock_s3.upload_json.return_value = "s3://bucket/key"

        result = tournaments_bronze(ctx, mock_leaguepedia, mock_s3)

        mock_s3.upload_json.assert_called_once()
        assert result.metadata["row_count"] == 10

    def test_multi_page_aggregates_all_rows(self, ctx, mock_s3, mock_leaguepedia):
        page1 = [{"Name": "T"}] * PAGE_SIZE
        page2 = [{"Name": "Last"}] * 5
        mock_leaguepedia.get_client.return_value.cargo_client.query.side_effect = [page1, page2]
        mock_s3.upload_json.return_value = "s3://bucket/key"

        with patch("dagster_lol.assets.bronze.leaguepedia.time.sleep"):
            result = tournaments_bronze(ctx, mock_leaguepedia, mock_s3)

        assert result.metadata["row_count"] == PAGE_SIZE + 5

    def test_s3_key_format(self, ctx, mock_s3, mock_leaguepedia):
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = []
        mock_s3.upload_json.return_value = "s3://bucket/key"

        tournaments_bronze(ctx, mock_leaguepedia, mock_s3)

        _, key = mock_s3.upload_json.call_args[0]
        assert key.startswith("bronze/leaguepedia/tournaments/")
        assert key.endswith("/tournaments.json")

    def test_metadata_contains_s3_path(self, ctx, mock_s3, mock_leaguepedia):
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = []
        mock_s3.upload_json.return_value = "s3://bucket/bronze/key"

        result = tournaments_bronze(ctx, mock_leaguepedia, mock_s3)

        assert result.metadata["s3_path"] == "s3://bucket/bronze/key"


# ---------------------------------------------------------------------------
# players_bronze
# ---------------------------------------------------------------------------

class TestPlayersBronze:
    def test_single_page(self, ctx, mock_s3, mock_leaguepedia):
        rows = [{"Player": "Faker"}, {"Player": "Caps"}, {"Player": "Rekkles"}]
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = rows
        mock_s3.upload_json.return_value = "s3://bucket/key"

        result = players_bronze(ctx, mock_leaguepedia, mock_s3)

        assert result.metadata["row_count"] == 3

    def test_s3_key_format(self, ctx, mock_s3, mock_leaguepedia):
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = []
        mock_s3.upload_json.return_value = "s3://bucket/key"

        players_bronze(ctx, mock_leaguepedia, mock_s3)

        _, key = mock_s3.upload_json.call_args[0]
        assert key.startswith("bronze/leaguepedia/players/")
        assert key.endswith("/players.json")


# ---------------------------------------------------------------------------
# tournament_rosters_bronze
# ---------------------------------------------------------------------------

class TestTournamentRostersBronze:
    def test_single_page(self, ctx, mock_s3, mock_leaguepedia):
        rows = [{"Team": "Fnatic", "RosterLinks": "Caps;;Rekkles"}] * 7
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = rows
        mock_s3.upload_json.return_value = "s3://bucket/key"

        result = tournament_rosters_bronze(ctx, mock_leaguepedia, mock_s3)

        assert result.metadata["row_count"] == 7

    def test_s3_key_format(self, ctx, mock_s3, mock_leaguepedia):
        mock_leaguepedia.get_client.return_value.cargo_client.query.return_value = []
        mock_s3.upload_json.return_value = "s3://bucket/key"

        tournament_rosters_bronze(ctx, mock_leaguepedia, mock_s3)

        _, key = mock_s3.upload_json.call_args[0]
        assert key.startswith("bronze/leaguepedia/tournamentrosters/")
        assert key.endswith("/tournamentrosters.json")

    def test_multi_page_pagination(self, ctx, mock_s3, mock_leaguepedia):
        page1 = [{"Team": "T"}] * PAGE_SIZE
        page2 = [{"Team": "T"}] * 3
        mock_leaguepedia.get_client.return_value.cargo_client.query.side_effect = [page1, page2]
        mock_s3.upload_json.return_value = "s3://bucket/key"

        with patch("dagster_lol.assets.bronze.leaguepedia.time.sleep"):
            result = tournament_rosters_bronze(ctx, mock_leaguepedia, mock_s3)

        assert result.metadata["row_count"] == PAGE_SIZE + 3
