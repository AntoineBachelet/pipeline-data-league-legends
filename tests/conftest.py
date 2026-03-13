import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


@pytest.fixture
def mock_s3():
    return MagicMock()


@pytest.fixture
def mock_snowflake():
    return MagicMock()


@pytest.fixture
def mock_leaguepedia():
    return MagicMock()
