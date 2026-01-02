import pytest


@pytest.fixture
def access_token():
    pytest.skip("External KIS API not available in test environment")


@pytest.fixture
def token():
    pytest.skip("External KIS API not available in test environment")
