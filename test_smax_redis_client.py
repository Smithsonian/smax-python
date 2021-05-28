from smax import SmaxRedisClient
import pytest


@pytest.fixture
def smax_client():
    return SmaxRedisClient("localhost")


def test_share_pull_string(smax_client):
    expected_data = "just a string"
    expected_type = "str"
    expected_dim = 1
    table = "pytest"
    key = "test_share_pull_string"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_share_pull_int(smax_client):
    expected_data = 123456789
    expected_type = "integer"
    expected_dim = 1
    table = "pytest"
    key = "test_share_pull_int"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_share_pull_string_list(smax_client):
    expected_data = ["i", "am", "list"]
    expected_type = "str"
    expected_dim = 3
    table = "pytest"
    key = "test_share_pull_string_list"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim