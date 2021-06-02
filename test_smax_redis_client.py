import numpy as np
import pytest

from smax import SmaxRedisClient


@pytest.fixture
def smax_client():
    return SmaxRedisClient("localhost")


def test_roundtrip_string(smax_client):
    expected_data = "just a string"
    expected_type = str
    expected_dim = 1
    table = "pytest"
    key = "test_roundtrip_string"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_roundtrip_int(smax_client):
    expected_data = 123456789
    expected_type = int
    expected_dim = 1
    table = "pytest"
    key = "test_roundtrip_int"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_roundtrip_string_list(smax_client):
    expected_data = ["i", "am", "list"]
    expected_type = str
    expected_dim = 3
    table = "pytest"
    key = "test_roundtrip_string_list"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_roundtrip_int_list(smax_client):
    data = [0, 1, -1, 100000]
    expected_data = np.array(data)
    expected_type = type(data[0])
    expected_dim = len(data)
    table = "pytest"
    key = "test_roundtrip_string_list"
    smax_client.smax_share(table, key, data)
    result = smax_client.smax_pull(table, key)
    assert np.array_equal(result.data, expected_data.data)
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_roundtrip_float_list(smax_client):
    data = [0.0, 1.12345, -1.54321, 100000.12345]
    expected_data = np.array(data)
    expected_type = expected_data.dtype
    expected_dim = len(data)
    table = "pytest"
    key = "test_roundtrip_string_list"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert np.array_equal(result.data, expected_data.data)
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_roundtrip_2d_float_array(smax_client):
    expected_data = np.array([[0.0, 1.1],
                              [1.12345, 2.123456],
                              [-1.654321, -1.54321]])
    expected_type = expected_data.dtype
    expected_dim = expected_data.shape
    table = "pytest"
    key = "test_roundtrip_2d_float_array"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert np.array_equal(result.data, expected_data.data)
    assert result.type == expected_type
    assert result.dim == expected_dim
