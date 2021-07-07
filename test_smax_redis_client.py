import socket
from time import sleep

import numpy as np
import pytest
from redis import TimeoutError

from smax import SmaxRedisClient


@pytest.fixture
def smax_client():
    return SmaxRedisClient("localhost")


def test_context_manager():
    expected_data = "just a context manager string"
    expected_type = str
    expected_dim = 1
    table = "pytest"
    key = "test_context_manager_string"
    with SmaxRedisClient("localhost") as s:
        s.smax_share(table, key, expected_data)
        result = s.smax_pull(table, key)

    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_roundtrip_string(smax_client):
    expected_data = "just a roundtrip string"
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
    key = "test_roundtrip_float_list"
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


def test_pubsub(smax_client):
    smax_client.smax_subscribe("pytest:test_pubsub")
    expected_data = "just a string"
    expected_type = str
    expected_dim = 1
    table = "pytest"
    key = "test_pubsub"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_wait_on_any_subscribed()
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_pubsub_pattern(smax_client):
    smax_client.smax_subscribe("pytest:test_pubsub*")
    expected_data = "just a string"
    expected_type = str
    expected_dim = 1
    table = "pytest:test_pubsub"
    key = "pattern"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_wait_on_any_subscribed()
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_pubsub_with_timeout(smax_client):
    smax_client.smax_subscribe("pytest:test_pubsub_with_timeout")
    expected_data = "just a timeout string"
    expected_type = str
    expected_dim = 1
    table = "pytest"
    key = "test_pubsub_with_timeout"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_wait_on_any_subscribed(timeout=3.0)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim


def test_pubsub_with_timeout_exception(smax_client):
    smax_client.smax_subscribe("pytest:test_pubsub")
    with pytest.raises(TimeoutError):
        smax_client.smax_wait_on_any_subscribed(timeout=.5)


def test_pubsub_notification(smax_client):
    smax_client.smax_subscribe("pytest:test_pubsub")
    expected_data = socket.gethostname()
    expected_channel = "smax:pytest:test_pubsub"
    table = "pytest"
    key = "test_pubsub"
    smax_client.smax_share(table, key, "doesn't matter")
    result = smax_client.smax_wait_on_any_subscribed(notification_only=True)
    assert result["data"].decode("utf-8") == expected_data
    assert result["channel"].decode("utf-8") == expected_channel


def test_pubsub_wait_on_pattern(smax_client):
    smax_client.smax_subscribe("pytest:test_pubsub*")
    expected_data1 = 666
    expected_data2 = 33
    smax_client.smax_share("pytest:test_pubsub:nop", "nop", "nopvalue")
    smax_client.smax_share("pytest:test_pubsub:fpga", "temp", expected_data1)
    smax_client.smax_share("pytest:test_pubsub:fpga", "speed", expected_data2)
    result1 = smax_client.smax_wait_on_subscribed("pytest:test_pubsub:fpga*")
    result2 = smax_client.smax_wait_on_subscribed("pytest:test_pubsub:fpga*")
    assert result1["pytest"]["test_pubsub"]["fpga"]["temp"].data == expected_data1
    assert result2["pytest"]["test_pubsub"]["fpga"]["speed"].data == expected_data2


def test_pubsub_pattern_callback(smax_client):
    expected_value = 42

    # Inner functions can't modify outer variables unless they are mutable.
    actual = {"value": None}

    def my_callback(message):
        actual["value"] = message["pytest"]["pubsub_callback"]["fpga1"]["temp"].data

    smax_client.smax_subscribe("pytest:pubsub_callback*", my_callback)
    smax_client.smax_share("pytest:pubsub_callback:fpga1", "temp", expected_value)

    # Sleep and then check actual value
    sleep(.1)
    smax_client.smax_unsubscribe()
    assert actual["value"] == expected_value


def test_pubsub_callback(smax_client):
    expected_value = 42

    # Inner functions can't modify outer variables unless they are mutable.
    actual = {"value": None}

    def my_callback(message):
        actual["value"] = message.data

    smax_client.smax_subscribe("pytest:callback:fpga1:temp", my_callback)
    smax_client.smax_share("pytest:callback:fpga1", "temp", expected_value)

    # Sleep and then check actual value
    sleep(.1)
    smax_client.smax_unsubscribe()
    assert actual["value"] == expected_value


def test_multiple_pubsub_callback(smax_client):
    expected_value1 = 42
    expected_value2 = 24

    # Inner functions can't modify outer variables unless they are mutable.
    actual = {"value1": None, "value2": None}

    def my_callback1(message):
        actual["value1"] = message.data

    def my_callback2(message):
        actual["value2"] = message.data

    smax_client.smax_subscribe("pytest:callback:fpga1:temp", my_callback1)
    smax_client.smax_subscribe("pytest:callback:fpga2:temp", my_callback2)
    smax_client.smax_share("pytest:callback:fpga1", "temp", expected_value1)
    smax_client.smax_share("pytest:callback:fpga2", "temp", expected_value2)

    # Sleep and then check actual value
    sleep(.5)
    assert actual["value1"] == expected_value1
    assert actual["value2"] == expected_value2


def test_pull_struct(smax_client):
    expected_temp_value1 = np.array([42, 24], dtype=np.int32)
    expected_temp_value2 = np.array([24, 42], dtype=np.int32)
    expected_firmware_value1 = 1.0
    expected_firmware_value2 = 1.1
    expected_type_temp = np.int32
    expected_dim_temp = 2
    expected_type_firmware = float
    expected_dim_firmware = 1

    smax_client.smax_share("swarm:dbe:roach2-01", "temp", expected_temp_value1)
    smax_client.smax_share("swarm:dbe:roach2-02", "temp", expected_temp_value2)
    smax_client.smax_share("swarm:dbe:roach2-01", "firmware", expected_firmware_value1)
    smax_client.smax_share("swarm:dbe:roach2-02", "firmware", expected_firmware_value2)
    result = smax_client.smax_pull("swarm", "dbe")

    roach01_temp = result["swarm"]["dbe"]["roach2-01"]["temp"]
    roach02_temp = result["swarm"]["dbe"]["roach2-02"]["temp"]
    roach01_firmware = result["swarm"]["dbe"]["roach2-01"]["firmware"]
    roach02_firmware = result["swarm"]["dbe"]["roach2-02"]["firmware"]

    assert (roach01_temp.data == expected_temp_value1).all()
    assert (roach02_temp.data == expected_temp_value2).all()
    assert roach01_firmware.data == expected_firmware_value1
    assert roach02_firmware.data == expected_firmware_value2
    assert roach01_temp.type == expected_type_temp
    assert roach02_temp.type == expected_type_temp
    assert roach01_firmware.type == expected_type_firmware
    assert roach02_firmware.type == expected_type_firmware
    assert roach01_temp.dim == expected_dim_temp
    assert roach02_temp.dim == expected_dim_temp
    assert roach01_firmware.dim == expected_dim_firmware
    assert roach02_firmware.dim == expected_dim_firmware


def test_share_struct(smax_client):
    expected_temp_value1 = 100
    expected_temp_value2 = 0
    expected_firmware_value1 = 2.0
    expected_firmware_value2 = 2.1
    expected_type_temp = int
    expected_dim_temp = 1
    expected_type_firmware = float
    expected_dim_firmware = 1

    struct = {"roach2-03": {"temp": expected_temp_value1, "firmware": expected_firmware_value1},
              "roach2-04": {"temp": expected_temp_value2, "firmware": expected_firmware_value2}}

    smax_client.smax_share("swarm", "dbe", struct)
    result = smax_client.smax_pull("swarm", "dbe")
    roach03_temp = result["swarm"]["dbe"]["roach2-03"]["temp"]
    roach04_temp = result["swarm"]["dbe"]["roach2-04"]["temp"]
    roach03_firmware = result["swarm"]["dbe"]["roach2-03"]["firmware"]
    roach04_firmware = result["swarm"]["dbe"]["roach2-04"]["firmware"]

    # Data and type checks.
    assert roach03_temp.data == expected_temp_value1
    assert roach04_temp.data == expected_temp_value2
    assert roach03_firmware.data == expected_firmware_value1
    assert roach04_firmware.data == expected_firmware_value2
    assert roach03_temp.type == expected_type_temp
    assert roach04_temp.type == expected_type_temp
    assert roach03_firmware.type == expected_type_firmware
    assert roach04_firmware.type == expected_type_firmware
    assert roach03_temp.dim == expected_dim_temp
    assert roach04_temp.dim == expected_dim_temp
    assert roach03_firmware.dim == expected_dim_firmware
    assert roach04_firmware.dim == expected_dim_firmware


def test_roundtrip_meta(smax_client):
    # Do a normal share to generate the automatic metadata.
    smax_client.smax_share("pytest", "test_roundtrip_meta", "Doesn't Matter")

    # Now change metadata for timestamps to equal "1".
    expected_value = "1"
    smax_client.smax_push_meta("timestamps", "pytest:test_roundtrip_meta", expected_value)

    # Now pull just metadata.
    result = smax_client.smax_pull_meta("pytest:test_roundtrip_meta", "timestamps")
    assert result == expected_value


def test_description_meta(smax_client):
    # Do a normal share to generate the automatic metadata.
    smax_client.smax_share("pytest", "test_units_meta", "Doesn't Matter")

    # Now add units metadata.
    expected_value = "I am a description"
    smax_client.smax_set_description("pytest:test_description_meta", expected_value)

    # Now pull just metadata.
    result = smax_client.smax_get_description("pytest:test_description_meta")
    assert result == expected_value


def test_units_meta(smax_client):
    # Do a normal share to generate the automatic metadata.
    smax_client.smax_share("pytest", "test_units_meta", "Doesn't Matter")

    # Now add units metadata.
    expected_value = "feet"
    smax_client.smax_set_units("pytest:test_units_meta", expected_value)

    # Now pull just metadata.
    result = smax_client.smax_get_units("pytest:test_units_meta")
    assert result == expected_value
