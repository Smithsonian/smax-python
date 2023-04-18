import socket
import threading
from time import sleep
import logging

import psutil
import os
import subprocess
import numpy as np
import pytest
from redis import TimeoutError

from smax import SmaxRedisClient

smax_redis_ip = "127.0.0.1"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logger.debug("In test_smax_redis_client.py")

@pytest.fixture
def smax_client():
    logger.debug("In test_smax_redis_client.py:smax_client test fixture")
    return SmaxRedisClient(smax_redis_ip)

def test_redis_connection():
    ps = subprocess.run(f"redis-cli -h {smax_redis_ip} PING".split(" "), capture_output=True)
    assert ps.stdout == b'PONG\n'
    
    
def test_redis_scripts():
    ps = subprocess.run(f"redis-cli -h {smax_redis_ip} KEYS *".split(" "), capture_output=True)
    keys = ps.stdout.split(b'\n')
    assert b'scripts' in keys
    

def test_redis_HGetWithMeta():
    ps = subprocess.run(f"redis-cli -h {smax_redis_ip} HGET scripts HGetWithMeta".split(" "), capture_output=True)
    logger.debug(ps.stdout)
    hget_sha = ps.stdout.decode().strip()
    ps = subprocess.run(f"redis-cli -h {smax_redis_ip} EVALSHA {hget_sha} 1 scripts HGetWithMeta".split(" "), capture_output=True)
    logger.debug(ps)
    assert ps.stdout.decode().strip() == hget_sha
    
    
def test_context_manager():
    expected_data = "just a context manager string"
    expected_type = str
    expected_dim = 1
    table = "test_context_manager"
    key = "pytest"
    with SmaxRedisClient(smax_redis_ip) as s:
        s.smax_share(table, key, expected_data)
        # Run a raw test of redis values
        logger.debug(f"Got HGetWithMeta SHA: {s._getSHA}")
        result = s.smax_pull(table, key)

    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_roundtrip_string(smax_client):
    expected_data = "just a roundtrip string"
    expected_type = str
    expected_dim = 1
    table = "test_roundtrip_string"
    key = "pytest"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_roundtrip_int(smax_client):
    expected_data = 123456789
    expected_type = int
    expected_dim = 1
    table = "test_roundtrip_int"
    key = "pytest"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_roundtrip_string_list(smax_client):
    expected_data = ["i", "am", "list"]
    expected_type = str
    expected_dim = 3
    table = "test_roundtrip_string_list"
    key = "pytest"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_roundtrip_int_list(smax_client):
    data = [0, 1, -1, 100000]
    expected_data = np.array(data)
    expected_type = type(data[0])
    expected_dim = len(data)
    table = "test_roundtrip_int_list"
    key = "pytest"
    smax_client.smax_share(table, key, data)
    result = smax_client.smax_pull(table, key)
    assert np.array_equal(result.data, expected_data.data)
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_roundtrip_float_list(smax_client):
    data = [0.0, 1.12345, -1.54321, 100000.12345]
    expected_data = np.array(data)
    expected_type = expected_data.dtype
    expected_dim = len(data)
    table = "test_roundtrip_float_list"
    key = "pytest"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert np.array_equal(result.data, expected_data.data)
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_roundtrip_2d_float_array(smax_client):
    expected_data = np.array([[0.0, 1.1],
                              [1.12345, 2.123456],
                              [-1.654321, -1.54321]])
    expected_type = expected_data.dtype
    expected_dim = expected_data.shape
    table = "test_roundtrip_2d_float_array"
    key = "pytest"
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_pull(table, key)
    assert np.array_equal(result.data, expected_data.data)
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_pubsub(smax_client):
    expected_data = "just a string"
    expected_type = str
    expected_dim = 1
    table = "test_pubsub"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}")
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_wait_on_any_subscribed()
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_pubsub_pattern(smax_client):
    expected_data = "just a string"
    expected_type = str
    expected_dim = 1
    table = "test_pubsub_pattern"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}*")
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_wait_on_any_subscribed()
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_pubsub_with_timeout(smax_client):
    expected_data = "just a timeout string"
    expected_type = str
    expected_dim = 1
    table = "test_pubsub_with_timeout"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}")
    smax_client.smax_share(table, key, expected_data)
    result = smax_client.smax_wait_on_any_subscribed(timeout=3.0)
    assert result.data == expected_data
    assert result.type == expected_type
    assert result.dim == expected_dim
    assert result.smaxname == f"{table}:{key}"


def test_pubsub_with_timeout_exception(smax_client):
    table = "test_pubsub_with_timeout_exception"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}")
    with pytest.raises(TimeoutError):
        smax_client.smax_wait_on_any_subscribed(timeout=.5)


def test_pubsub_notification(smax_client):
    table = "test_pubsub_notification"
    key = "pytest"
    
    program_name = psutil.Process(os.getpid()).name()
    
    expected_data = f"{socket.gethostname()}:{program_name}"
    
    smax_client.smax_subscribe(f"{table}:{key}")
    smax_client.smax_share(table, key, "doesn't matter")
    
    result = smax_client.smax_wait_on_any_subscribed(notification_only=True)
    logger.debug(f"Received result: {result}")
    assert result["data"] == expected_data


def test_pubsub_wait_on_pattern(smax_client):
    table = "test_pubsub_wait_on_pattern"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}*")
    expected_data1 = 666
    expected_data2 = 33

    with SmaxRedisClient("localhost") as smax_producer:
        smax_producer.smax_share(f"{table}:{key}:nop", "nop", "nopvalue")
        smax_producer.smax_share(f"{table}:{key}:fpga", "temp", expected_data1)
        smax_producer.smax_share(f"{table}:{key}:fpga", "speed", expected_data2)

    result1 = smax_client.smax_wait_on_subscribed(f"{table}:{key}:fpga*")
    result2 = smax_client.smax_wait_on_subscribed(f"{table}:{key}:fpga*")
    logger.debug(f"Received result1: {result1}")
    logger.debug(f"Received result1: {result2}")
    
    assert result1["fpga"]["temp"].data == expected_data1
    assert result2["fpga"]["speed"].data == expected_data2


def test_pubsub_pattern_callback(smax_client):
    table = "test_pubsub_pattern_callback"
    key = "pytest"
    expected_value = 42

    # Inner functions can't modify outer variables unless they are mutable.
    actual = {"value": None}

    def my_callback(message):
        logger.debug(f"my_callback received message:\n{message}")
        actual["value"] = message[key]["fpga1"]["temp"].data

    smax_client.smax_subscribe(f"{table}:{key}*", callback=my_callback)
    with SmaxRedisClient("localhost") as smax_producer:
        smax_producer.smax_share(f"{table}:{key}:fpga1", "temp", expected_value)

    # Sleep and then check actual value
    sleep(1)
    assert actual["value"] == expected_value


def test_pubsub_callback(smax_client):
    expected_value = 42

    # Inner functions can't modify outer variables unless they are mutable.
    actual = {"value": None}

    def my_callback(message):
        actual["value"] = message.data

    table = "test_pubsub_callback"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}:fpga1:temp", callback=my_callback)

    with SmaxRedisClient("localhost") as smax_producer:
        smax_producer.smax_share(f"{table}:{key}:fpga1", "temp", expected_value)

    # Sleep and then check actual value
    sleep(1)
    smax_client.smax_unsubscribe()
    assert actual["value"] == expected_value


def test_multiple_pubsub_callback(smax_client):
    expected_value1 = 42
    expected_value2 = 24

    # Inner functions can't modify outer variables unless they are mutable.
    actual1 = {"value1": None}
    actual2 = {"value2": None}

    def my_callback1(message):
        actual1["value1"] = message.data

    def my_callback2(message):
        actual2["value2"] = message.data

    table = "test_multiple_pubsub_callback"
    key = "pytest"
    smax_client.smax_subscribe(f"{table}:{key}:fpga1:temp", callback=my_callback1)
    smax_client.smax_subscribe(f"{table}:{key}:fpga2:temp", callback=my_callback2)

    with SmaxRedisClient("localhost") as smax_producer:
        smax_producer.smax_share(f"{table}:{key}:fpga1", "temp", expected_value1)
        sleep(.1)  # Sleep a little bit in between these.
        smax_producer.smax_share(f"{table}:{key}:fpga2", "temp", expected_value2)

    # Sleep and then check actual value.
    # The long sleep only seems needs on Windows, mac and linux work with .1s.
    sleep(1)
    assert actual1["value1"] == expected_value1
    assert actual2["value2"] == expected_value2


def test_mixed_pubsub_callback(smax_client):

    def my_callback1(message):
        actual1["value1"] = message.data

    def producer():
        sleep(.1)
        smax_client.smax_share(table, "nocallback", expected_data)

    expected_data = "just a string"
    table = "test_mixed_pubsub_callback"
    expected_value1 = 42

    # Inner functions can't modify outer variables unless they are mutable.
    actual1 = {"value1": None}

    # Subscribes with callbacks have to be declared first for some reason.
    smax_client.smax_subscribe(f"{table}:nocallback")
    smax_client.smax_subscribe(f"{table}:callback:fpga1:temp", callback=my_callback1)

    # Create a seperate thread that will share a value while the wait is active.
    delayed_producer = threading.Thread(target=producer)
    delayed_producer.start()

    # Now wait for the thread to share, and check the result.
    result = smax_client.smax_wait_on_any_subscribed()
    delayed_producer.join()
    assert result.data == expected_data

    # This call to smax_share will trigger the callback.
    smax_client.smax_share(f"{table}:callback:fpga1", "temp", expected_value1)

    # Sleep and then check callback actual value.
    # The long sleep only seems needs on Windows, mac and linux work with .1s.
    sleep(1)
    assert actual1["value1"] == expected_value1


def test_pull_struct(smax_client):
    expected_temp_value1 = np.array([42, 24], dtype=np.int32)
    expected_temp_value2 = np.array([24, 42], dtype=np.int32)
    expected_firmware_value1 = 1.0
    expected_firmware_value2 = 1.1
    expected_type_temp = np.int32
    expected_dim_temp = 2
    expected_type_firmware = float
    expected_dim_firmware = 1

    table = "test_pull_struct"

    smax_client.smax_share(f"{table}:swarm:dbe:roach2-01", "temp", expected_temp_value1)
    smax_client.smax_share(f"{table}:swarm:dbe:roach2-02", "temp", expected_temp_value2)
    smax_client.smax_share(f"{table}:swarm:dbe:roach2-01", "firmware", expected_firmware_value1)
    smax_client.smax_share(f"{table}:swarm:dbe:roach2-02", "firmware", expected_firmware_value2)
    result = smax_client.smax_pull(f"{table}:swarm", "dbe")
    logger.debug(f"Pull result with keys: {list(result.keys())}")

    roach01_temp = result["dbe"]["roach2-01"]["temp"]
    roach02_temp = result["dbe"]["roach2-02"]["temp"]
    roach01_firmware = result["dbe"]["roach2-01"]["firmware"]
    roach02_firmware = result["dbe"]["roach2-02"]["firmware"]

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
    table = "test_share_struct"
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

    smax_client.smax_share(f"{table}:swarm", "dbe", struct)
    result = smax_client.smax_pull(f"{table}:swarm", "dbe")
    
    logger.debug(f"pulled dbe struct keys: {list(result['dbe'].keys())}")

    roach03_temp = result["dbe"]["roach2-03"]["temp"]
    roach04_temp = result["dbe"]["roach2-04"]["temp"]
    roach03_firmware = result["dbe"]["roach2-03"]["firmware"]
    roach04_firmware = result["dbe"]["roach2-04"]["firmware"]

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
    assert roach03_temp.smaxname == f"{table}:swarm:dbe:roach2-03:temp"
    assert roach04_temp.smaxname == f"{table}:swarm:dbe:roach2-04:temp"
    assert roach03_firmware.smaxname == f"{table}:swarm:dbe:roach2-03:firmware"
    assert roach04_firmware.smaxname == f"{table}:swarm:dbe:roach2-04:firmware"

def test_roundtrip_meta(smax_client):
    # Do a normal share to generate the automatic metadata.
    table = "test_roundtrip_meta"
    key = "pytest"
    smax_client.smax_share(table, key, "Doesn't Matter")

    # Now change metadata for timestamps to equal "1".
    expected_value = "1"
    smax_client.smax_push_meta("timestamps", f"{table}:{key}", expected_value)

    # Now pull just metadata.
    result = smax_client.smax_pull_meta(f"{table}:{key}", "timestamps")
    assert result == expected_value


def test_description_meta(smax_client):
    table = "test_description_meta"
    key = "pytest"

    # Do a normal share to generate the automatic metadata.
    smax_client.smax_share(table, key, "Doesn't Matter")

    # Now add units metadata.
    expected_value = "I am a description"
    smax_client.smax_set_description(f"{table}:{key}", expected_value)

    # Now pull just metadata.
    result = smax_client.smax_get_description(f"{table}:{key}")
    assert result == expected_value


def test_units_meta(smax_client):
    table = "test_units_meta"
    key = "pytest"

    # Do a normal share to generate the automatic metadata.
    smax_client.smax_share(table, key, "Doesn't Matter")

    # Now add units metadata.
    expected_value = "feet"
    smax_client.smax_set_units(f"{table}:{key}", expected_value)

    # Now pull just metadata.
    result = smax_client.smax_get_units(f"{table}:{key}")
    assert result == expected_value
