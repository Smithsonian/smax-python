[![pytest](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/python_tests.yml/badge.svg)](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/python_tests.yml)

# SMAX for Python 
Client side python library for communicating with SMA-X (aka Redis).

[API Documentation](https://ideal-funicular-124903cd.pages.github.io/)

# Install
There is a proper python package in this repo named "smax".  You can use pip to install
directly from the repo, which is useful if you just need to import the library for your
own application. For development of this package, clone the repo and install with the 
editable flag. 

### Install with pip directly from github
```bash
pip install git+ssh://git@github.com/Smithsonian/smax-python.git
```

### Clone repo and install in editable mode
```bash
git clone git@github.com:Smithsonian/smax-python.git
cd smax-python
pip install . -e
```

# Examples
The best place to find example usages is the unit tests (test_smax_redis_client.py), but here are 
a few simple ones to help get you started.

### Share/Pull roundtrip
```python
from smax import SmaxRedisClient

smax_client = SmaxRedisClient("localhost") # Replace localhost with redis hostname or IP.
value = 0.183
table = "weather:forecast:gfs"
key = "test_tau"
smax_client.smax_share(table, key, value)
result = smax_client.smax_pull(table, key)
print(result.data, result.type)
```
### Pubsub
```python
from smax import SmaxRedisClient

smax_client = SmaxRedisClient("localhost") # Replace localhost with redis hostname or IP.
table = "weather:forecast:gfs"
key = "test_array"
smax_client.smax_subscribe(f"{table}:{key}")

# Share something, which send publish notifications automatically.
value = [0.0, 1.12345, -1.54321, 100000.12345]
smax_client.smax_share(table, key, value)

# Wait for publish notifications.
result = smax_client.smax_wait_on_any_subscribed()
print(result.data, result.type)
```

