[![pytest](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/python_tests.yml/badge.svg)](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/python_tests.yml)

# SMAX for Python 
Client side python library for communicating with SMA-X (aka Redis).

Version 1.0.3

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
### Upgrade with pip directly from github
```bash
pip install git+ssh://git@github.com/Smithsonian/smax-python.git -U
```

### Clone repo and install in editable mode
```bash
git clone git@github.com:Smithsonian/smax-python.git
cd smax-python
pip install . -e
```

### Set up redis server for local SMAX testing and development
* Install Redis or Valkey or equivalent, e.g.:
  * Using [Conda](https://anaconda.org/conda-forge/redis-server)
  * For [Raspberry pi](https://redis.io/topics/ARM)
  * [From source](https://redis.io/topics/quickstart)
* Set up the SMA-X server on top of Redis
  * Clone [Smithsonian/smax-server](https://github.com/Smithsonian/smax-server)
  * In `smax-server`: edit `redis.conf` files as necessary or desired for your application.
  * In `smax-server`: run `install.sh`.
* Clone this repo locally
* That's it, you now have SMAX running locally!


# Python Examples
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

