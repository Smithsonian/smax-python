[![Unit tests](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/test.yml/badge.svg)](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/test.yml)
[![API Documentation](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/docs.yml/badge.svg)](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/docs.yml)
[![codecov](https://codecov.io/gh/Smithsonian/smax-python/graph/badge.svg?token=BTEFMR9W4F)](https://codecov.io/gh/Smithsonian/smax-python)

<picture>
  <source srcset="resources/CfA-logo-dark.png" alt="CfA logo" media="(prefers-color-scheme: dark)"/>
  <source srcset="resources/CfA-logo.png" alt="CfA logo" media="(prefers-color-scheme: light)"/>
  <img src="resources/CfA-logo.png" alt="CfA logo" width="400" height="67" align="right"/>
</picture>
<br clear="all">

# SMAX for Python 

Client side python library for the [SMA Exchange (SMA-X)](https://docs.google.com/document/d/1eYbWDClKkV7JnJxv4MxuNBNV47dFXuUWu7C4Ve_YTf0/edit?usp=sharing) realtime structured database.

Version 1.2.4

- [API Documentation](https://smithsonian.github.io/smax-python/)
 

## Table of Contents

 - [Introduction](#introduction)
 - [Installing](#installing)
 - [Examples](#examples)

<a name="introduction"></a>
## Introduction

The [SMA Exchange (SMA-X)](https://docs.google.com/document/d/1eYbWDClKkV7JnJxv4MxuNBNV47dFXuUWu7C4Ve_YTf0/edit?usp=sharing) 
is a high performance and versatile real-time data sharing platform for distributed software systems. It is built 
around a central Redis database, and provides atomic access to structured data, including specific branches and/or 
leaf nodes, with associated metadata. SMA-X was developed at the Submillimeter Array (SMA) observatory, where we use 
it to share real-time data among hundreds of computers and nearly a thousand individual programs.

SMA-X consists of a set of server-side [LUA](https://lua.org/) scripts that run on [Redis](https://redis.io) (or one 
of its forks / clones such as [Valkey](https://valkey.io) or [Dragonfly](https://dragonfly.io)); a set of libraries to 
interface client applications; and a set of command-line tools built with them. Currently we provide client libraries 
for Python 3 and C/C++ (C99). This repository contains the Python 3 client libraries for SMA-X.

There are no official releases of __smax-python__ yet. An initial stable release is expected early/mid 2025. 
Before then the API may undergo slight changes and tweaks. Use the repository as is at your own risk for now.

### Related links

 - [SMA-X specification](https://docs.google.com/document/d/1eYbWDClKkV7JnJxv4MxuNBNV47dFXuUWu7C4Ve_YTf0/edit?usp=sharing)
 - [Smithsonian/smax-clib](https://github.com/Smithsonian/smax-clib) an alternative library for C/C++ (C99).
 - [Smithsonian/smax-postgres](https://github.com/Smithsonian/smax-postgres) for creating a time-series history of 
   SMA-X in a __PostgreSQL__ database.


<a name="introduction"></a>
## Installing
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
  1. Clone [Smithsonian/smax-server](https://github.com/Smithsonian/smax-server)
  2. In `smax-server`: edit `redis.conf` files as necessary or desired for your application.
  3. In `smax-server`: run `sudo install.sh`.
* Clone this repo ([Smithsonian/smax-python](https://github.com/Smithsonian/smax-python)) locally

That's it, you now have SMAX running locally.


<a name="examples"></a>
## Examples

The best place to find example usages is the unit tests (`test_smax_redis_client.py`), but here are 
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
### Pub/sub

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

Note that `smax_client.smax_pull()` returns `Smax<type>` variables, with the numerical types being derived from `numpy` dtype variables such as `numpy.int32` and `numpy.float64`.  `smax_client.smax_share()` with builtin types will default to converting builtin ints and float to SMA-X types `int32` and `float64`.  Thus a round trip through SMA-X through `smax_client.smax_share()` and `smax_client.smax_pull()` will convert built-in Python  `int` and `float` types to `SmaxInt32` and `SmaxFloat64`, derived from `numpy.int32` and `np.float64` respectively.  When 

Performing arithmetic operations on `Smax<type>` variables will result in the `numpy.<type>` that they are derived from, dropping the metadata (which is now invalid). This includes binary operations with builtin types.

This may cause issues if you try to use type testing to confirm that an argument is of the right type, or use constructs such as `if <SmaxInt32> in range(0,5):`.  We recommend either ducktyping instead, or explicit casts to your preferred type. For example:

```
a = smax_client.smax_pull('example:table', 'int8key')

# cast to int before checking range
if int(a) in range(0,5):
  <do something>
```

------------------------------------------------------------------------------
Copyright (C) 2024 Center for Astrophysics \| Harvard \& Smithsonian

