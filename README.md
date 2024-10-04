[![pytest](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/python_tests.yml/badge.svg)](https://github.com/Smithsonian/sma-python-redis-client/actions/workflows/python_tests.yml)

<picture>
  <source srcset="resources/CfA-logo-dark.png" alt="CfA logo" media="(prefers-color-scheme: dark)"/>
  <source srcset="resources/CfA-logo.png" alt="CfA logo" media="(prefers-color-scheme: light)"/>
  <img src="resources/CfA-logo.png" alt="CfA logo" width="400" height="67" align="right"/>
</picture>
<br clear="all">

# SMAX for Python 

Client side python library for the [SMA Exchange (SMA-X)](https://docs.google.com/document/d/1eYbWDClKkV7JnJxv4MxuNBNV47dFXuUWu7C4Ve_YTf0/edit?usp=sharing) realtime structured database.

Version 1.0.3

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

There are no official releases of __smax-python__ yet. An initial 1.0.0 release is expected early/mid 2025. 
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

