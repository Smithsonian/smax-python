#!/usr/bin/python3 -i

from smax.sendToRedis import *
key = "example_smax_daemon:logging_action:random_base"

sr = SendToRedis("127.0.0.1")
