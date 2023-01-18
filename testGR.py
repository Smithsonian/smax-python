#!/usr/bin/python3 -i

from smax.getFromRedis import *
hostName = os.uname()[1]
#if hostName == 'loa0':
#  redisIP = "192.168.0.1"
#else:
#  redisIP = "128.171.116.189"
redisIP = "127.0.0.1"
key = "antenna:1:rxA:IF:0:scanspec"

gr = GetFromRedis(server = redisIP)
