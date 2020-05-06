#!/usr/bin/python3 -i

import os
import numpy as np
from getFromRedis import *
hostName = os.uname()[1]
if hostName == 'localhost':
  redisIP = "192.168.0.1"
else:
  redisIP = "128.171.116.189"
key = "ScanningSpectrometers:%c" % hostName[-1]
key1 = "antenna:1:rxA:IF:0:scanspec"

gr = GetFromRedis(server = redisIP)
