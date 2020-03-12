#!/usr/bin/python3 -i

import numpy as np
from getFromRedis import *
key = "ScanningSpectrometers:t"


gr = GetFromRedis(server = '192.168.0.1')
