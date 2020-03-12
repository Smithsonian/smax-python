#!/usr/bin/python3 -i

import numpy as np
from sendToRedis import *
key = "ScanningSpectrometers:t"

sr = SendToRedis("192.168.0.1")
