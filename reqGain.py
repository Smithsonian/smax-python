#! /usr/bin/python3
from time import sleep
import readline
import os
from redisPubSub import *

# chan should be generated from the host name
chan = "scanspec9:rxA:IF:0"
rps = RedisPubSubSend(channel = chan, destination = chan)

while True:
  inp = input("Enter command arg1,arg2.. >").split()
  if len(inp) > 1:
    args = inp[1]
  else:
    args = ""
  if inp[0][0] == 'q':
    exit()
  rps.sendCommand(inp[0], args)
  print(rps.getResponse(10))
