#! /usr/bin/python3
from time import sleep
import readline
import os
from redisPubSub import *

# chan should be generated from the host name
chan = os.uname()[1].split(".")[0]
#chan = "loa0"
hostname = os.uname()[1]
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
