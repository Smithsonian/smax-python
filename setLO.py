from time import sleep
import os
from redisPubSub import *

# chan should be generated from the host name
chan = "loa4"
hostname = os.uname()[1]
rps = RedisPubSubSend(channel = chan, destination = chan)

while True:
  inp = input("Enter desired frequency")
  rps.sendCommand('tune', inp)
  print(rps.getResponse(10))
