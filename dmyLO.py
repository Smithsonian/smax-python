from time import sleep
from redisPubSub import *

# chan should be generated from the host name
chan = "loa4"
rps = RedisPubSubGet(channel = chan)

# There will be a lot of setting up the LO here

while True:
#  print(rps.getMessage())
  (origin, cmd, args) = rps.getMessage().split(';')
  print("received request from %s cmd=%s args=%s" % (origin, cmd, args))
  sleep(1)
  reply = "%s;%s;%s" % (origin, "ACK", cmd)
  print("Sending reply:", reply)
  rps.respond(origin, reply)
