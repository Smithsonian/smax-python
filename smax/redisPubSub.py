"""
Python routines for sending or receiving commands or messages
using redisPubSub
"""
import sys
import atexit
from redis import StrictRedis

class RedisPubSubGet:
  def __init__(self, channel = 'test1', server="localhost"):
    self.redis = StrictRedis(host=server, port = 6379)
    self.channel = channel
    self.ps = self.redis.pubsub()
    try:
      self.ps.subscribe(channel)
      self.connected = True
      atexit.register(self.unsubscribe)
    except:
      print("Subscribe to ", channel, "failed", file = sys.stderr, flush = True)
      self.connected = False
    
  def getMessage(self, timeout = 1000000.):
    while True:
      m = self.ps.get_message(timeout=timeout)
      if m == None:
        return("")
      if type(m) == dict and m['type'] == 'message':
        return(m['data'].decode("utf-8"))

  def respond(self, origin, message):
    self.redis.publish(self.channel+'->'+origin, message)

  def unsubscribe(self):
    self.ps.unsubscribe()
    print("Unsubscribed to ", self.channel)

#Server in Cambridge 192.168.0.1
#Server in the SMA 128.171.116.189
class RedisPubSubSend:
  def __init__(self, channel, destination, server="128.171.116.189"):
    self.redis = StrictRedis(host=server, port = 6379)
    self.channel = channel
    self.dest = destination
    self.ps = self.redis.pubsub()
    try:
      self.ps.subscribe(channel+'->'+self.dest)
      self.connected = True
      atexit.register(self.unsubscribe)
    except:
      print("Subscribe to ", channel, "failed", file = sys.stderr, flush = True)
      self.connected = False
    
  def getResponse(self, timeout = 1000.):
    while True:
      m = self.ps.get_message(timeout=timeout)
      if m == None:
        return("")
      if type(m) == dict and m['type'] == 'message':
        return(m['data'].decode("utf-8"))

  def sendCommand(self, cmd):
    message = self.dest+'\n'+cmd
    self.redis.publish(self.channel, message)

  def unsubscribe(self):
    self.ps.unsubscribe()
    print("Unsubscribed to ", self.channel)
