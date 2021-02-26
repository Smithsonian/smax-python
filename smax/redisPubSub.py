"""
Python routines for sending or receiving commands or messages
using redisPubSub
"""
import sys
from redis import StrictRedis

class RedisPubSubGet:
  def __init__(self, channel = 'test1', server="128.171.116.189"):
    self.redis = StrictRedis(host=server, port = 6379)
    self.channel = channel
    self.ps = self.redis.pubsub()
    try:
      self.ps.subscribe(channel)
      self.connected = True
    except:
      print("Subscribe to ", channel, "failed", file = sys.stderr, flush = True)
      self.connected = False

  def __enter__(self):
        print('__enter__ called')
        return self
    
  def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.connected == True:
          self.ps.unsubscribe()
          self.connected = False
          print("Unsubscribed to %s" % (self.channel))
        print('__exit__ called')
        if exc_type:
            print(f'exc_type: {exc_type}')
            print(f'exc_value: {exc_value}')
            print(f'exc_traceback: {exc_traceback}')
    
  def getMessage(self, timeout = 1000000.):
    while True:
      m = self.ps.get_message(timeout=timeout)
      if m == None:
        return("")
      if type(m) == dict and m['type'] == 'message':
        return(m['data'].decode("utf-8"))

  def respond(self, origin, message):
    self.redis.publish(self.channel+'->'+origin, message)
#Server in Cambridge 192.168.0.1
#Server in the SMA 128.171.116.189
class RedisPubSubSend:
  def __init__(self, channel, destination, server="128.171.116.189"):
    self.redis = StrictRedis(host=server, port = 6379)
    self.channel = channel
    self.dest = destination
    self.ps = self.redis.pubsub()
    self.ps.subscribe(channel+'->'+self.dest)
    try:
      self.ps.subscribe(channel+'->'+self.dest)
      self.connected = True
    except:
      print("Subscribe to ", channel, "failed", file = sys.stderr, flush = True)
      self.connected = False

  def __enter__(self):
        print('__enter__ called')
        return self
    
  def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.connected == True:
          self.ps.unsubscribe()
          self.connected = False
          print("Unsubscribed to %s" % (self.channel))
        print('__exit__ called')
        if exc_type:
            print(f'exc_type: {exc_type}')
            print(f'exc_value: {exc_value}')
            print(f'exc_traceback: {exc_traceback}')
    
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
