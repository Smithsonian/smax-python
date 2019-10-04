from redis import StrictRedis

class RedisPubSub:
  def __init__(self, channel = 'test1', server="128.171.116.189"):
    self.redis = StrictRedis(host=server, port = 6379)
    self.channel = channel
    self.ps = self.redis.pubsub()
    self.ps.subscribe(channel)
    
  def getMessage(self, timeout = 1000.):
    while True:
      m = self.ps.get_message(timeout=timeout)
      if type(m) == dict and m['type'] == 'message':
        return(m['data'].decode("utf-8"))

  def respond(self, origin, message):
    self.redis.publish(self.channel+'->'+origin, message)
