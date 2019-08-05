import sys
import os
import time
from redis import StrictRedis
from threading import Thread, Semaphore

class RedisInterface:
  def __init__(self, server="128.171.116.189"):
    self.db = StrictRedis(host=server, port=6379, db=0)
    try:
      self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta');
    except:
      self.setSHA = ''
    self.hostName = os.uname()[1]
    self.table = ""
    self.sem = Semaphore(0)
    self.dataName = ''
    self.data=()
    self.Thread = Thread(target=self.sendToRedis)
    self.Thread.start()

  def sendToRedis(self):
#    print("sendToRedis started")
    wait = 0
    notifyWait = 0
    while True:
      self.sem.acquire()
#      print("Sending %s to redis" % self.dataName)
#      ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
      if self.setSHA  == '':
        if wait >= 0:
          try:
            self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta') 
          except:
            wait = -50
            if notifyWait >= 0:
              sys.stderr.write("Unable to load redis macros\n")
              sys.stderr.flush()
              notifyWait = -10
            else:
              notifyWait += 1
        else:
          wait += 1
      else:
        try:
          self.db.evalsha(self.setSHA, '1', self.table, self.hostName, \
              self.dataName,self.data, 'float32', '1')
#              self.data, 'float32', '1', ts, self.hostName)
          notifyWait = 0
        except:
          if notifyWait >= 0:
            sys.stderr.write("Unable to load redis macros\n")
            sys.stderr.flush()
            notifyWait = -180
          else:
            notifyWait += 1


  def send(self, table, dataName, data):
    self.table = table
    self.dataName = dataName
    self.data = data
    self.sem.release()
