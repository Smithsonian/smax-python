import sys
import os
import time
import numpy as np
from redis import StrictRedis
from threading import Thread, Semaphore

default_server = "127.0.0.1"

class RedisInterface:
  def __init__(self, server=default_server):
    self.db = StrictRedis(host=server, port=6379, db=0)
    try:
      self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta');
    except:
      print("getting scripts failed", file = sys.stderr)
      self.setSHA = ''
    self.hostName = os.uname()[1]
    self.table = ""
    self.sem = Semaphore(0)
    self.dataName = ''
    self.data=()
    self.Thread = Thread(target=self.sendToRedis)
    self.Thread.start()

  def sendToRedis(self):
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
        st = np.array2string(self.data, precision=1,\
           max_line_width=5000)[1:-1]
        size = len(self.data)
        try:
          self.db.evalsha(self.setSHA, '1', self.table, self.hostName, \
              self.dataName, st, self.dataType, size)
          notifyWait = 0
        except Exception as inst:
          print(type(inst))    # the exception instance
          print(inst.args)     # arguments stored in .args
          print(inst)          # __str__ allows args to be printed directly,
                               # but may be overridden in exception subclasses
#        except:
          if notifyWait >= 0:
            sys.stderr.write("Sending data to Redis failed\n")
            sys.stderr.flush()
            notifyWait = -180
          else:
            notifyWait += 1


  def send(self, table, dataName, data, dataType='float32'):
    self.table = table
    self.dataName = dataName
    self.data = data
    self.dataType = dataType
    self.sem.release()
