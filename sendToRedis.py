import os
import sys
import time
import numpy as np
from redis import StrictRedis

class SendToRedis:
  def __init__(self, server="128.171.116.189", progName = ""):
    self.db = StrictRedis(host=server, port=6379, db=0)
    try:
      self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta', timeout = 5);
    except:
      print("Connecting to redis and getting scripts failed", \
          file = sys.stderr, flush = True)
      self.setSHA = None
    self.hostName = os.uname()[1]
    if len(progName) > 0:
      self.hostName += ':'+progName
    self.notifyTime = 0

  def toString(self, data, precision = None, suppress_small = None):

    typeNames = { int:'integer', np.int16:'int16',np.int32:'int32', \
        np.int64:'int64', np.int8: 'int8', \
        float:'float', np.float32:'float32', np.float64:'float64'}
  
    t = type(data)
    if t in typeNames:
      dataType = typeNames[t]
    else:
      dataType = "UNK"
#    print("dataType = ", dataType, "t = ", t)
    if t == list or t == tuple:
      d = np.array(data)
      t = np.ndarray
    else:
      d = data
    if t == np.ndarray:
      dataType = d.dtype.name
      s = d.shape
      if len(s) == 1:
        size = s[0]
      else:
        d = d.flatten()
        size = " ".join(str(i) for i in s)
      st = np.array_str(d, precision=precision, suppress_small = suppress_small, \
          max_line_width=5000)[1:-1]
    elif dataType[0] != 'U':
      if type(precision) ==type(None):
        st = str(d)
      else:
        st = str(round(d, precision))
      size = 1
    elif t == str:
      st = d
      size = len(st)
      dataType = 'str'
    else:
      print("I don't know how to send "+str(t)+"to redis", file = sys.stderr)
      return("", "", 0)
    return(st, dataType, size)

  def send(self, key, dataName, data, \
        precision = None, suppress_small = None):

    if self.setSHA == None:
      if time.time() - self.notifyTime >= 600:
        try:
          self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta', \
              timeout = 5)
        except:
          self.notifyTime = time.time()
          sys.stderr.write("Unable to connect and load redis macros\n")
          sys.stderr.flush()
          return(False)
      else:
          return(False)

    (st, dataType, size) = self.toString(data, precision = precision, \
          suppress_small = suppress_small)
    if size == 0:
      return(False)
    try:
      self.db.evalsha(self.setSHA, '1', key, self.hostName, dataName, \
           st, dataType, size)
      self.notifyTime = 0
    except Exception as inst:
      if time.time() - self.notifyTime >= 600:
        sys.stderr.write("Sending data to Redis failed\n")
        print(type(inst), file = sys.stderr)    # the exception instance
        print(inst.args, file = sys.stderr)     # arguments stored in .args
        print(inst, file = sys.stderr)          # __str__ allows args to be
          # printed directly, but may be overridden in exception subclasses
        sys.stderr.flush()
        self.notifyTime = time.time()

# Set name under key to value where all are strings
  def setHash(self, key, name, value):
    if self.setSHA == None:
      return(False)
    self.db.hset(key, name, value)

  def setHashesFromDict(self, key, dict):
    if self.setSHA == None:
      return(False)
    for k in dict.keys():
      self.db.hset(key, k, dict[k])
    
