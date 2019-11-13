import os
import sys
import numpy as np
from redis import StrictRedis

class SendToRedis:
  def __init__(self, server="128.171.116.189", progName = ""):
    self.db = StrictRedis(host=server, port=6379, db=0)
    try:
      self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta');
    except:
      print("getting scripts failed", file = sys.stderr)
      self.setSHA = None
    self.hostName = os.uname()[1]
    if len(progName) > 0:
      self.hostName += ':'+progName
    self.notifyWait = 0

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
      st = str(d)
      size = 1
    elif t == str:
      st = d
      size = len(st)
      dataType = 'str'
    else:
      print("I don't know how to send "+str(t)+"to redis", file = sys.stderr)
      return("", "", 0)
    return(st, dataType, size)

  def send(self, data, dataName = 'array', key = "_RWW_Test_", \
        precision = None, suppress_small = None):

    if self.setSHA == None:
      if self.notifyWait >= 0:
        try:
          self.setSHA = self.db.hget('persistent:scripts', 'HSetWithMeta')
        except:
          self.notifyWait = -90
          sys.stderr.write("Unable to load redis macros\n")
          sys.stderr.flush()
        else:
          self.notifyWait += 1

    (st, dataType, size) = self.toString(data, precision = precision, \
          suppress_small = suppress_small)
    if size == 0:
      return(False)
    try:
      self.db.evalsha(self.setSHA, '1', key, self.hostName, dataName, \
           st, dataType, size)
      self.notifyWait = 0
    except Exception as inst:
      if self.notifyWait >= 0:
        sys.stderr.write("Sending data to Redis failed\n")
        print(type(inst), file = sys.stderr)    # the exception instance
        print(inst.args, file = sys.stderr)     # arguments stored in .args
        print(inst, file = sys.stderr)          # __str__ allows args to be
          # printed directly, but may be overridden in exception subclasses
        sys.stderr.flush()
        self.notifyWait = -90
      else:
        self.notifyWait += 1
