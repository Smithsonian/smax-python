"""
Python routines for retreiving data from redis with or without smax
metadata.
"""
import os
import sys
import numpy as np
from redis import StrictRedis

class GetFromRedis:
  def __init__(self, server="localhost"):
    self.db = StrictRedis(host=server, port=6379, db=0)
    try:
      self.getSHA = self.db.hget('scripts', 'HGetWithMeta')
    except:
      print("getting scripts failed", file = sys.stderr)
      self.getSHA = None
    self.hostName = os.uname()[1]
    self.notifyWait = 0

  def decode(self, dataPlusMeta):

    nameTypes = {'integer': int, 'int16': np.int16, 'int32': np.int32,\
       'int64': np.int64, 'int8': np.int8, \
       'float': float, 'float32': np.float32, 'float64': np.float64, \
       'str':str}
  
    typeName = dataPlusMeta[1].decode("utf-8")
    if typeName in nameTypes:
      dataType = nameTypes[typeName]
    else:
      print("I can't deal with data of type ", typeName, file = sys.stderr)
      return(None,None,None,None,None,None)
#    print("typeNmae = ", typeName, "dataType = ", dataType)
    dataDim = tuple(int(s) for s in dataPlusMeta[2].decode("utf-8").split())
    if len(dataDim) == 1:
      dataDim = dataDim[0]
    dataDate = float(dataPlusMeta[3])
    source = dataPlusMeta[4].decode("utf-8")
    sequence = int(dataPlusMeta[5])
    if dataType == str:
      return((dataPlusMeta[0], dataType, dataDim, dataDate, source, sequence))
    elif dataDim == 1:            #single datum
      if dataType == str:
        d = dataPlusMeta[0].decode("utf-8")
      else:
        d = dataType(dataPlusMeta[0])
      return((d, typeName, dataDim, dataDate, source, sequence))
    else:                         #this is some kind of array
      d = tuple(float(s) for s in dataPlusMeta[0].decode("utf-8").split())
      d = np.array(d, dtype = dataType)
      if type(dataDim) == tuple:  #n-d array
        d = d.reshape(dataDim)
    return((d, typeName, dataDim, dataDate, source, sequence))
  
  def get(self, key, dataName):
    """
    Get data which was stored with the smax macro HSetWithMeta along
    with the associated metadata.
    The return will be the data, typeName, dataDimension(s), dataDate,
    source of the data, and a sequence number.
    """
    try:
      dataPlusMeta = self.db.evalsha(self.getSHA, '1',key, dataName)
    except Exception as inst:
      print("Reading %s from Redis failed" % dataName, file = sys.stderr)
      print(type(inst), file = sys.stderr)    # the exception instance
      print(inst.args, file = sys.stderr)     # arguments stored in .args
      print(inst, file = sys.stderr)          # __str__ allows args to be
        # printed directly, but may be overridden in exception subclasses
      sys.stderr.flush()
      return(None,None,None,None,None,None)
    if dataPlusMeta[0] == None:
      return(None,None,None,None,None,None)
    else:
      return(self.decode(dataPlusMeta))

  def simpleGet(self, key, name):
    """
    This will get data without any metadata whether that datum was
    stored with the smax macro or not.
    """
    return(self.db.hget(key, name).decode("utf-8"))
