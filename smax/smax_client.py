import os
import sys
from time import time
import numpy as np
from redis import StrictRedis, ConnectionError, TimeoutError


class SmaxClient:
    """
    Combined class for both sending and getting data from redis.
    """
    def __init__(self, host="128.171.116.189", port=6379, db=0, prog_name=""):
        self.db = StrictRedis(host=host, port=port, db=db)
        try:
            self.getSHA = self.db.hget('scripts', 'HGetWithMeta')
            self.setSHA = self.db.hget('scripts', 'HSetWithMeta')
        except (ConnectionError, TimeoutError):
            print("Connecting to redis and getting scripts failed",
                  file=sys.stderr, flush=True)
            self.getSHA = None
            self.setSHA = None
        self.hostName = os.uname()[1]

        if len(prog_name) > 0:
            self.hostName += ':' + prog_name

        self.notifyWait = 0
        self.notifyTime = time()

    def pull(self, key, data_name):
        """
        Get data which was stored with the smax macro HSetWithMeta along
        with the associated metadata.
        The return will be the data, typeName, dataDimension(s), dataDate,
        source of the data, and a sequence number.
        """
        try:
            dataPlusMeta = self.db.evalsha(self.getSHA, '1', key, data_name)
        except Exception as inst:
            print("Reading %s from Redis failed" % data_name, file=sys.stderr)
            print(type(inst), file=sys.stderr)  # the exception instance
            print(inst.args, file=sys.stderr)  # arguments stored in .args
            print(inst, file=sys.stderr)  # __str__ allows args to be
            # printed directly, but may be overridden in exception subclasses
            sys.stderr.flush()
            return None, None, None, None, None, None
        if dataPlusMeta[0] is None:
            return None, None, None, None, None, None
        else:
            return _decode(dataPlusMeta)

    def pull_no_meta(self, key, name):
        """
        This will get data without any metadata whether that datum was
        stored with the smax macro or not.
        """
        return self.db.hget(key, name).decode("utf-8")

    def share(self, key, data_name, data, precision=None, suppress_small=None):
        """
        Send data to redis using the smax macro HSetWithMeta to include
        metadata.  The metadata is typeName, dataDimension(s), dataDate,
        source of the data, and a sequence number.  The first two are
        determined from the data and the source from this computer's name
        plus the program name if given when this class is instantiated.
        Date and sequence number are added by the redis macro.
        """

        if self.setSHA is None:
            if time() - self.notifyTime >= 600:
                try:
                    self.setSHA = self.db.hget('scripts', 'HSetWithMeta')
                except (ConnectionError, TimeoutError):
                    self.notifyTime = time()
                    sys.stderr.write("Unable to connect and load redis macros\n")
                    sys.stderr.flush()
                    return False
            else:
                return False

        st, dataType, size = _to_string(data,
                                       precision=precision,
                                       suppress_small=suppress_small)
        if size == 0:
            return False
        try:
            self.db.evalsha(self.setSHA, '1', key, self.hostName, data_name,
                            st, dataType, size)
        except Exception as inst:
            if time() - self.notifyTime >= 600:
                sys.stderr.write("Sending data to Redis failed\n")
                print(type(inst), file=sys.stderr)  # the exception instance
                print(inst.args, file=sys.stderr)  # arguments stored in .args
                print(inst, file=sys.stderr)  # __str__ allows args to be
                # printed directly, but may be overridden in exception subclasses
                sys.stderr.flush()
                self.notifyTime = time()

    def share_no_meta(self, key, name, value):
        """
        Simply send data to redis without any metadata.
        """
        if self.setSHA is None:
            return False
        try:
            self.db.hset(key, name, value)
        except (ConnectionError, TimeoutError):
            self.setSHA = None

    def share_from_dict(self, key, data_dict):
        """
        Send a sequence of data to redis based on a dictionary containing
        data names indexing data values.  All go under the same key.
        """
        for k in data_dict.keys():
            if self.setSHA is None:
                return False
            self.share_no_meta(key, k, data_dict[k])


# "Static" internal functions for smax.
def _decode(data_plus_meta):

    nameTypes = {'integer': int,
                 'int16': np.int16,
                 'int32': np.int32,
                 'int64': np.int64,
                 'int8': np.int8,
                 'float': float,
                 'float32': np.float32,
                 'float64': np.float64,
                 'str': str}

    typeName = data_plus_meta[1].decode("utf-8")
    if typeName in nameTypes:
        dataType = nameTypes[typeName]
    else:
        print("I can't deal with data of type ", typeName, file=sys.stderr)
        return None, None, None, None, None, None

    dataDim = tuple(int(s) for s in data_plus_meta[2].decode("utf-8").split())
    if len(dataDim) == 1:
        dataDim = dataDim[0]
    dataDate = float(data_plus_meta[3])
    source = data_plus_meta[4].decode("utf-8")
    sequence = int(data_plus_meta[5])
    if dataType == str:
        return data_plus_meta[0], dataType, dataDim, dataDate, source, sequence
    elif dataDim == 1:  # single datum
        if dataType == str:
            d = data_plus_meta[0].decode("utf-8")
        else:
            d = dataType(data_plus_meta[0])
        return d, typeName, dataDim, dataDate, source, sequence
    else:  # this is some kind of array
        d = tuple(float(s) for s in data_plus_meta[0].decode("utf-8").split())
        d = np.array(d, dtype=dataType)
        if type(dataDim) == tuple:  # n-d array
            d = d.reshape(dataDim)
    return d, typeName, dataDim, dataDate, source, sequence


def _to_string(data, precision=None, suppress_small=None):
    typeNames = {int: 'integer',
                 np.int16: 'int16',
                 np.int32: 'int32',
                 np.int64: 'int64',
                 np.int8: 'int8',
                 float: 'float',
                 np.float32: 'float32',
                 np.float64: 'float64'}

    t = type(data)
    if t in typeNames:
        dataType = typeNames[t]
    else:
        dataType = "UNK"
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
        st = np.array_str(d, precision=precision,
                          suppress_small=suppress_small,
                          max_line_width=5000)[1:-1]
    elif dataType[0] != 'U':
        if precision is None:
            st = str(d)
        else:
            st = str(round(d, precision))
        size = 1
    elif t == str:
        st = d
        size = len(st)
        dataType = 'str'
    else:
        print("I don't know how to send " + str(t) + "to redis",
              file=sys.stderr)
        return "", "", 0
    return st, dataType, size
