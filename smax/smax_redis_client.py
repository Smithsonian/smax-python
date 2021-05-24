import os
import sys
from time import time

import numpy as np
from redis import StrictRedis, ConnectionError, TimeoutError

from .smax_client import SmaxClient, SmaxData


class SmaxRedisClient(SmaxClient):

    def __init__(self, redis_ip="128.171.116.189", redis_port=6379, redis_db=0,
                 program_name=None, hostname=None):
        self.redis_ip = redis_ip
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.getSHA = None
        self.setSHA = None
        self.notify_wait = 0
        self.notify_time = time()

        # Obtain hostname automatically, unless 'hostname' argument is used.
        self.hostname = os.uname()[1] if hostname is None else hostname

        # Optionally add a program name into the hostname.
        if program_name is not None:
            self.hostname += ':' + program_name

        # Call parent constructor, which calls smax_connect_to().
        super().__init__(redis_ip, redis_port, redis_db)

    def smax_connect_to(self, redis_ip, redis_port, redis_db):
        try:
            # Connect to redis-server, and store LUA scripts on the object.
            redis_db = StrictRedis(host=redis_ip, port=redis_port, db=redis_db)
            self.getSHA = redis_db.hget('scripts', 'HGetWithMeta')
            self.setSHA = redis_db.hget('scripts', 'HSetWithMeta')
            return redis_db
        except (ConnectionError, TimeoutError):
            print("Connecting to redis and getting scripts failed",
                  file=sys.stderr, flush=True)
            raise

    def smax_disconnect(self):
        # Python manages a connection pool automatically, if somehow that fails
        # release the connection, this disconnect function will do it.
        if self.client.connection:
            self.client.connection.disconnect()

    def smax_pull(self, table, key):
        """
        Get data which was stored with the smax macro HSetWithMeta along with
        the associated metadata. The return will be the data, typeName,
        dataDimension(s), dataDate, source of the data, and a sequence number.
        :param table: SMAX table name
        :param key: SMAX key name
        :return: SmaxData object: (data, type, dimension(s), date, source, sequence)
        """
        try:
            lua_data = self.client.evalsha(self.getSHA, '1', table, key)

        except Exception as inst:
            print("Reading %s from Redis failed" % key, file=sys.stderr)
            print(type(inst), file=sys.stderr)  # the exception instance
            print(inst.args, file=sys.stderr)  # arguments stored in .args
            print(inst, file=sys.stderr)  # __str__ allows args to be
            # printed directly, but may be overridden in exception subclasses
            sys.stderr.flush()
            return SmaxData(None, None, None, None, None, None)
        if lua_data[0] is None:
            return SmaxData(None, None, None, None, None, None)
        else:
            return _decode(lua_data)

    def smax_share(self, table, key, value, precision=None, suppress_small=None):
        """
        Send data to redis using the smax macro HSetWithMeta to include
        metadata.  The metadata is typeName, dataDimension(s), dataDate,
        source of the data, and a sequence number.  The first two are
        determined from the data and the source from this computer's name
        plus the program name if given when this class is instantiated.
        Date and sequence number are added by the redis macro.
        """

        if self.setSHA is None:
            if time() - self.notify_time >= 600:
                try:
                    self.setSHA = self.client.hget('scripts', 'HSetWithMeta')
                except (ConnectionError, TimeoutError):
                    self.notify_time = time()
                    sys.stderr.write("Unable to connect and load redis macros\n")
                    sys.stderr.flush()
                    return False
            else:
                return False

        data_string, data_type, size = _to_string(value,
                                                  precision=precision,
                                                  suppress_small=suppress_small)
        if size == 0:
            return False
        try:
            self.client.evalsha(self.setSHA, '1', table, self.hostname, key,
                                data_string, data_type, size)
        except Exception as inst:
            if time() - self.notify_time >= 600:
                sys.stderr.write("Sending data to Redis failed\n")
                print(type(inst), file=sys.stderr)  # the exception instance
                print(inst.args, file=sys.stderr)  # arguments stored in .args
                print(inst, file=sys.stderr)  # __str__ allows args to be
                # printed directly, but may be overridden in exception subclasses
                sys.stderr.flush()
                self.notify_time = time()

    def smax_lazy_pull(self, table, key, value):
        pass

    def smax_lazy_end(self, table, key):
        pass

    def smax_subscribe(self, pattern, key):
        pass

    def smax_unsubscribe(self, pattern, key):
        pass

    def smax_wait_on_subscribed(self, table, key):
        pass

    def smax_wait_on_subscribed_group(self, match_table, changed_key):
        pass

    def smax_wait_on_subscribed_var(self, matchKey, changed_table):
        pass

    def smax_wait_on_any_subscribed(self, changed_table, changed_key):
        pass

    def smax_release_waits(self, pattern, key):
        pass

    def smax_queue(self, table, key, value, meta):
        pass

    def smax_queue_callback(self, function, *args):
        pass

    def smax_create_sync_point(self):
        pass

    def smax_sync(self, sync_point, timeout_millis):
        pass

    def smax_wait_queue_complete(self, timeout_millis):
        pass

    def smax_set_description(self, table, key, description):
        pass

    def smax_get_description(self, table, key):
        pass

    def smax_set_units(self, table, key, unit):
        pass

    def smax_get_units(self, table, key, unit):
        pass

    def smax_set_coordinate_system(self, table, key, coordinate_system):
        pass

    def smax_get_coordinate_system(self, table, key):
        pass

    def smax_create_coordinate_system(self, n_axis):
        pass

    def smax_push_meta(self, meta, table, key, value):
        pass

    def smax_pull_meta(self, table, key):
        pass

    def smax_set_resilient(self, value):
        pass

    def smax_is_resilient(self):
        pass


# "Static" internal functions for smax.
def _decode(data_plus_meta):
    type_name = data_plus_meta[1].decode("utf-8")
    if type_name in _TYPE_MAP:
        data_type = _TYPE_MAP[type_name]
    else:
        print("I can't deal with data of type ", type_name, file=sys.stderr)
        return SmaxData(None, None, None, None, None, None)

    data_dim = tuple(int(s) for s in data_plus_meta[2].decode("utf-8").split())
    if len(data_dim) == 1:
        data_dim = data_dim[0]
    data_date = float(data_plus_meta[3])
    source = data_plus_meta[4].decode("utf-8")
    sequence = int(data_plus_meta[5])
    if data_type == str:
        return SmaxData(data_plus_meta[0], data_type, data_dim, data_date, source, sequence)
    elif data_dim == 1:  # single datum
        if data_type == str:
            d = data_plus_meta[0].decode("utf-8")
        else:
            d = data_type(data_plus_meta[0])
        return SmaxData(d, type_name, data_dim, data_date, source, sequence)
    else:  # this is some kind of array
        d = tuple(float(s) for s in data_plus_meta[0].decode("utf-8").split())
        d = np.array(d, dtype=data_type)
        if type(data_dim) == tuple:  # n-d array
            d = d.reshape(data_dim)

    return SmaxData(d, type_name, data_dim, data_date, source, sequence)


def _to_string(data, precision=None, suppress_small=None):

    t = type(data)
    if t in _REVERSE_TYPE_MAP:
        data_type = _REVERSE_TYPE_MAP[t]
    else:
        data_type = "UNK"
    if t == list or t == tuple:
        d = np.array(data)
        t = np.ndarray
    else:
        d = data
    if t == np.ndarray:
        data_type = d.dtype.name
        s = d.shape
        if len(s) == 1:
            size = s[0]
        else:
            d = d.flatten()
            size = " ".join(str(i) for i in s)
        st = np.array_str(d, precision=precision, suppress_small=suppress_small,
                          max_line_width=5000)[1:-1]
    elif data_type[0] != 'U':
        if precision is None:
            st = str(d)
        else:
            st = str(round(d, precision))
        size = 1
    elif t == str:
        st = d
        size = len(st)
        data_type = 'str'
    else:
        print("I don't know how to send " + str(t) + "to redis", file=sys.stderr)
        return "", "", 0
    return st, data_type, size


_TYPE_MAP = {'integer': int,
             'int16': np.int16,
             'int32': np.int32,
             'int64': np.int64,
             'int8': np.int8,
             'float': float,
             'float32': np.float32,
             'float64': np.float64,
             'str': str}
_REVERSE_TYPE_MAP = inv_map = {v: k for k, v in _TYPE_MAP.items()}
