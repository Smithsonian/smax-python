import logging
import os

import numpy as np
from redis import StrictRedis, ConnectionError, TimeoutError

from .smax_client import SmaxClient, SmaxData


class SmaxRedisClient(SmaxClient):

    def __init__(self, redis_ip="128.171.116.189", redis_port=6379, redis_db=0,
                 program_name=None, hostname=None):
        """
        Constructor for SmaxRedisClient, automatically establishes connection
        and sets the redis-py connection object to 'self._client'. This magic
        happens in the SmaxClient parent class that is inherited.
        :param str redis_ip: IP address of redis-server.
        :param str redis_port: Port of redis-server.
        :param int redis_db: Database index to connect to.
        :param str program_name: Optional program name gets appended to hostname.
        :param str hostname: Optional hostname, obtained automatically otherwise.
        """

        # Logging convention for messages to have module names in them.
        self.logger = logging.getLogger(__name__)

        # Attributes for package, not exposed to users.
        self._redis_ip = redis_ip
        self._redis_port = redis_port
        self._redis_db = redis_db
        self._getSHA = None
        self._setSHA = None

        # Obtain _hostname automatically, unless '_hostname' argument is passed.
        self._hostname = os.uname()[1] if hostname is None else hostname

        # Optionally add a program name into the _hostname.
        if program_name is not None:
            self._hostname += ':' + program_name

        # Call parent constructor, which calls smax_connect_to().
        super().__init__(redis_ip, redis_port, redis_db)

    def smax_connect_to(self, redis_ip, redis_port, redis_db):
        """
        Uses the redis-py library to establish a connection to redis, then
        obtains and stores the LUA scripts in the object (ex self._getSHA). This
        function is called automatically by the SmaxClient parent class, so
        there shouldn't be a need to call this explicitly.
        :param str redis_ip: IP address of redis-server.
        :param str redis_port: Port of redis-server.
        :param int redis_db: Database index to connect to.
        :return: Redis connection object
        :rtype: Redis
        """
        try:
            # Connect to redis-server, and store LUA scripts on the object.
            redis_client = StrictRedis(host=redis_ip, port=redis_port, db=redis_db)
            self._getSHA = redis_client.hget('scripts', 'HGetWithMeta')
            self._setSHA = redis_client.hget('scripts', 'HSetWithMeta')
            self.logger.info(f"Connected to redis server {redis_ip}:{redis_port} db={redis_db}")
            return redis_client
        except (ConnectionError, TimeoutError):
            self.logger.info("Connecting to redis and getting scripts failed")
            raise

    def smax_disconnect(self):
        """
        Python manages a connection pool automatically, if somehow that fails
        release the connection, this disconnect function will do it.
        """

        if self._client.connection:
            self._client.connection.disconnect()
        self.logger.info(f"Disconnected redis server {self._redis_ip}:{self._redis_port} db={self._redis_db}")

    def smax_pull(self, table, key):
        """
        Get data which was stored with the smax macro HSetWithMeta along with
        the associated metadata. The return will be the data, typeName,
        dataDimension(s), dataDate, source of the data, and a sequence number.
        :param str table: SMAX table name
        :param str key: SMAX key name
        :return: SmaxData tuple data, type, dimension(s), date, source, sequence
        :rtype: SmaxData
        """
        try:
            lua_data = self._client.evalsha(self._getSHA, '1', table, key)
        except (ConnectionError, TimeoutError):
            self.logger.error(f"Reading {table}:{key} from Redis failed")
            raise

        return _decode(lua_data)

    def smax_share(self, table, key, value):
        """
        Send data to redis using the smax macro HSetWithMeta to include
        metadata.  The metadata is typeName, dataDimension(s), dataDate,
        source of the data, and a sequence number.  The first two are
        determined from the data and the source from this computer's name
        plus the program name if given when this class is instantiated.
        Date and sequence number are added by the redis macro.
        :param str table: SMAX table name
        :param str key: SMAX key name
        :param value: data to store in smax, can be any defined type, list, array, or numpy array.
        :return: tuple of (converted string of data, type, size) that was sent to redis.
        """

        # Derive the type according to Python.
        python_type = type(value)

        # Single value of a supported type, cast to string and send to redis.
        if python_type in _REVERSE_TYPE_MAP:
            type_name = _REVERSE_TYPE_MAP[python_type]
            return self._evalsha_set(table, key, str(value), type_name, 1)

        # If python type is list or tuple, first convert to numpy array.
        converted_data = None
        if python_type == list or python_type == tuple:
            converted_data = np.array(value)
            python_type = np.ndarray

        # Now if its a numpy array, flatten, convert to a string, and return.
        if python_type == np.ndarray:
            type_name = converted_data.dtype.name

            # If the shape is a single dimension, set 'size' equal to that value.
            data_shape = converted_data.shape
            if len(data_shape) == 1:
                size = data_shape[0]

            # Flatten and make a space delimited string of dimensions.
            else:
                converted_data = converted_data.flatten()
                size = " ".join(str(i) for i in data_shape)

            # Create a string representation of the data in the array.
            converted_data = np.array_str(converted_data, max_line_width=5000)[1:-1]
            return self._evalsha_set(table, key, converted_data, type_name, size)

        else:
            raise TypeError(f"Unable to convert {python_type} for SMAX")

    def _evalsha_set(self, table, key, data_string, type_name, size):
        """
        Private function that calls evalsha() using an SMAX LUA script.
        :param str table: SMAX table name
        :param str key: SMAX key name
        :param str data_string: Data converted to proper SMAX string format.
        :param str type_name: String representation of type
        :param size: Representation of the dimensions of the data. If one
                     dimension, than a single integer.Otherwise will be a string
                     of space delimited dimension values.
        :return: tuple of (data, type, size) that was sent to redis.
        """
        try:
            self._client.evalsha(self._setSHA, '1', table, self._hostname, key,
                                 data_string, type_name, size)
            return data_string, type_name, size
        except (ConnectionError, TimeoutError):
            self.logger.error("Redis seems down, unable to call the _setSHA LUA script.")
            raise

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

    def smax_wait_on_subscribed_var(self, match_key, changed_table):
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
    # Extract the type out of the meta data, and map string to real type object.
    type_name = data_plus_meta[1].decode("utf-8")
    if type_name in _TYPE_MAP:
        data_type = _TYPE_MAP[type_name]
    else:
        raise TypeError(f"I can't deal with data of type {type_name}")

    # Extract dimension information from meta data.
    data_dim = tuple(int(s) for s in data_plus_meta[2].decode("utf-8").split())

    # If only one dimension convert to a single value (rather than list)
    if len(data_dim) == 1:
        data_dim = data_dim[0]

    # Extract data, source and sequence from meta data.
    data_date = float(data_plus_meta[3])
    source = data_plus_meta[4].decode("utf-8")
    sequence = int(data_plus_meta[5])

    # If there is only a single value, decode and return.
    if data_dim == 1:
        if data_type == str:
            data = data_plus_meta[0].decode("utf-8")
        else:
            data = data_type(data_plus_meta[0])

    # If type data type is a string, return the data as-is. 
    elif data_type == str:
        data = data_type(data_plus_meta[0])

    # This is some kind of array.
    else:
        data = tuple(float(s) for s in data_plus_meta[0].decode("utf-8").split())
        data = np.array(data, dtype=data_type)
        if type(data_dim) == tuple:  # n-d array
            data = data.reshape(data_dim)

    return SmaxData(data, type_name, data_dim, data_date, source, sequence)


# Lookup tables for converting python types to smax type names.
_TYPE_MAP = {'integer': int,
             'int16': np.int16,
             'int32': np.int32,
             'int64': np.int64,
             'int8': np.int8,
             'float': float,
             'float32': np.float32,
             'float64': np.float64,
             'str128': str,
             'str160': str,
             'str': str}
_REVERSE_TYPE_MAP = inv_map = {v: k for k, v in _TYPE_MAP.items()}
