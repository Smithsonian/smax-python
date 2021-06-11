import logging
import socket

import numpy as np
from redis import StrictRedis, ConnectionError, TimeoutError
from fnmatch import fnmatch

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
        self._pubsub = None
        self._getstructSHA = None

        # Obtain _hostname automatically, unless '_hostname' argument is passed.
        self._hostname = socket.gethostname() if hostname is None else hostname

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
            redis_client = StrictRedis(host=redis_ip,
                                       port=redis_port,
                                       db=redis_db,
                                       health_check_interval=30)
            self._getSHA = redis_client.hget('scripts', 'HGetWithMeta')
            self._setSHA = redis_client.hget('scripts', 'HSetWithMeta')
            self._getstructSHA = redis_client.hget('scripts', 'GetStruct')
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

    def _parse_lua_pull_response(self, lua_data):

        # Extract the type out of the meta data, and map string to real type object.
        type_name = lua_data[1].decode("utf-8")

        if type_name in _TYPE_MAP:
            data_type = _TYPE_MAP[type_name]
        else:
            raise TypeError(f"I can't deal with data of type {type_name}")

        # Extract data, source and sequence from meta data.
        data_date = float(lua_data[3])
        source = lua_data[4].decode("utf-8")
        sequence = int(lua_data[5])

        # Extract dimension information from meta data.
        data_dim = tuple(int(s) for s in lua_data[2].decode("utf-8").split())

        # If only one dimension convert to a single value (rather than list)
        if len(data_dim) == 1:
            data_dim = data_dim[0]

        # If there is only a single value, cast to the appropriate type and return.
        if data_dim == 1:
            if data_type == str:
                data = lua_data[0].decode("utf-8")
            else:
                data = data_type(lua_data[0])
            return SmaxData(data, data_type, data_dim, data_date, source, sequence)

        # This is some kind of array.
        else:
            data = lua_data[0].decode("utf-8").split(" ")

            # If this is a list of strings, just clean up string and return.
            if data_type == str:
                # Remove the leading and trailing \' in each string in the list.
                data = [s.strip("\'") for s in data]
                return SmaxData(data, data_type, data_dim, data_date, source, sequence)
            else:
                # Use numpy for all other numerical types
                data = np.array(data, dtype=data_type)

            # If this is a multi-dimensional array, reshape with numpy.
            if type(data_dim) == tuple:  # n-d array
                data = data.reshape(data_dim)

            return SmaxData(data, data_type, data_dim, data_date, source, sequence)

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

        # Extract the type out of the meta data, and map string to real type object.
        type_name = lua_data[1].decode("utf-8")

        if type_name == "struct":
            ls = self._client.evalsha(self._getstructSHA, '1', table, key)
            tree = {}

            for i, item in enumerate(ls[0]):
                t = tree
                names = item.decode("utf-8").split(':')
                for j, part in enumerate(names):
                    t = t.setdefault(part, {})
                    if j == len(names) - 1:
                        for k, leaf in enumerate(ls[i + i + 1]):
                            lua_type = ls[i + i + 2][1][k]

                            if lua_type.decode("utf-8") == "struct":
                                continue

                            lua_data = ls[i + i + 2][0][k]
                            lua_dim = ls[i + i + 2][2][k]
                            lua_date = ls[i + i + 2][3][k]
                            lua_hostname = ls[i + i + 2][4][k]
                            lua_sequence = ls[i + i + 2][5][k]
                            smax_data_object = self._parse_lua_pull_response([lua_data, lua_type, lua_dim, lua_date, lua_hostname, lua_sequence])
                            t.setdefault(ls[i + i + 1][k].decode("utf-8"), smax_data_object)
            return tree

        return self._parse_lua_pull_response(lua_data)

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

        # Copy the data into a variable that we will manipulate for smax.
        converted_data = value

        # If type is list or tuple, convert to numpy array for further manipulation.
        if python_type == list or python_type == tuple:
            # Convert to numpy array, dtype="O" preserves the original types.
            converted_data = np.array(value, dtype="O")
            python_type = np.ndarray

        # Now if its a numpy array, flatten, convert to a string, and return.
        if python_type == np.ndarray or python_type == np.array:

            # If the shape is a single dimension, set 'size' equal to that value.
            data_shape = converted_data.shape
            if len(data_shape) == 1:
                size = data_shape[0]
            else:
                # Convert shape tuple to a space delimited list for smax.
                size = " ".join(str(i) for i in data_shape)

                # Flatten and make a space delimited string of dimensions.
                converted_data = converted_data.flatten()

            # Check this 1D representation of the data for type uniformity.
            if not all(isinstance(x, type(converted_data[0])) for x in converted_data):
                raise TypeError("All values in list are not the same type.")

            type_name = _REVERSE_TYPE_MAP[type(converted_data[0])]

            # Create a string representation of the data in the array.
            converted_data = ' '.join(str(x) for x in converted_data)
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

    def smax_subscribe(self, pattern, callback=None):
        """
        Subscribe to a redis field or group of fields. You can type the full
        name of the field you'd like to subscribe too, or use a wildcard "*"
        character to specify a pattern. Use a callback for asynchronous
        processing of notifications, or use one of the smax_wait_on functions.
        :param pattern: Either full name of smax field, or use a wildcard '*' at
        the end of the pattern to be notified for anything underneath.
        :param callback: Optional callback function to process notifications.
        """
        if self._pubsub is None:
            self._pubsub = self._client.pubsub()

        if pattern.endswith("*"):
            if callback is None:
                self._pubsub.psubscribe(f"smax:{pattern}")
            else:
                self._pubsub.psubscribe(**{f"smax:{pattern}": callback})
        else:
            if callback is None:
                self._pubsub.subscribe(f"smax:{pattern}")
            else:
                self._pubsub.subscribe(**{f"smax:{pattern}": callback})

    def smax_unsubscribe(self, pattern=None):
        if self._pubsub is not None:
            if pattern is None:
                self._pubsub.punsubscribe()
                self._pubsub.unsubscribe()
            elif pattern.endswith("*"):
                self._pubsub.punsubscribe(f"smax:{pattern}")
            else:
                self._pubsub.unsubscribe(f"smax:{pattern}")

    def _redis_listen(self, pattern=None, timeout=None, notification_only=False):
        # Throw away any blank messages or of type 'subscribe'
        found_real_message = False
        message = None
        channel = None

        while not found_real_message:
            if timeout is None:
                for message in self._pubsub.listen():
                    break
            else:
                message = self._pubsub.get_message(timeout=timeout)

            if message is None:
                raise TimeoutError("Timed out waiting for redis message.")
            elif message["type"] == "message" or message["type"] == "pmessage":
                channel = message["channel"].decode("utf-8")
                if channel.startswith("smax:"):
                    if pattern is None:
                        found_real_message = True
                    elif fnmatch(channel[5:], pattern):
                        found_real_message = True

        if notification_only:
            return message
        else:
            table = channel[5:channel.rfind(":")]
            key = channel.split(":")[-1]
            return self.smax_pull(table, key)

    def smax_wait_on_subscribed(self, pattern, timeout=None,
                                notification_only=False):
        return self._redis_listen(pattern=pattern, timeout=timeout,
                                  notification_only=notification_only)

    def smax_wait_on_any_subscribed(self, timeout=None, notification_only=False):
        return self._redis_listen(timeout=timeout,
                                  notification_only=notification_only)

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
