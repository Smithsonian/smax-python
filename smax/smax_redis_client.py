import logging
import socket
from fnmatch import fnmatch

import numpy as np
from redis import StrictRedis, ConnectionError, TimeoutError

from .smax_client import SmaxClient, SmaxData, SmaxCommand


class SmaxRedisClient(SmaxClient):

    def __init__(self, redis_ip="128.171.116.189", redis_port=6379, redis_db=0,
                 program_name=None, hostname=None):
        """
        Constructor for SmaxRedisClient, automatically establishes connection
        and sets the redis-py connection object to 'self._client'. This magic
        happens in the SmaxClient parent class that is inherited.
        Args:
            redis_ip (str): IP address of redis-server.
            redis_port (int): Port of redis-server.
            redis_db (int): Database index to connect to.
            program_name (str): Optional program name gets appended to hostname.
            hostname (str): Optional hostname, obtained automatically otherwise.
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

        Args:
            redis_ip (str): IP address of redis-server.
            redis_port (int): Port of redis-server.
            redis_db (int): Database index to connect to.

        Returns:
            Redis: A Redis client object configured from the given args.
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

    @staticmethod
    def _parse_lua_pull_response(lua_data):
        """
        Private method to parse the response from calling the HGetWithMeta LUA
        script.
        Args:
            lua_data (list): value, vtype, dim, timestamp, origin, serial

        Returns:
            SmaxData: Populated SmaxData NamedTuple object.
        """

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
        the associated metadata. The return value will an SmaxData object
        containing the data, typeName, dataDimension(s), dataDate, source of the
        data, and a sequence number. If you pulled a struct, you will get a
        nested dictionary back, with each leaf being an SmaxData object.
        Args:
            table (str): SMAX table name
            key (str): SMAX key name

        Returns:
            SmaxData: Populated SmaxData NamedTuple object.
            dict: If a struct is pulled, this returns a nested dictionary.
        """

        try:
            lua_data = self._client.evalsha(self._getSHA, '1', table, key)
        except (ConnectionError, TimeoutError):
            self.logger.error(f"Reading {table}:{key} from Redis failed")
            raise

        # Extract the type out of the meta data, and map string to real type object.
        type_name = lua_data[1].decode("utf-8")

        # If the lua response says its a struct we have to now use another LUA
        # script to go back to redis and collect the struct.
        if type_name == "struct":
            lua_struct = self._client.evalsha(self._getstructSHA, '1', table, key)

            # The struct will be parsed into a nested python dictionary.
            tree = {}
            for struct_name_index, stuct_name in enumerate(lua_struct[0]):
                t = tree
                names = stuct_name.decode("utf-8").split(':')

                for table_name_index, table_name in enumerate(names):

                    # Grow a new hierarchical level with a blank dictionary.
                    t = t.setdefault(table_name, {})

                    # If this is the last name in the path, add actual data.
                    if table_name_index == len(names) - 1:

                        # Create offset indices for more readable code.
                        offset = struct_name_index + struct_name_index + 1
                        offset2 = struct_name_index + struct_name_index + 2

                        # Process leaf node like it is a normal smax_pull.
                        for leaf_index, leaf in enumerate(lua_struct[offset]):

                            # If the leaf says its a struct, ignore it.
                            lua_type = lua_struct[offset2][1][leaf_index]
                            if lua_type.decode("utf-8") == "struct":
                                continue

                            # Extract data and metadata to pass into parser.
                            lua_data = lua_struct[offset2][0][leaf_index]
                            lua_dim = lua_struct[offset2][2][leaf_index]
                            lua_date = lua_struct[offset2][3][leaf_index]
                            lua_hostname = lua_struct[offset2][4][leaf_index]
                            lua_sequence = lua_struct[offset2][5][leaf_index]

                            # Parser will return an SmaxData object.
                            smax_data_object = self._parse_lua_pull_response(
                                [lua_data, lua_type, lua_dim, lua_date,
                                 lua_hostname, lua_sequence])

                            # Add SmaxData object into the nested dictionary.
                            t.setdefault(lua_struct[offset][leaf_index].decode("utf-8"),
                                         smax_data_object)
            return tree

        return self._parse_lua_pull_response(lua_data)

    @staticmethod
    def _to_smax_format(value):
        """
        Private function that converts a given data value to the string format
        that SMAX supports.
        Args:
            value: Any supported data type, including (nested) dicts.

        Returns:
            tuple: tuple of (data_string, type_name, dim_string)
        """

        # Derive the type according to Python.
        python_type = type(value)

        # Single value of a supported type, cast to string and send to redis.
        if python_type in _REVERSE_TYPE_MAP:
            type_name = _REVERSE_TYPE_MAP[python_type]
            return str(value), type_name, 1

        # If this is an SmaxData object, just pass along the data attribute.
        if python_type == SmaxData:
            value = value.data

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
            return converted_data, type_name, size

        else:
            raise TypeError(f"Unable to convert {python_type} for SMAX")

    def _recurse_nested_dict(self, dictionary, table=None, commands=[]):
        """
        Private function to recursively traverse a nested dictionary, finding
        the leaf nodes that have actual data values.  Each real data value
        ends up as an SmaxCommand object and gets appended to a list, which is
        returned after the dictionary is searched.
        Args:
            dictionary (dict): Dict containing keys that exist in SMAX.
            table: Do not use, this is used to build the table name as the function recurses.

        Returns:
            list: List of SmaxCommand objects.
        """

        for key, value in dictionary.items():
            if isinstance(value, dict):
                if table is None:
                    self._recurse_nested_dict(value, key)
                else:
                    self._recurse_nested_dict(value, f"{table}:{key}")
            else:
                converted_data, type_name, size = self._to_smax_format(value)
                commands.append(SmaxCommand(table, key, converted_data, type_name, size))
        return commands

    def smax_share(self, table, key, value):
        """
        Send data to redis using the smax macro HSetWithMeta to include
        metadata.  The metadata is typeName, dataDimension(s), dataDate,
        source of the data, and a sequence number.  The first two are
        determined from the data and the source from this computer's name
        plus the program name if given when this class is instantiated.
        Date and sequence number are added by the redis macro.
        Args:
            table (str): SMAX table name
            key (str): SMAX key name
            value: data to store, takes supported types, including (nested) dicts.

        Returns:
            return value from redis-py's evalsha() function.
        """

        # If this is not a dict, then convert data to smax format and send.
        if not isinstance(value, dict):
            converted_data, type_name, size = self._to_smax_format(value)
            return self._evalsha_set(table, key, converted_data, type_name, size)
        else:
            # Recursively traverse the (nested) dictionary to generate a set
            # of values to update atomically.
            return self._pipeline_evalsha_set(table, key, self._recurse_nested_dict(value))

    def _evalsha_set(self, table, key, data_string, type_name, size):
        """
        Private function that calls evalsha() using an SMAX LUA script.
        Args:
            table (str): SMAX table name.
            key (str): SMAX key name.
            data_string (str): Data converted to proper SMAX string format.
            type_name (str): String representation of type.
            size: (str): Representation of the dimensions of the data. If one
                     dimension, than a single integer.Otherwise will be a string
                     of space delimited dimension values.

        Returns:
            return value from redis-py's evalsha() function.
        """

        try:
            return self._client.evalsha(self._setSHA, '1', table, self._hostname, key,
                                        data_string, type_name, size)
        except (ConnectionError, TimeoutError):
            self.logger.error("Redis seems down, unable to call the _setSHA LUA script.")
            raise

    def _pipeline_evalsha_set(self, table, key, commands):
        """
        In order to execute multiple LUA scripts atomically, it has to use the
        pipeline module in redis-py.  This function takes a list of commands,
        and issues them as a "pipeline", which uses a MULTI/EXEC block under the
        covers.
        Args:
            table (str): SMAX table name
            key (str): SMAX key name
            commands (list): List of SmaxCommand objects.

        Returns:
            return value from redis-py's pipeline.execute() function.
        """
        try:
            pipeline = self._client.pipeline()
            for command in commands:
                pipeline.evalsha(self._setSHA, '1',
                                 f"{table}:{key}:{command.table}",
                                 self._hostname,
                                 command.key,
                                 command.data,
                                 command.type,
                                 command.dim)
            return pipeline.execute()
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
        Args:
            pattern (str): Either full name of smax field, or use a wildcard '*'
                           at the end of the pattern to be notified for anything
                           underneath.
            callback (func): Function that takes a single argument (Default=None).
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
        """
        Unsubscribe from all subscribed channels, or pass a pattern argument
        to unsubscribe from specific channels.
        Args:
            pattern (str): Either full name of smax field, or use a wildcard '*'
                           at the end of the pattern to be notified for anything
                           underneath.
        """
        if self._pubsub is not None:
            if pattern is None:
                self._pubsub.punsubscribe()
                self._pubsub.unsubscribe()
            elif pattern.endswith("*"):
                self._pubsub.punsubscribe(f"smax:{pattern}")
            else:
                self._pubsub.unsubscribe(f"smax:{pattern}")

    def _redis_listen(self, pattern=None, timeout=None, notification_only=False):
        """
        Private function to help implement the "wait" functions in this API.
        In the redis-py library, the listen() function is a blocking call, so
        it gets used if there is no timeout specified.  When there is a timeout,
        the get_message() function is used, because listen() doesn't take a timeout
        value.  The get_message() function doesn't block by default, but when
        a timeout is specified it blocks until the timeout is reached. This function
        will raise a redis Timeout exception when the timeout is reached.

        Args:
            pattern (str): SMAX table/key pattern to listen on.
            timeout (float): Value in seconds to wait before raising timeout exception.
            notification_only (bool): If True, only returns the notification from redis.

        Returns:
            Either a (list) notification, or the actual pulled data.
        """

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

    def smax_wait_on_subscribed(self, pattern, timeout=None, notification_only=False):
        """
        If you use smax_subscribe without a callback, you can use this function
        to specify with channel to listen to, and block until a message is received.
        Args:
            pattern (str): SMAX table/key pattern to listen on.
            timeout (int): Value in seconds to wait before raising timeout exception.
            notification_only (bool): If True, only returns the notification from redis.

        Returns:
            Either a (list) notification, or the actual pulled data.
        """
        return self._redis_listen(pattern=pattern, timeout=timeout,
                                  notification_only=notification_only)

    def smax_wait_on_any_subscribed(self, timeout=None, notification_only=False):
        """
        If you use smax_subscribe without a callback, you can use this function
        to block until a message is received from any channel you are subscribed to.
        Args:
            timeout (float): Value in seconds to wait before raising timeout exception.
            notification_only (bool): If True, only returns the notification from redis.

        Returns:
            Either a (list) notification, or the actual pulled data.
        """
        return self._redis_listen(timeout=timeout,
                                  notification_only=notification_only)

    def smax_set_description(self, table, description):
        return self.smax_push_meta("description", table, description)

    def smax_get_description(self, table):
        return self.smax_pull_meta(table, "description")

    def smax_set_units(self, table, unit):
        return self.smax_push_meta("units", table, unit)

    def smax_get_units(self, table):
        return self.smax_pull_meta(table, "units")

    def smax_set_coordinate_system(self, table, coordinate_system):
        pass

    def smax_get_coordinate_system(self, table):
        pass

    def smax_create_coordinate_system(self, n_axis):
        pass

    def smax_push_meta(self, meta, table, value):
        return self._client.hset(f"<{meta}>", table, value)

    def smax_pull_meta(self, table, meta):
        result = self._client.hget(f"<{meta}>", table).decode("utf-8")
        if type(result) == bytes:
            return result.decode("utf-8")
        else:
            return result


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
