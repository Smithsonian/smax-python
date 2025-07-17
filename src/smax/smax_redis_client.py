import os
from math import ceil, log
from collections.abc import Container, Sequence
import logging
import socket
from datetime import datetime, timezone
from fnmatch import fnmatch

import psutil
import numpy as np
from numpy._core._exceptions import UFuncTypeError

from redis import Redis
from redis.exceptions import NoScriptError, BusyLoadingError, ConnectionError, TimeoutError
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from .smax_client import SmaxClient, SmaxData, SmaxInt, SmaxFloat, SmaxBool, SmaxStr, \
        SmaxStrArray, SmaxArray, SmaxStruct, SmaxInt8, SmaxInt16, SmaxInt32, \
        SmaxInt64, SmaxFloat32, SmaxFloat64, SmaxBool, SmaxBytes, \
        _TYPE_MAP, _REVERSE_TYPE_MAP, _SMAX_TYPE_MAP, _REVERSE_SMAX_TYPE_MAP, \
        optional_metadata, SmaxConnectionError, SmaxKeyError, SmaxUnderflowWarning, \
        join, normalize_pair, print_smax, print_tree
        
# This prefix is used on SMA-X pub/sub channels to identify the messages/notification
# channels relevant to SMA-X
pubsub_prefix = "smax"
# Default to looping the pubsub threads on 10 ms cadence.
pubsub_sleep = 0.010

loglevel = logging.INFO
logging.basicConfig(level=loglevel)
logger = logging.getLogger(__name__)

class SmaxRedisClient(SmaxClient):
    def __init__(self, redis_ip="localhost", redis_port=6379, redis_db=0,
                 program_name=None, hostname=None, debug=False, logger=logger):
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
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.ERROR)
        
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger(__name__)

        # Attributes for package, not exposed to users.
        self._redis_ip = redis_ip
        self._redis_port = redis_port
        self._redis_db = redis_db

        self._getSHA = None
        self._setSHA = None
        self._multi_getSHA = None
        self._multi_setSHA = None
        self._get_structSHA = None

        self._list_zeroesSHA = None
        self._list_higher_thanSHA = None
        self._list_newer_thanSHA = None
        
        self._dsm_get_tableSHA = None
        
        self._purgeSHA = None
        self._purge_volatileSHA = None
        self._del_structSHA = None

        self._pubsub = None
        self._callback_pubsub = None
        self._pipeline = None

        self._threads = []

        # Obtain _hostname automatically, unless '_hostname' argument is passed.
        self._hostname = socket.gethostname() if hostname is None else hostname
        
        program_name = psutil.Process(os.getpid()).name()
        self._hostname += ':' + program_name

        # Call parent constructor, which calls smax_connect_to() and sets the
        # returned client as self._client.
        super().__init__(redis_ip, redis_port, redis_db)

        # load the script SHAs from the server
        self._get_scripts()


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
        retry = Retry(ExponentialBackoff(cap=30, base=0.05), 30)
        try:
            # Connect to redis-server, and store LUA scripts on the object.
            # StrictRedis and Redis are now identical, so let's be explicit
            redis_client = Redis(host=redis_ip,
                                       port=redis_port,
                                       db=redis_db,
                                       retry=retry,
                                       retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError, OSError],
                                       health_check_interval=30)
            self._logger.info(f"Connected to redis server {redis_ip}:{redis_port} db={redis_db}")
            return redis_client
        except (ConnectionError, TimeoutError) as e:
            self._logger.error(f"Connecting to redis and getting scripts failed with {e}")
            raise SmaxConnectionError(e.args)


    def _get_scripts(self):
        """
        Get the SHAs of the cached scripts using the Redis.hget methods
        """
        self._logger.info(f"Pulling script SHAs from server")
        
        self._getSHA = self._client.hget('scripts', 'HGetWithMeta')
        self._setSHA = self._client.hget('scripts', 'HSetWithMeta')
        self._multi_getSHA = self._client.hget('scripts', 'HMGetWithMeta')
        self._multi_setSHA = self._client.hget('scripts', 'HMSetWithMeta')
        self._get_structSHA = self._client.hget('scripts', 'GetStruct')
        self._purgeSHA = self._client.hget('scripts', 'Purge')
        self._purge_volatileSHA = self._client.hget('scripts', 'PurgeVolatile')
        self._list_newer_thanSHA = self._client.hget('scripts', 'ListNewerThan')
        self._list_newer_thanSHA = self._client.hget('scripts', 'ListOlderThan')
        self._list_higher_thanSHA = self._client.hget('scripts', 'ListHigherThan')
        self._list_zeroesSHA = self._client.hget('scripts', 'ListZeroes')
        self._dsm_get_tableSHA = self._client.hget('scripts', 'DSMGetTable')

    def smax_disconnect(self):
        """
        Python manages a connection pool automatically, if somehow that fails
        release the connection, this disconnect function will do it.
        """

        if self._client.connection:
            self._client.connection.disconnect()
        self._logger.info(f"Disconnected redis server {self._redis_ip}:{self._redis_port} db={self._redis_db}")

    def _parse_lua_pull_response(self, lua_data, smaxname, pull_meta=False, raw=False):
        """
        Private method to parse the response from calling the HGetWithMeta LUA
        script.
        Args:
            lua_data (list): value, vtype, dim, timestamp, origin, serial
            smaxname (str): Full name of the SMAX table and key
            pull_meta (bool): Whether to pull additional metadata
            raw (bool): Return the byte string from Redis as an SmaxBytes object.

        Returns:
            Smax<var>: Populated Smax<var> dataclass object.
        """

        # Extract the type out of the meta data, and map string to real type object.
        type_name = lua_data[1].decode("utf-8")
        self._logger.debug(f"_parse_lua_pull_response() got type {type_name}")

        if raw:
            data_type = SmaxBytes
        elif type_name in _SMAX_TYPE_MAP:
            data_type = _SMAX_TYPE_MAP[type_name]
        else:
            self._logger.warning(f"I can't deal with data of type {type_name}, defaulting to 'string'")
            type_name = "string"
            data_type = SmaxStr

        # Extract data, origin and sequence from meta data.
        data_date = datetime.fromtimestamp(float(lua_data[3]), timezone.utc)
        origin = lua_data[4].decode("utf-8")
        sequence = int(lua_data[5])
        
        # Extract dimension information from meta data.
        data_dim = tuple(int(s) for s in lua_data[2].decode("utf-8").split())
        # If only one dimension convert to a single value (rather than list)
        if len(data_dim) == 1:
            data_dim = data_dim[0]

        # If there is only a single value, cast to the appropriate type and return.
        if raw:
            data = data_type(lua_data[0], type=type_name, dim=data_dim, timestamp=data_date, \
                            origin=origin, seq=sequence, smaxname=smaxname)
        elif data_dim == 1:
            if type_name == 'string':
                strdata = lua_data[0].decode("utf-8")
                data = data_type(strdata, type=type_name, dim=data_dim, timestamp=data_date, \
                                origin=origin, seq=sequence, smaxname=smaxname)
            else:
                # Use the builtin base type's conversion from string to that type
                data = data_type(lua_data[0], type=type_name, dim=data_dim, timestamp=data_date, origin=origin, seq=sequence, smaxname=smaxname)

        # This is some kind of array.
        else:
            # If this is a list of strings, just clean up string and return.
            if type_name == 'string':
                data = lua_data[0].decode("utf-8").split("\r")
                # Remove the leading and trailing \' in each string in the list.
                data = [s.strip("\'") for s in data]
                return SmaxStrArray(data, type=type_name, dim=data_dim, timestamp=data_date, \
                        origin=origin, seq=sequence, smaxname=smaxname)
            else:
                data = lua_data[0].decode("utf-8").split(" ")
                # Use numpy for all other numerical types
                data = SmaxArray(data, type=type_name, dim=data_dim, \
                        timestamp=data_date, origin=origin, seq=sequence, smaxname=smaxname)

        # Get additional meta data.
        if pull_meta:
            for meta in optional_metadata:
                m = None
                try:
                    m = self.smax_pull_meta(meta, smaxname)
                except:
                    continue
                if m is not None:
                    setattr(data, meta, m)

        self._logger.debug(f"_parse_lua_pull: returning {data}, {data.metadata}")
        return data

    def smax_pull(self, table, key, pull_meta=False, raw=False):
        """
        Get data which was stored with the smax macro HSetWithMeta along with
        the associated metadata. The return value will an Smax<type> object
        containing the data, type name, data dimension(s), data date, source of the
        data, and a sequence number. If you pulled a struct, you will get a
        nested dictionary back, with each leaf being an Smax<type> object.
        
        Note that Smax<type> scalars for numerical values are subclasses of
        `numpy` dtypes: i.e. `SmaxInt32` is a subclass of `numpy.int32`, and
        not related to Python's arbitrary precision built-in `int`.  You must cast
        either the built-in to an appropriate numpy type or the Smax<type> to 
        a built-in to test for (in) equality or membership of collections.
        
        Args:
            table (str): SMAX table name
            key (str): SMAX key name
            pull_meta (bool): Flag whether to pull optional metadata
            raw (bool): Return the unparsed data in a SmaxBytes object

        Returns:
            Smax<type>: Populated Smax<type> dataclass object.
        """
        # Carry out a sanity check on (table, key) pair and normalize
        table, key = normalize_pair(table, key)
        self._logger.debug(f"Calliing _getSHA with: '1', {table}, {key}")
        try:
            lua_data = self._client.evalsha(self._getSHA, '1', table, key)
        except NoScriptError:
            self._get_scripts()
            lua_data = self._client.evalsha(self._getSHA, '1', table, key)
        except (ConnectionError, TimeoutError) as e:
            self._logger.error(f"Reading {join(table, key)} from Redis {self._client} failed")
            raise SmaxConnectionError(e.args)
    
        self._logger.debug(f"Received response: {lua_data}")
        
        if lua_data is not None:
            self._logger.info(f"Successfully pulled {join(table, key)}")
        else:
            self._logger.warning(f"Failed to pull valid data for {join(table, key)} from {self._client}")
            raise SmaxKeyError(f"Unknown SMA-X error pulling {join(table, key)}")


        # Check that we got a valid response
        if lua_data[0] is None:
            self._logger.error(f"Could not find {join(table, key)} in Redis")
            raise SmaxKeyError(f"Could not find {join(table, key)} in Redis {self._client}")

        # Extract the type out of the meta data, and map string to real type object.
        type_name = lua_data[1].decode("utf-8")
        self._logger.debug(f"Type: {type_name}")
        # If the lua response says its a struct we have to now use another LUA
        # script to go back to redis and collect the struct.
        if type_name == "struct":
            lua_dim = int(lua_data[2])  # assuming that structs can't be multidimensional arrays
            lua_date = datetime.fromtimestamp(float(lua_data[3]), timezone.utc)
            if lua_data[4]:
                lua_origin = lua_data[4].decode("utf-8")
            else:
                lua_origin = ""
            if lua_data[5]:
                lua_sequence = int(lua_data[5])
            else:
                lua_sequence = -1
            try:
                lua_struct = self._client.evalsha(self._get_structSHA, '1', f"{table}:{key}")
                self._logger.info(f"Successfully pulled struct {table}:{key}")
            except NoScriptError:
                self._get_scripts()
                lua_struct = self._client.evalsha(self._get_structSHA, '1', f"{table}:{key}")
                self._logger.info(f"Successfully pulled struct {table}:{key}")
            except (ConnectionError, TimeoutError) as e:
                self._logger.error(f"Reading {table}:{key} from Redis failed")
                raise SmaxConnectionError(e.args)

            # The struct will be parsed into a nested python dictionary.
            tree = SmaxStruct({}, dim=lua_dim, timestamp=lua_date, origin=lua_origin, seq=lua_sequence, smaxname=f"{table}:{key}")
            
            if pull_meta:
                for meta in optional_metadata:
                    try:
                        m = self.smax_pull_meta(meta, smaxname)
                    except:
                        continue
                    if m is not None:
                        setattr(tree, meta, m)
            
            for struct_name_index, struct_name in enumerate(lua_struct[0]):
                t = tree
                names = struct_name.decode("utf-8").replace(f"{table}:", "", 1).split(':')

                for table_name_index, table_name in enumerate(names):
                    offset = struct_name_index + struct_name_index + 1
                    smaxname = join(struct_name.decode("utf-8"), table_name)
                            
                    # Grow a new hierarchical level with a blank dictionary.
                    t = t.setdefault(table_name, SmaxStruct({}, dim=lua_dim, timestamp=lua_date, \
                        origin=lua_origin, seq=lua_sequence, smaxname=smaxname))

                    # If this is the last name in the path, add actual data.
                    if table_name_index == len(names) - 1:

                        # Create offset indices for more readable code.
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
                            lua_origin = lua_struct[offset2][4][leaf_index]
                            lua_sequence = lua_struct[offset2][5][leaf_index]

                            # Parser will return an SmaxData object.
                            self._logger.debug(f"struct_name: {struct_name.decode('utf-8')}")
                            smaxname = struct_name.decode("utf-8") + ":" + leaf.decode("utf-8")
                            smax_data_object = self._parse_lua_pull_response(
                                [lua_data, lua_type, lua_dim, lua_date,
                                 lua_origin, lua_sequence], smaxname, pull_meta)

                            # Add SmaxData object into the nested dictionary.
                            t.setdefault(lua_struct[offset][leaf_index].decode("utf-8"),
                                         smax_data_object)
            
            return tree

        return self._parse_lua_pull_response(lua_data, f"{table}:{key}", raw=raw)

    def smax_share(self, table, key, value, push_meta=False, maintain_type=False, smax_type=None):
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
            push_meta (bool): Push optional metadata if present in object 
                            (does not work for SmaxStructs and is not atomic).
            maintain_type (bool): Check the metadata on Redis for each node, and 
                                  cast to that type if possible when sharing.
            smax_type (str): Force casting to smax_type before sharing. Overrides
                             maintain type.

        Returns:
            return value from redis-py's evalsha() function.
        """
        # If this is not a dict, then convert data to smax format and send.
        if not isinstance(value, dict):
            if maintain_type and not smax_type:
                smax_type = self.smax_pull_meta('types', join(table, key))
            if push_meta:
                if type(value) in _REVERSE_SMAX_TYPE_MAP:
                    for meta in optional_metadata:
                        if hasattr(value, meta):
                            self.smax_push_meta(meta, table, getattr(value, meta))

            converted_data, type_name, size = _to_smax_format(value, smax_type=smax_type)
            self._logger.debug(f"Calling HSetWithMeta script with {table}, {key}, {converted_data}, {type_name}, {size}")
            return self._evalsha_set(table, key, converted_data, type_name, size)
        else:
            # Recursively traverse the (nested) dictionary to generate a set
            # of values to update atomically. The recurse_nested_dict function
            # yields key/value pairs as it finds the leaf nodes of the nested
            # dict.
            leaves = _recurse_nested_dict(value)
            fields = _get_struct_fields(leaves)
            if maintain_type:
                for f in fields:
                    if f[3] == "value":
                        smax_type = self.smax_pull_meta('types', join(table, key, f[0], f[1]))
                        self._logger.info(f'smax_share: Got type {smax_type} from SMA-X metadata, casting {f[1]} to type')
                        f[3] = smax_type
            tables = _get_struct_tables(table, key, fields)

            self._logger.debug(f"Calling HMSetWithMeta script with {table}, {key} {tables}")

            return self._pipeline_evalsha_set(table, key, tables)

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
            result = self._client.evalsha(self._setSHA, '1', table,
                                          self._hostname, key, data_string,
                                          type_name, size)
            self._logger.info(f"Successfully shared to {table}:{key}")
            return result
        except NoScriptError:
            self._get_scripts()
            result = self._client.evalsha(self._setSHA, '1', table,
                                              self._hostname, key, data_string,
                                              type_name, size)
            self._logger.info(f"Successfully shared to {table}:{key}")
            return result
        except (ConnectionError, TimeoutError) as e:
            self._logger.error("Redis seems down, unable to call the _setSHA LUA script.")
            raise SmaxConnectionError(e.args)

    def _pipeline_evalsha_set(self, table, key, commands):
        """
        In order to execute multiple LUA scripts atomically, it has to use the
        pipeline module in redis-py.  This function takes a list of commands,
        and issues them as a "pipeline", which uses a MULTI/EXEC block under the
        covers. The HMSetWithMeta LUA script allows you to update multiple
        values for a table, although a separate call for each new table is needed.

        Args:
            table (str): SMAX table name
            key (str): SMAX key name
            commands (dict): Keys for each table to update, and list of commands
            to pass to HMSetWithMeta LUA script.

        Returns:
            return value from redis-py's pipeline.execute() function.
        """
        # Append the optional 'T' value to the end of the last entry in the command dict.
        #
        # Dict keys are (as of Py 3.7) sorted by the order they are added to the dict,
        # so this command should be the last one accessed in the for loop below.
        commands[list(commands.keys())[-1]].append('T')

        self._logger.debug(commands)

        try:
            if self._pipeline is None:
                self._pipeline = self._client.pipeline()
            for k in commands.keys():
                t, ke = normalize_pair(table, k)
                self._logger.debug(f"munged table name {t}\n munged key name {ke}")
                try:
                    self._logger.debug(f"evalsha arguments: {t}, {ke}, {commands[k]}")
                    self._pipeline.evalsha(self._multi_setSHA, '1',
                                       f"{t}:{ke}",
                                       self._hostname,
                                       *commands[k])
                except NoScriptError:
                    self._get_scripts()
                    self._pipeline.evalsha(self._multi_setSHA, '1',
                                       join(t, ke),
                                       self._hostname,
                                       *commands[k])
            result = self._pipeline.execute()
            self._logger.info(f"Successfully executed pipeline share to {table}:{key}:{list(commands.keys())}")
            return result
        except (ConnectionError, TimeoutError) as e:
            self._logger.error("Unable to call HMSetWithMeta LUA script.")
            raise SmaxConnectionError(e.args)

    def smax_lazy_pull(self, table, key, value):
        raise NotImplementedError("Available in C API, not in python")

    def smax_lazy_end(self, table, key):
        raise NotImplementedError("Available in C API, not in python")

    def fail(self, error):
        """A minimalist error handler required by call_with_retry in smax_subscribe"""
        self._logger.error(f"Redis connection issue {repr(error)}")

    def smax_subscribe(self, pattern, callback=None, pubsub_sleep=pubsub_sleep):
        """
        Subscribe to a redis field or group of fields. You can type the full
        name of the field you'd like to subscribe too, or use a wildcard "*"
        character as a suffix to specify a pattern. Use a callback for asynchronous
        processing of notifications, or use one of the smax_wait_on functions.
        
        Internal to the HSET* and HMSET*, notifications for pub/sub are
        generated with the prefix `smax:`. This needs to be added/removed within
        `smax_subscribe`, etc.
        
        Args:
            pattern (str): Either full name of smax field, or use a wildcard '*'
                           at the end of the pattern to be notified for anything
                           underneath.
            callback (func): Function that takes a single argument (Default=None).
                             The message in your callback will be an SmaData
                             object, or a nested dictionary for a struct.
            pubsub_sleep (float): Sleep time within each loop of the pubsub
                                  event handling thread
        """
        def parent_callback(message):
            msg_pattern = message["pattern"]
            if msg_pattern is not None:
                path = message["pattern"].decode("utf-8")[:-1]
            else:
                path = message["channel"].decode("utf-8")

            table, key = normalize_pair(path[len(pubsub_prefix)+1:])
            self._logger.debug(f"Callback notification received:{message}")
            data = self.smax_pull(table, key)
            self._logger.debug(f"Callback notification received:{type(data)} {data}")
            self._logger.debug(f".data: {data.data}")
            self._logger.debug(f"metadata {data.asdict()}")
            callback(data)
            
        def exception_handler(ex, pubsub, thread):
            """Silently close threads if connection fails - other code will catch the missing
            connection"""
            self._logger.info("Pubsub lost connection")
            pubsub.connection.retry.call_with_retry(pubsub.ping, self.fail)
            pubsub.on_connect(pubsub.connection)
            self._logger.info("Pubsub reconnected")

        if callback is not None and self._callback_pubsub is None:
            self._callback_pubsub = self._client.pubsub()
            self._logger.debug("Created redis pubsub object for callbacks")

        elif callback is None and self._pubsub is None:
            self._pubsub = self._client.pubsub()
            self._logger.debug("Created redis pubsub object")

        if pattern.endswith("*"):
            if callback is None:
                self._pubsub.psubscribe(f"{pubsub_prefix}:{pattern}")
                self._logger.info(f"Subscribed to {pattern}")
            else:
                self._callback_pubsub.psubscribe(**{f"{pubsub_prefix}:{pattern}": parent_callback})
                self._callback_pubsub.run_in_thread(sleep_time=pubsub_sleep, daemon=True, exception_handler=exception_handler)
                self._logger.info(f"Subscribed to {pattern} with a callback")
        else:
            if callback is None:
                self._pubsub.subscribe(f"{pubsub_prefix}:{pattern}")
                self._logger.info(f"Subscribed to {pattern}")
            else:
                self._callback_pubsub.subscribe(**{f"{pubsub_prefix}:{pattern}": parent_callback})
                self._logger.info(f"Subscribed to {pattern} with a callback")
                self._callback_pubsub.run_in_thread(sleep_time=pubsub_sleep, daemon=True, exception_handler=exception_handler)

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
                self._logger.info("Unsubscribed from all tables")
            elif pattern.endswith("*"):
                self._pubsub.punsubscribe(f"{pubsub_prefix}:{pattern}")
                self._logger.info(f"Unsubscribed from {pattern}")
            else:
                self._pubsub.unsubscribe(f"{pubsub_prefix}:{pattern}")
                self._logger.info(f"Unsubscribed from {pattern}")

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
                self._logger.debug(f"Waiting for message matching {pattern} with .listen()")
                for message in self._pubsub.listen():
                    break
            else:
                self._logger.debug(f"Waiting for message matching {pattern} with .get_message() with timeout {timeout}")
                message = self._pubsub.get_message(timeout=timeout)
                
            self._logger.debug(f"Redis message received:{message}")
            if message is None:
                raise TimeoutError("Timed out waiting for redis message.")
            elif message["type"] == "message" or message["type"] == "pmessage":
                channel = message["channel"].decode("utf-8")
                if channel.startswith(f"{pubsub_prefix}:"):
                    if pattern is None:
                        found_real_message = True
                    elif fnmatch(channel[len(pubsub_prefix)+1:], pattern):
                        found_real_message = True

        if notification_only:
            # Strip the "smax:" prefix off of the channel.
            channel = message["channel"].decode("utf-8")
            if channel.startswith(pubsub_prefix):
                message["channel"] = channel[len(pubsub_prefix)+1:]
            else:
                message["channel"] = channel

            # Decode the other fields.
            message["data"] = message["data"].decode("utf-8")
            self._logger.debug(f"Got notification: message = {message}")
            return message
        else:
            if pattern is None:
                # Pull the exact table that sent the notification.
                table, key = normalize_pair(channel[len(pubsub_prefix)+1:])
            else:
                # Pull the parent struct or "pattern" that was subscribed to.
                # (pattern is not prefixed with `smax:`)
                table, key = normalize_pair(pattern)
                key = key.strip('*')
            self._logger.debug(f"Got table = {table}, key = {key}")
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

    def smax_purge(self, table, key=None):
        """Purges a table or key from Redis.  Use with ultimate caution.

        Args:
            table (str): SMA-X table to purge. Can include wild cards to purge by pattern matching.
            key (str, optional): Specific SMA-X key in table to purge. Defaults to None.
            
        Returns:
            int: number of SMA-X keys purged from Redis
        """
        if table is None:
            pattern = "*"
        else:
            pattern = table

        if key is not None:
            pattern = f"{pattern}:{key}"
            
        self._logger.warning(f"Purging all keys matching {pattern}")
            
        return self._client.evalsha(self._purgeSHA, '0', pattern)
    
    def smax_purge_volatile(self):
        """Purges all volatile tables and keys from Redis.  Use with ultimate caution.
        """
        self._logger.warning(f"Purging all volatile keys")
        self._client.evalsha(self._purge_volatileSHA, '0')
        
    def smax_dsm_get_table(self, target, key, host=None):
        """Get the SMA-X name that maps to a DSM target, key, and (optionally) host.

        Args:
            target (str): DSM target or caller
            key (str): DSM key
            host (str, optional): DSM host (caller). If not set, defaults to target.
            
        Returns:
            str : SMA-X table where the key can be found.
        """
        if host is None:
            host = target
        return self._client.evalsha(self._dsm_get_tableSHA, host, target, key)
    
    def smax_list_newer_than(self, dt):
        """List all SMA-X keys newer than the datetime object.

        Args:
            dt (datetime.datetime): cutoff time.
            
        Returns:
            list[(str, smax.SmaxData)]: list of pairs of SMA-X keys and values newer than dt
        """
        # If tzinfo is not given in the datetime object, assume it is UTC
        if dt.tzinfo is None:
            dt.replace(tzinfo=datetime.timezone.utc)
            
        timestamp = dt.timestamp()
        
        keypairs = self._client.evalsha(self._list_newer_thanSHA, timestamp)
    
        result  = []
        for kp in keypairs:
            smax_value = self.smax_pull(kp[0])
            result.append((kp[0], smax_value))
            
        return result

    def smax_list_older_than(self, dt):
        """List all SMA-X keys older than the datetime object.

        Args:
            dt (datetime.datetime): cutoff time.
            
        Returns:
            list[(str, smax.SmaxData)]: list of pairs of SMA-X keys and values older than dt
        """
        # If tzinfo is not given in the datetime object, assume it is UTC
        if dt.tzinfo is None:
            dt.replace(tzinfo=datetime.timezone.utc)
            
        timestamp = dt.timestamp()
        
        keypairs = self._client.evalsha(self._list_older_thanSHA, timestamp)
    
        result  = []
        for kp in keypairs:
            smax_value = self.smax_pull(kp[0])
            result.append((kp[0], smax_value))
            
        return result
    
    def smax_list_higher_than(self, table, value):
        """List all SMA-X fields in table with values higher than the value.

        Args:
            table (str): table, struct or meta name
            value (int or float): cutoff value.
            
        Returns:
            list[(str, smax.SmaxData)]: list of pairs of SMA-X fields and values higher than value
        """
        fields = self._client.evalsha(self._list_higher_thanSHA, table, value)
    
        result  = []
        for f in fields:
            smax_value = self.smax_pull(table, f)
            result.append((f, smax_value))
            
        return result
    
    def smax_list_zeroes(self, key):
        """List all fields in key equal to zero.
        
        Args:
            key (str): Redis key to test.
        
        Returns:
            list(str): list of all fields equal to zero."""
        return self._client.evalsha(self._list_zeroesSHA, key)

    def smax_set_description(self, table, description):
        """
        Creates a <description> metadata field for specified table.
        Args:
            table (str): Full SMAX table name (with key included).
            description (str): String for the description of this smax field.
        """
        return self.smax_push_meta("description", table, description)

    def smax_get_description(self, table):
        return self.smax_pull_meta("description", table)

    def smax_set_units(self, table, unit):
        return self.smax_push_meta("units", table, unit)

    def smax_get_units(self, table):
        return self.smax_pull_meta("units", table)

    def smax_set_coordinate_system(self, table, coordinate_system):
        self.smax_push_meta("coords", table, coordinate_system)

    def smax_get_coordinate_system(self, table):
        self.smax_pull_meta("coords", table)

    def smax_create_coordinate_system(self, n_axis):
        raise NotImplementedError("Available in C API, not in python")

    def smax_push_meta(self, meta, table, value):
        """
        Sets additional metadata for a given table.
        Args:
            meta (str): Key for the metadata field.
            table (str): Name of the table to set metadata for.
            value (str or int): Metadata value to store in redis, note this
                                needs to be a string int.

        Returns:
            Result of redis-py hset function.
        """
        try:
            result = self._client.hset(f"<{meta}>", table, value)
            self._logger.info(f"Successfully shared metadata to {table}")
            return result
        except (ConnectionError, TimeoutError) as e:
            self._logger.error("Redis seems down, unable to call hset.")
            raise SmaxConnectionError(e.args)

    def smax_pull_meta(self, meta, table):
        """
        Pulls specified metadata field from a given table.
        Args:
            table (str): Name of the table to pull metadata from.
            meta (str): Metadata field name to pull.

        Returns:
            Result of redis-py hget function.
        """
        try:
            result = self._client.hget(f"<{meta}>", table).decode("utf-8")
            self._logger.info(f"Successfully pulled metadata from {table}")
            if type(result) == bytes:
                return result.decode("utf-8")
            else:
                return result
        except AttributeError:
            self._logger.warning(f"Could not find metadata for {table}")
            return None
        except (ConnectionError, TimeoutError) as e:
            self._logger.error("Redis seems down, unable to call hget.")
            raise SmaxConnectionError(e.args)

def _to_smax_format(value, smax_type=None):
    """
    Private function that converts a given data value to the string format
    that SMAX supports.
    Args:
        value: Any supported data type, including (nested) dicts.
        smax_type (str): SMA-X type to cast value(s) to.

    Returns:
        tuple: tuple of (data_string, type_name, dim_string)
    """
    # Get a value for type testing, and determine the shape
    type_name=None
    
    # If this is a container, detect the minimum scalar SMA-X type, and the shape of
    # the container
    if _container_but_not_str(value):
        logger.debug("_to_smax_format: detected a Container type")
        # Cast the (possibly nested) container to an array
        try:
            # This will cast the array to the most restrictive numpy type that can contain all
            # of the values.
            # It will fail for inhomogenous nested containers, and possibly in other circumstances
            value_array = np.array(value)
            python_type = value_array.dtype.type
            logger.debug(f"_to_smax_format: Successfully cast Container to array of dtype {python_type}")
        except ValueError as e:
            # This likely means that the array is inhomogenous in shape, and must be 
            # flattened to single dimension array of string representations of subarrays
            # as SMA-X does not allow for inhomogenous arrays of any type
            logger.warning(f"_to_smax_format: Array conversion failed with '{e.args[0]}'. Will revert to 'string' format and a single dimension array.")
            smax_type = 'string'
            retval, shape = _container_to_stringarray(value, flatten=False)
            return retval, smax_type, shape
        
        # Only do type promotion if we have numerical types
        if issubclass(python_type, str):
            smax_type = 'string'
            test_value = value_array.flatten()[0]
        elif python_type is np.object_:
            smax_type = 'string'
            retval, shape = _container_to_stringarray(value)
            return retval, smax_type, shape
        else:
            # Get the largest value to use for determining the SmaxType
            # Where the largest value is not defined, use the first value
            try:
                # This will fail for non-numeric types
                test_value = np.nanmax(np.abs(value_array))
                logger.debug(f"_to_smax_format: Detected largest value of {test_value}, which will be used to determine type")
            except (UFuncTypeError, TypeError):
                test_value = value_array.flatten()[0]
                logger.debug(f"_to_smax_format: Could not find largest value in array, using {test_value} to determine type")
        shape = _get_dims(value_array)
    else: # If this is a single value, get the type of the value
        test_value = value
        python_type = type(test_value)
        shape = 1
    
    logger.debug(f"_to_smax_format received {value}, test value {test_value}, type {str(python_type)}, smax_type {smax_type}, shape {shape}")
    
    # If we've been given a SMA-X type to use, try to use it
    if smax_type in _TYPE_MAP:
        logger.debug(f"_to_smax_format: Received valid SMA-X Type {smax_type}")
        type_name = smax_type
        
        # If we have int type, check that the values fit within the given width, and promote if not
        if smax_type.startswith('int'):
            logger.debug("_to_smax_format: Requested integer type, determining length required")
            # We assume here that only numpy.int<x> types are in _TYPE_MAP
            # We have to add 1 to the Python int().bit_length() as it excludes the
            # sign bit.
            if _TYPE_MAP[smax_type](0).nbytes*8 < (int(test_value)).bit_length() + 1:
                min_type = np.min_scalar_type(int(test_value))
                # Test for uints
                # Add one to test_value before negating, as e.g. 128 does not fit in int8, but -128 does.
                if min_type.name.startswith('u'):
                    min_type = np.min_scalar_type(-int(test_value+1))
                new_smax_type = _REVERSE_TYPE_MAP[min_type.type]
                logger.warning(f"Value {int(test_value)} would overflow {smax_type}, promoting to {new_smax_type}")
                type_name = new_smax_type
        
        if smax_type.startswith('float'):
            logger.debug("_to_smax_format: Requested float type, determining length required")
            with np.errstate(over='raise'):
                try:
                    _TYPE_MAP[smax_type](test_value)
                except FloatingPointError:
                    logger.warning(f"Value {test_value} would overflow {smax_type}, promoting to float64")
                    type_name = 'float64'
        
        # Check that unsafe casting is possible
        if not np.can_cast(np.min_scalar_type(test_value), _TYPE_MAP[smax_type], casting='unsafe'):
            logger.warning(f"Cannot cast {type(test_value)} to {smax_type}, trying automatic determination of type.")
        else:
            # Only warn about casting float to int, int to bool, etc.
            if not np.can_cast(np.min_scalar_type(test_value), _TYPE_MAP[smax_type], casting='same_kind'):
                logger.warning(f"Casting {type(test_value)} to {smax_type} may result in loss of information.")

            try:
                if _container_but_not_str(value):
                    # cast value_array created above to the given type
                    value_array = value_array.astype(_TYPE_MAP[smax_type], casting='unsafe')
                    
                    if smax_type == 'string' or smax_type == 'str':
                        retval, shape =  _container_to_stringarray(value_array)
                        return retval, smax_type, shape
                    
                    # This will only return if type conversion is successful
                    return _array_to_string(value_array), smax_type, _shape_to_dims(value_array.shape)
                
                else:
                    # Cast single value to given smax type
                    value = _TYPE_MAP[type_name](value)
                    
                    # This will only return if type conversion is successful
                    return _value_to_string(value), type_name, 1
                
            except ValueError or OverflowError as e:
                logger.warning(f"Could not cast {type(value_array.dtype)} to {smax_type}: {e.args[0]}. Trying automatic determination of type.")
                # Pass the SmaxType determination off to the automatic determinator
    else:
        if smax_type is not None:
            logger.warning(f"Do not recognize requested SMA-X type {smax_type}")
                
    # Automatic determination of SMA-X type:
    #
    # We haven't been given a valid smax_type that works, so use the reverse type map to pick the type_name
    if python_type in _REVERSE_TYPE_MAP:
        type_name = _REVERSE_TYPE_MAP[python_type]
        logger.debug(f"_to_smax_format: autodetected Python type as SMA-X type {type_name}")
        
        # For Python ints, detect the appropriate width of the SMA-X int<n> to use
        if python_type is int or type_name.startswith('int'):
            # This bit of mathmagics calculates the size of a signed int, then rounds it up
            # to the next power of two.
            width = 2**ceil(log((int(test_value).bit_length() + 1), 2))
            if width < 8:
                width = 8
            type_name = f'int{width}'
            logger.debug(f"_to_smax_format: Supplied Python int, which fits into {type_name}")

    # Value of a Smax<var> type
    elif python_type in _REVERSE_SMAX_TYPE_MAP:
        type_name = value.type
    
    # Single value of an unknown type
    else:
        logger.warning(f"Did not recognize type {str(type(value))}, storing str(value) as SMA-X string type.")
        type_name = "string"

    # Convert and return the value
    if _container_but_not_str(value):
        if type_name == "string" or type_name == "str":
            retval, shape = _container_to_stringarray(value)
        else:
            retval = _array_to_string(value_array.astype(_TYPE_MAP[type_name], casting='unsafe'))
    else:
        retval = _value_to_string(_TYPE_MAP[type_name](value))
        
    return retval, type_name, shape
            

def _recurse_nested_dict(dictionary):
    """
    Private function to recursively traverse a nested dictionary, finding
    the leaf nodes that have actual data values.  Each real data value
    is yielded back as it recurses.
    Args:
        dictionary (dict): Dict containing keys that exist in SMAX.

    Yields:
        (key, value) for every leaf node in the nested dictionary.
    """
    for key, value in dictionary.items():
        if isinstance(value, dict):
            # If value is dict then iterate over all its values
            for pair in _recurse_nested_dict(value):
                yield (f"{key}:{pair[0]}", *pair[1:])
            
        else:
            yield key, value


def _get_struct_fields(leaves):
    """
    Private function to generate the set of all SMA-X fields required to describe the
    tree structure of a nested dictionary.
    Args:
        leaves (list): List of leaf nodes from _recurse_nested_dict()

    Returns:
        list of (key, field, value, type) for each field at each level of the SMA-X nested structure.
                    type is either "value" for a leaf node, or "struct" for an intermediate node.
    """
    outpairs = []
    for l in leaves:
        if ":" in l[0]:
            tiers = l[0].split(":")
            for t, tier in enumerate(tiers[:-1]):
                superstruct = join(*tiers[0:t])
                pair = [superstruct, tiers[t], join(*tiers[0:t+1]), "struct"]
                if pair not in outpairs:
                    outpairs.append(pair)
        table, key = normalize_pair(l[0])
        outpairs.append([table, key, l[1], "value"])
    return outpairs


def _get_struct_tables(table, key, fields):
    """
    Private function to generate a dictionary of tables and arguments to HMSET_WITH_META calls from
    fields generated by _get_struct_fields() for a nested SMA-X struct.
    Args:
        table (str)   : The top level hash table name for the nested struct.
        key (str)     : The top level key for the nested struct.
        fields (list) : List of nested fields from _get_struct_fields()

    Returns:
        list of (key, field, value, type) for each field at each level of the SMA-X nested structure.
                    type is either "value" for a leaf node, or "struct" for an intermediate node.
    """
    tables = {}
    for field in fields:
        if field[3] == "struct":
            converted_data, type_name, dim = (join(table, key, field[2]), field[3], 1)
        else:
            if field[3] != 'value':
                converted_data, type_name, dim = _to_smax_format(field[2], smax_type=field[3])
            else:
                converted_data, type_name, dim = _to_smax_format(field[2])
        if field[0] != "":
            tab = join(key, field[0])
        else:
            tab = key
        if tab not in tables:
            tables[tab] = []
        tables[tab].extend([field[1], converted_data, type_name, dim])
    return tables


def _flatten_container(value):
    """Flatten a container of (potentially inhomogenous) containers to a single array
    
    Args:
        value (Container): container that may contain other containers
        
    Returns:
        list : flattened version of the container."""
    ret = []
    for v in value:
        if isinstance(v, str):
            ret.append(v)
        elif _container_but_not_str(value):
            ret.extend(_flatten_container(v))
        else:
            ret.append(v)
    return ret


def _container_to_stringarray(value, flatten=True):
    """Convert a container of object to a SMA-X string array
    
    Args:
        value (Container): container that may contain other containers or objects
    
    Returns:
        list of str : flat list of string representations of contents of value
    """    
    shape = _get_dims(value)
    
    if flatten:
        value = _flatten_container(value)
    
    ret = []
    for v in value:
        ret.append(_value_to_string(v))
    
    return "\r".join(ret), shape


def _value_to_string(value):
    """Converts a value to a string, applying JSON format escape sequences for special characters"""
    return str(value).encode('unicode_escape').decode('UTF-8')


def _array_to_string(value_array):
    """Convert a Numpy array to a SMA-X string"""
    return " ".join(np.array(value_array.flatten(), dtype=str))


def _shape_to_dims(shape):
    """Convert a Numpy shape tuple to a SMA-X dims string"""
    out = []
    for s in shape:
        out.append(str(s))
    return " ".join(out)


def _get_dims(value):
    """Get the dimensions of a value"""
    try:
        shape = _shape_to_dims(value.shape)
    except AttributeError:
        try:
            if _container_but_not_str(value):
                shape = len(value)
            else:
                shape = 1
        except TypeError:
            shape = 1
    return shape

def _container_but_not_str(obj):
    return isinstance(obj, Container) and not issubclass(type(obj), (str, bytes))