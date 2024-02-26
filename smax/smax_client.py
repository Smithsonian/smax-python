from abc import ABC, abstractmethod
from collections import namedtuple


# Named tuples for smax requests and responses.
SmaxData = namedtuple("SmaxData", "data type dim date origin seq smaxname")

class SmaxClient(ABC):

    def __init__(self, *args, **kwargs):
        # All child classes will call their smax_connect_to() when constructed.
        self._client = self.smax_connect_to(*args, **kwargs)

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return self.smax_disconnect()

    @abstractmethod
    def smax_connect_to(self, *args, **kwargs):
        pass

    @abstractmethod
    def smax_disconnect(self):
        pass

    @abstractmethod
    def smax_pull(self, table, key):
        pass

    @abstractmethod
    def smax_share(self, table, key, value):
        pass

    @abstractmethod
    def smax_subscribe(self, pattern):
        pass

    @abstractmethod
    def smax_unsubscribe(self, pattern):
        pass

    @abstractmethod
    def smax_wait_on_subscribed(self, pattern):
        pass

    @abstractmethod
    def smax_wait_on_any_subscribed(self):
        pass

    @abstractmethod
    def smax_set_description(self, table, description):
        pass

    @abstractmethod
    def smax_get_description(self, table):
        pass

    @abstractmethod
    def smax_set_units(self, table, unit):
        pass

    @abstractmethod
    def smax_get_units(self, table):
        pass

    @abstractmethod
    def smax_set_coordinate_system(self, table, coordinate_system):
        pass

    @abstractmethod
    def smax_get_coordinate_system(self, table):
        pass

    @abstractmethod
    def smax_create_coordinate_system(self, n_axis):
        pass

    @abstractmethod
    def smax_push_meta(self, meta, table, value):
        pass

    @abstractmethod
    def smax_pull_meta(self, table, meta):
        pass

def join(*args):
    """Join SMA-X tables and keys.
    
    params:
        *args : SMA-X key elements to join
    """
    return ":".join(args)

def normalize_pair(*args):
    """Return a SMA-X table, key pair with exactly one level in the key
    
    params:
        *args : SMA-X key elements to join and split
    """
    full_key = join(*args)
    return full_key.rsplit(":", maxsplit=1)

def print_tree(d, verbose, indent=0):
    """Walk through a tree of SMA-X values, printing the leaf nodes"""
    if indent != 0:
        indent_str = " "*(indent)
    else:
        indent_str = ""
    for k, i in d.items():
        if type(i) is dict:
            print(indent_str, k, sep="")
            print_tree(i, verbose, indent + 4)
        else:
            print_smax(i, verbose, indent)
            
def print_smax(smax_value, verbose, indent=0):
    """Print a SMA-X value"""
    if indent != 0:
        indent_str = " "*(indent)
    else:
        indent_str = ""
    if verbose:
        print(indent_str, f"SMA-X value {smax_value.smaxname} :", sep="")
        prefix = "    data   : "
    else:
        prefix = f"{smax_value.smaxname.split(':')[-1]} : "
    if smax_value.type == str:
        if smax_value.dim == 1:
            print(indent_str, prefix, smax_value.data, sep="")
        else:
            for l in smax_value.data:
                print(indent_str, prefix, l, sep="")
                prefix = " "*len(prefix)
    else:
        for l in str(smax_value.data).splitlines():
            print(indent_str, prefix, l, sep="")
            prefix = " "*len(prefix)
    if verbose:
        print(indent_str, f"    type   : {_REVERSE_TYPE_MAP[smax_value.type]}", sep="")
        print(indent_str, f"    dim    : {smax_value.dim}", sep="")
        print(indent_str, f"    date   : {datetime.datetime.utcfromtimestamp(smax_value.date)}", sep="")
        print(indent_str, f"    origin : {smax_value.origin}", sep="")
        print(indent_str, f"    seq    : {smax_value.seq}", sep="")
        
# Lookup tables for converting python types to smax type names.
_TYPE_MAP = {
             'integer': int,
             'int': int,
             'int16': np.int16,
             'int32': np.int32,
             'int64': np.int64,
             'int8': np.int8,
             'float': float,
             'float32': np.float32,
             'float64': np.float64,
             'str': str,
             'string': str
             }
_REVERSE_TYPE_MAP = inv_map = {v: k for k, v in _TYPE_MAP.items()}
