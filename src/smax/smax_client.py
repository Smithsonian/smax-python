import datetime
from abc import ABC, abstractmethod
import numpy as np


from .smax_data_types import SmaxData, \
    SmaxInt, SmaxFloat, SmaxBool, SmaxStr, SmaxStrArray, SmaxArray, SmaxStruct, \
    SmaxInt8, SmaxInt16, SmaxInt32, SmaxInt64, SmaxFloat32, SmaxFloat64, \
    _TYPE_MAP, _REVERSE_TYPE_MAP, _SMAX_TYPE_MAP, _REVERSE_SMAX_TYPE_MAP, \
    optional_metadata

# The separator used in key names
smax_sep = ':'
    
class SmaxConnectionError(RuntimeError):
    pass
    
class SmaxKeyError(RuntimeError):
    pass

class SmaxUnderflowWarning(RuntimeWarning):
    pass

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
    """Join SMA-X tables and keys.  We use this method to
    avoid the TypeErrors from string.join()
    
    params:
        *args : SMA-X key elements to join
    """
    if len(args) == 1:
        if args[0].startswith(smax_sep):
            args[0] = args[0][1:]
        return str(args[0])
    elif len(args) == 0:
        return None
    else:
        out = ""
        for a in args:
            if a is None or a == '':
                pass
            else:
                if type(a) is not str:
                    a = str(a)
                if a.startswith(smax_sep):
                    a = a[1:]
                out += f'{a}{smax_sep}'
        return out[:-1]


def normalize_pair(*args):
    """Return a SMA-X table, key pair with exactly one level in the key
    
    params:
        *args : SMA-X key elements to join and split
    """
    full_key = join(*args)
    table_key = full_key.rsplit(":", maxsplit=1)
    if len(table_key) == 1:
        table_key = ["", table_key[0]]
    return table_key


def print_tree(d, verbose=False, indent=0):
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


def print_smax(smax_value, verbose=False, indent=0):
    """Print a SMA-X value"""
    if indent != 0:
        indent_str = " "*(indent)
    else:
        indent_str = ""
    if verbose:
        if hasattr(smax_value, "smaxname"):
            if smax_value.smaxname is not None:
                print(indent_str, f"SMA-X value {smax_value.smaxname} :", sep="")
            else:
                print(indent_str, f"{type(smax_value).__name__} :", sep="")
        else:
            print(indent_str, f"{type(smax_value).__name__} :", sep="")
        prefix = "    data   : "
    else:
        if hasattr(smax_value, "smaxname"):
            if smax_value.smaxname is not None:
                prefix = smax_value.smaxname + ": "
            else:
                prefix = f"{type(smax_value).__name__}: "
        else:
            prefix = f"{type(smax_value).__name__}: "
    
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
        print(indent_str, f"    type   : {smax_value.type}", sep="")
        print(indent_str, f"    dim    : {smax_value.dim}", sep="")
        try:
            print(indent_str, f"    date   : {smax_value.timestamp}", sep="")
        except TypeError:
            print(indent_str, "    date   : None", sep="")
        print(indent_str, f"    origin : {smax_value.origin}", sep="")
        print(indent_str, f"    seq    : {smax_value.seq}", sep="")
        for meta in optional_metadata:
            if hasattr(smax_value, meta):
                if getattr(smax_value, meta) is not None:
                    print(indent_str, f"    {meta} : {getattr(smax_value, meta)}", sep="")
                    
        

