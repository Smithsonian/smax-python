from typing import Any, Optional
from dataclasses import dataclass, InitVar, field, asdict
from datetime import datetime
from collections import namedtuple

import numpy as np

# Lookup tables for converting python types to smax type names.
# The official SMA-X types are not currently given in the spec, but
# the following types have been seen in the wild.
#
# For the reverse conversion of Python types to SMA-X types
# (represented in SMA-X in their string form), the last
# occurence of the Python type as a value in _TYPE_MAP
# will be used to pair Python values of that type to a
# named SMA-X type (this is a consequency of the order of dictionaries
# being maintained in Python since 3.7).
#
# Thus in the current form of the _TYPE_MAP below, Python
# floats will be sent to SMA-X with the type 'float' rather 
# than 'double' or 'float64', even though Python floats are 
# double precision by default, and Python ints will be sent with
# the SMA-X type 'integer' rather than 'int'.
# 
# See the bottom of this file for the SmaxVar version of these maps
_TYPE_MAP = {
             'int': int,
             'integer': int,
             'int16': np.int16,
             'int32': np.int32,
             'int64': np.int64,
             'int8': np.int8,
             'double': float,
             'float': float,
             'float32': np.float32,
             'float64': np.float64,
             'bool': bool,
             'boolean': bool,
             'str': str,
             'string': str
             }

_REVERSE_TYPE_MAP = inv_map = {v: k for k, v in _TYPE_MAP.items()}

# Legacy Named tuples for smax requests and responses.
#
# This namedtuple method was originally used for storing SMA-X values
# and their metadata.  However, it does not allow for additional metadata values
# that may be defined, and is somewhat awkward to use, as the value can only
# be directly used in Python by calling SmaxData.data.
SmaxData = namedtuple("SmaxData", "data type dim date origin seq smaxname")

# Smax<Type> dataclasses that inherit from Python native types
# 
# Each dataclass behaves as the base Python type, but additionally
# includes properties from the SMA-X <meta> tables
# 
# A .data property is included for backwards compatibility with the SmaxData
# named tuple above.

@dataclass
class SmaxVarBase(object):
    """Class defining the metadata for SMA-X data types.
    
    We define all the metadata except data and type here."""
    timestamp: datetime | None = field(kw_only=True, default = None)
    origin: str | None  = field(kw_only=True, default = None)
    seq: int = field(kw_only=True, default = 1)
    smaxname: str | None = field(kw_only=True, default=None)
    dim: int = field(kw_only=True, default=1)
    description: str | None = field(kw_only=True, default=None)
    unit: str | None = field(kw_only=True, default=None)
    coords: Any | None = field(kw_only=True, default=None)

@dataclass
class SmaxFloat(float, SmaxVarBase):
    """Class for holding SMA-X float objects, with their metadata"""
    data: InitVar[float]
    type: str = field(kw_only=True, default='float')
    
    def __repr__(self):
        return str(float(self))
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

# For ints, we can't directly subclass the Python int type as they
# are unmutable.  
# So we have to go via an intermediate with an added .__new__ method
class UserInt(int):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = int.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxInt(UserInt, SmaxVarBase):
    """Class for holding SMA-X integer objects, with their metadata"""
    data: InitVar[int]
    type: str = field(kw_only=True, default='integer')

    def __repr__(self):
        return str(int(self))
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        print(self.__dict__)
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
        

class UserStr(str):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = str.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxStr(UserStr, SmaxVarBase):
    """Class for holding SMA-X string objects, with their metadata"""
    data: InitVar[str]
    type: str = field(kw_only=True, default='string')
    
    def __repr__(self):
        return str(self)

    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

class UserDict(dict):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = dict.__new__(cls, args[0])
        return x
    
class UserList(list):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = list.__new__(cls, args[0])
        return x

@dataclass
class SmaxStrArray(UserList, SmaxVarBase):
    """Class for holding SMA-X string arrays, with their metadata"""
    data: InitVar[list]
    type: str = field(kw_only=True, default='string')
    
    def __post_init__(self, *args, **kwargs):
        super().__init__(*args)
        if 'dim' not in kwargs.keys():
            self.dim = len(args[0])
        
    def __repr__(self):
        return super().__repr__()

    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

@dataclass
class SmaxStruct(UserDict, SmaxVarBase):
    """Class for holding SMA-X string objects, with their metadata"""
    data: InitVar[dict]
    type: str = field(kw_only=True, default='struct')
    
    def __post_init__(self, *args):
        super().__init__(*args)
        
    def __repr__(self):
        return super().__repr__()

    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
    
class UserArray(np.ndarray):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        
        if 'dim' in kwargs.keys():
            shape = kwargs['dim']
        else:
            shape = None
        
        if 'type' in kwargs.keys():
            datatype = kwargs['type']
            dtype = _TYPE_MAP[datatype]
        else:
            dtype = None
            
        x = np.array(args[0], dtype=dtype, subok=True).view(cls)
        if shape:
            x.resize(shape)
        
        return x


@dataclass
class SmaxArray(UserArray, SmaxVarBase):
    """Class for holding SMA-X array objects, with their metadata"""
    data: InitVar[np.ndarray | list]
    type: str | None = field(kw_only=True, default=None)
        
    def __repr__(self):
        return super().__repr__()
    
    def __array_finalize__(self, obj):
        print("In array_finalize")
        try:
            for f in obj.__dataclass_fields__.keys():
                if f != 'data':
                    setattr(self, f, getattr(obj, f))
        except AttributeError:
            pass
        
        self.dim = self.shape
        self.type = _REVERSE_TYPE_MAP[self.dtype.type]
        
        
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic


class UserFloat32(np.float32):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = np.float32.__new__(cls, args[0])
        return x
    
@dataclass
class SmaxFloat32(UserFloat32, SmaxVarBase):
    """Class for holding SMA-X float objects, with their metadata"""
    data: InitVar[float | np.float32 ]
    type: str = field(kw_only=True, default='float32')
    
    def __repr__(self):
        return str(self)
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
    
class UserFloat64(np.float64):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = np.float64.__new__(cls, args[0])
        return x
    
@dataclass
class SmaxFloat64(UserFloat64, SmaxVarBase):
    """Class for holding SMA-X float objects, with their metadata"""
    data: InitVar[float | np.float32 | np.float64 ]
    type: str = field(kw_only=True, default='float64')
    
    def __repr__(self):
        return str(self)
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        return self.__dict__
    
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
    
class UserInt8(np.int8):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = np.int8.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxInt8(UserInt8, SmaxVarBase):
    """Class for holding SMA-X integer objects, with their metadata"""
    data: InitVar[np.int8]
    type: str = field(kw_only=True, default='int8')
    
    def __repr__(self):
        return str(int(self))
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        print(self.__dict__)
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

class UserInt16(np.int16):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = np.int16.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxInt16(UserInt16, SmaxVarBase):
    """Class for holding SMA-X integer objects, with their metadata"""
    data: InitVar[int | np.int8 | np.int16]
    type: str = field(kw_only=True, default='int16')
    
    def __repr__(self):
        return str(int(self))
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        print(self.__dict__)
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

class UserInt32(np.int32):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = np.int32.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxInt32(UserInt32, SmaxVarBase):
    """Class for holding SMA-X integer objects, with their metadata"""
    data: InitVar[int | np.int8 | np.int16 | np.int32]
    type: str = field(kw_only=True, default='int32')
    
    def __repr__(self):
        return str(int(self))
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        print(self.__dict__)
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

class UserInt64(np.int64):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = np.int64.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxInt64(UserInt64, SmaxVarBase):
    """Class for holding SMA-X integer objects, with their metadata"""
    data: InitVar[int | np.int8 | np.int16 | np.int32 | np.int64]
    type: str = field(kw_only=True, default='int64')
    
    def __repr__(self):
        return str(int(self))
    
    @property
    def data(self):
        return self
    
    @property
    def metadata(self):
        print(self.__dict__)
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
        
# bool can't be subclassed in Python, so we do some hacking to
# make ourselves our own boolean class.
#
# This won't work exactly as bool, e.g. with type testing, but
# should be correct in all the normal uses of bool
class UserBool(int):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'))
        if type(args[0]) is str:
            if args[0].lower().startswith('t'):
                arg = 1
            else:
                arg = 0
        elif args[0]:
            arg = 1
        else:
            arg = 0
        x = int.__new__(cls, arg)
        return x
    
    def __repr__(self):
        if self:
            return "True"
        else:
            return "False"
    __str__ = __repr__
    
    def __and__(self, other):
        if isinstance(other, bool):
            return bool(int(self) & int(other))
        else:
            return int.__and__(self, other)
        
    __rand__ = __and__
    
    def __or__(self, other):
        if isinstance(other, bool):
            return bool(int(self) | int(other))
        else:
            return int.__or__(self, other)

    __ror__ = __or__

    def __xor__(self, other):
        if isinstance(other, bool):
            return bool(int(self) ^ int(other))
        else:
            return int.__xor__(self, other)

    __rxor__ = __xor__

        
@dataclass
class SmaxBool(UserBool, SmaxVarBase):
    """Class for holding SMA-X boolean objects, with their metadata.
    
    This class is not an exact subclass for bool - e.g.: 
        In [1]: SmaxBool(True) is True
        Out[1]: False
    """
    data: InitVar[bool]
    type: str = field(kw_only=True, default='boolean')

    def __repr__(self):
        return super().__repr__()
    
    @property
    def data(self):
        if self == 0:
            return False
        else:
            return True
    
    @property
    def metadata(self):
        print(self.__dict__)
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

# Look up table for SMA-X data type maps
_SMAX_TYPE_MAP = {
             'int': SmaxInt,
             'integer': SmaxInt,
             'int16': SmaxInt16,
             'int32': SmaxInt32,
             'int64': SmaxInt64,
             'int8': SmaxInt8,
             'double' : SmaxFloat,
             'float': SmaxFloat,
             'float32': SmaxFloat32,
             'float64': SmaxFloat64,
             'str': SmaxStr,
             'string': SmaxStr,
             'bool': SmaxBool,
             'boolean': SmaxBool}
_REVERSE_SMAX_TYPE_MAP = inv_smax_map = {v: k for k, v in _SMAX_TYPE_MAP.items()}