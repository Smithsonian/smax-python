from typing import Any, Optional
from dataclasses import dataclass, InitVar, field, asdict
from datetime import datetime
from collections import namedtuple

import numpy as np

# Lookup tables for converting python types to smax type names.
#
# For the reverse conversion of Python types to SMA-X types
# (represented in SMA-X in their string form), the last
# occurence of the Python type as a value in _TYPE_MAP
# will be used to pair Python values of that type to a
# named SMA-X type (this is a consequence of the order of dictionaries
# being maintained in Python since 3.7).
#
# Thus in the current form of the _TYPE_MAP below, Python
# floats will be sent to SMA-X with the type 'float64' rather 
# than 'double' or 'float', even though Python floats are 
# double precision by default, and Python ints will be sent with
# the SMA-X type 'int32' rather than 'int'.
# 
# See the bottom of this file for the SmaxVar version of these maps
_TYPE_MAP = {
             'int': np.int32,
             'integer': np.int32,
             'int16': np.int16,
             'int32': np.int32,
             'int64': np.int64,
             'int8': np.int8,
             'single': np.float32,
             'double': np.float64,
             'float': np.float64,
             'float32': np.float32,
             'float64': np.float64,
             'bool': bool,
             'boolean': bool,
             'str': str,
             'string': str,
             'raw': bytes
             }

# The reverse mapping here relies on overwriting the non-standard
# SMA-X types with the standard types. 
_REVERSE_TYPE_MAP = {v: k for k, v in _TYPE_MAP.items()}

# Add standard Python types to the inverse map
_REVERSE_TYPE_MAP[int] = 'int32'
_REVERSE_TYPE_MAP[float] = 'float64'

inv_map = _REVERSE_TYPE_MAP

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

optional_metadata = [
    "description",
    "unit",
    "coords"
]

@dataclass
class SmaxVarBase(object):
    """Class defining the metadata for SMA-X data types.
    
    We define all the metadata except data and type here."""
    timestamp: datetime | None = field(kw_only=True, default = None)
    origin: str | None  = field(kw_only=True, default = None)
    seq: int = field(kw_only=True, default = 1)
    smaxname: str | None = field(kw_only=True, default=None)
    dim: int | tuple = field(kw_only=True, default=1)
    description: str | None = field(kw_only=True, default=None)
    unit: str | None = field(kw_only=True, default=None)
    coords: Any | None = field(kw_only=True, default=None)

    @property
    def data(self):
        return self

    @property
    def metadata(self):
        return self.__dict__

@dataclass
class SmaxFloat(float, SmaxVarBase):
    """Class for holding SMA-X float objects, with their metadata"""
    data: InitVar[float]
    type: str = field(kw_only=True, default='float')
    
    def __repr__(self):
        return str(float(self))
    
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
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
        
# For ints, we can't directly subclass the Python int type as they
# are unmutable.  
# So we have to go via an intermediate with an added .__new__ method
class UserBytes(bytes):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            args = (kwargs.pop('data'),)
        x = int.__new__(cls, args[0])
        return x
        
@dataclass
class SmaxBytes(UserBytes, SmaxVarBase):
    """Class for holding SMA-X raw objects, with their metadata"""
    data: InitVar[bytes]
    type: str = field(kw_only=True, default='bytes')

    def __repr__(self):
        return str(bytes(self))
        
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
        elif hasattr(args[0], 'shape'):
            shape = args[0].shape
        else:
            shape = len(args[0])
            
        if 'type' in kwargs.keys():
            datatype = kwargs['type']
            dtype = _TYPE_MAP[datatype]
        elif type(args[0]) is np.ndarray:
            dtype = args[0].dtype
        else:
            dtype = type(args[0][0])
        
        if dtype is bool:
            invar = np.array(args[0], dtype='O')
            initvar = np.ndarray(invar.shape, dtype='bool')
            for i, a in enumerate(invar.flat):
                initvar.flat[i] = _to_bool(a)
                
        else:
            initvar = np.array(args[0], copy=True)
            if type(shape) is not int:
                if len(shape) > 1:
                    initvar.resize(shape, refcheck=False)
        
        x = np.array(initvar, dtype=dtype).view(cls).copy()
        
        return x

@dataclass
class SmaxArray(UserArray, SmaxVarBase):
    """Class for holding SMA-X array objects, with their metadata"""
    data: InitVar[np.ndarray | list]
    # Don't set type during __post_init__, as Python floats will get
    # converted to np.float64 for array storage.
    # Instead, maintain the SMA-X type.
    type: str | None = field(kw_only=True, default=None)
        
    def __post_init__(self, *args, **kwargs):
        try:
            for f in self.__dataclass_fields__.keys():
                if f != 'data':
                    setattr(self, f, getattr(self, f))
        except AttributeError:
            pass
        
        if len(self.shape) == 1:
            self.dim = self.shape[0]
        else:
            self.dim = self.shape
        
    def __repr__(self):
        return super().__repr__()
    
    @property    
    def data(self):
        return self
    
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
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic
        
# bool can't be subclassed in Python, so we do some hacking to
# make ourselves our own boolean class.
#
# This won't work exactly as bool, e.g. with type testing, but
# should be correct in all the normal uses of bool
def _to_bool(a):
    """Convert a value to bool, according to SMA-X rules.
    
    Boolean values can be stored as 'T', 'F', 't', 'f', 
    'True', 'False', 'true', 'false', '0', '1', 0, or 1.
    
    Returns 1 or 0, which is then converted to bool."""
    if type(a) is bytes:
        a = a.decode('utf-8')
        
    if type(a) is str:
        if a.lower().startswith('t'):
            b = 1
        elif a.lower().startswith('f'):
            b = 0
        elif a == '0':
            b = 0
        else:
            try:
                if int(float(a)):
                    b = 1
                else:
                    b = 0
            except:
                b = 1
    elif a:
        b = 1
    else:
        b = 0
    
    return b

class UserBool(int):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            initvar = kwargs.pop('data')
        else:
            initvar = args[0]
            
        arg = _to_bool(initvar)
        
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
        
    def asdict(self):
        dic = {'data':self}
        dic.update(asdict(self))
        
        return dic

# Look up table for SMA-X data type maps
_SMAX_TYPE_MAP = {
             'int': SmaxInt32,
             'integer':SmaxInt32,
             'int16': SmaxInt16,
             'int32': SmaxInt32,
             'int64': SmaxInt64,
             'int8': SmaxInt8,
             'float': SmaxFloat64,
             'single': SmaxFloat32,
             'double': SmaxFloat64,
             'float32': SmaxFloat32,
             'float64': SmaxFloat64,
             'str': SmaxStr,
             'string': SmaxStr,
             'bool': SmaxBool,
             'boolean': SmaxBool,
             'raw': SmaxBytes}

# The reverse mapping here relies on overwriting the non-standard
# SMA-X type reverse maps with the standard types 
_REVERSE_SMAX_TYPE_MAP = {v: k for k, v in _SMAX_TYPE_MAP.items()}

# Add the SMAX subclasses of standard python types
_REVERSE_SMAX_TYPE_MAP[SmaxInt] = 'int32'
_REVERSE_SMAX_TYPE_MAP[SmaxFloat] = 'float64'

inv_smax_map = _REVERSE_SMAX_TYPE_MAP