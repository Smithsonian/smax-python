import datetime

import pytest

import numpy as np

from smax.smax_data_types import SmaxData, \
    SmaxFloat, SmaxFloat32, SmaxFloat64, SmaxInt, SmaxInt8, SmaxInt16, SmaxInt32, SmaxInt64, \
    SmaxBool, SmaxArray, SmaxStr, SmaxStrArray, SmaxBytes, SmaxStruct, \
    _TYPE_MAP, _REVERSE_TYPE_MAP, _SMAX_TYPE_MAP, _REVERSE_SMAX_TYPE_MAP

class TestTypeLookup:
    """Tests of _TYPE_MAP"""
    def test_smax_to_int8(self):
        smax_type = 'int8'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.int8
    
    def test_smax_to_int16(self):
        smax_type = 'int16'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.int16
        
    def test_smax_to_int32(self):
        smax_type = 'int32'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.int32
        
    def test_smax_to_int64(self):
        smax_type = 'int64'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.int64
    
    def test_smax_to_int(self):
        smax_type = 'int'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.int32
        
    def test_smax_to_integer(self):
        smax_type = 'integer'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.int32
        
    def test_smax_to_float32(self):
        smax_type = 'float32'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.float32
        
    def test_smax_to_float64(self):
        smax_type = 'float64'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.float64
        
    def test_smax_to_float(self):
        smax_type = 'float'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.float64
        
    def test_smax_to_single(self):
        smax_type = 'single'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.float32
        
    def test_smax_to_double(self):
        smax_type = 'double'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == np.float64
        
    def test_smax_to_string(self):
        smax_type = 'string'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == str
        
    def test_smax_to_str(self):
        smax_type = 'str'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == str
        
    def test_smax_to_boolean(self):
        smax_type = 'boolean'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == bool
        
    def test_smax_to_bool(self):
        smax_type = 'bool'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == bool
        
    def test_smax_to_bytes(self):
        smax_type = 'raw'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == bytes
        
class TestReverseTypeLookup:
    """Tests of _TYPE_MAP and _REVERSE_TYPE_MAP."""
    def test_int_to_smax(self):
        a = 42
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'int32'

    def test_npint8_to_smax(self):
        a = np.int8(42)
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'int8'

    def test_npint16_to_smax(self):
        a = np.int16(42)
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'int16'

    def test_npint32_to_smax(self):
        a = np.int32(42)
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'int32'

    def test_npint64_to_smax(self):
        a = np.int64(42)
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'int64'

    def test_float_to_smax(self):
        a = 3.141259
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'float64'

    def test_float32_to_smax(self):
        a = np.float32(3.141259)
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'float32'

    def test_float64_to_smax(self):
        a = np.float64(3.141259)
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'float64'

    def test_str_to_smax(self):
        a = 'spam'
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'string'
        
    def test_bool_to_smax(self):
        a = True
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'boolean'
        
    def test_bytes_to_smax(self):
        a = b'42'
        
        smax_type = _REVERSE_TYPE_MAP[type(a)]
        
        assert smax_type == 'raw'

class TestSMAXTypeLookup:
    """Tests of _TYPE_MAP"""
    def test_smax_to_int8(self):
        smax_type = 'int8'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxInt8
    
    def test_smax_to_int16(self):
        smax_type = 'int16'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxInt16
        
    def test_smax_to_int32(self):
        smax_type = 'int32'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxInt32
        
    def test_smax_to_int64(self):
        smax_type = 'int64'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxInt64
    
    def test_smax_to_int(self):
        smax_type = 'int'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxInt32
        
    def test_smax_to_integer(self):
        smax_type = 'integer'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxInt32
        
    def test_smax_to_float32(self):
        smax_type = 'float32'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxFloat32
        
    def test_smax_to_float64(self):
        smax_type = 'float64'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxFloat64
        
    def test_smax_to_float(self):
        smax_type = 'float'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxFloat64
        
    def test_smax_to_single(self):
        smax_type = 'single'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxFloat32
        
    def test_smax_to_double(self):
        smax_type = 'double'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxFloat64
        
    def test_smax_to_string(self):
        smax_type = 'string'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxStr
        
    def test_smax_to_str(self):
        smax_type = 'str'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxStr
        
    def test_smax_to_boolean(self):
        smax_type = 'boolean'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxBool
        
    def test_smax_to_bool(self):
        smax_type = 'bool'
        
        var_type = _SMAX_TYPE_MAP[smax_type]
        
        assert var_type == SmaxBool
        
    def test_smax_to_bytes(self):
        smax_type = 'raw'
        
        var_type = _TYPE_MAP[smax_type]
        
        assert var_type == SmaxBytes

class TestReverseSMAXTypeLookup:
    """Tests of _SMAX_TYPE_MAP and _REVERSE_SMAX_TYPE_MAP."""
    def test_int_to_smax(self):
        a = SmaxInt(42)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'int32'

    def test_npint8_to_smax(self):
        a = SmaxInt8(42)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'int8'

    def test_npint16_to_smax(self):
        a = SmaxInt16(42)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'int16'

    def test_npint32_to_smax(self):
        a = SmaxInt32(42)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'int32'

    def test_npint64_to_smax(self):
        a = SmaxInt64(42)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'int64'

    def test_float_to_smax(self):
        a = SmaxFloat(3.141259)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'float64'

    def test_float32_to_smax(self):
        a = SmaxFloat32(3.141259)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'float32'

    def test_float64_to_smax(self):
        a = SmaxFloat64(3.141259)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'float64'

    def test_str_to_smax(self):
        a = SmaxStr('spam')
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'string'
        
    def test_bool_to_smax(self):
        a = SmaxBool(True)
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'boolean'
        
    def test_bytes_to_smax(self):
        a = SmaxBytes('42')
        
        smax_type = _REVERSE_SMAX_TYPE_MAP[type(a)]
        
        assert smax_type == 'raw'

class TestSmaxFloat:
    """Tests of SmaxFloat"""
    def test_equality(self):
        a = 3.141259
        b = SmaxFloat(a)
        
        assert a == pytest.approx(b)
        
    def test_metadata(self):
        a = 3.141259
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat:pi"
        desc = "A test SmaxFloat"
        unit = "V"
        
        b = SmaxFloat(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=desc, unit=unit)
        
        assert a == pytest.approx(b)
        assert a == pytest.approx(b.data)
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "float" == b.type
        assert 1 == b.dim
        assert desc == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = 3.1
        c = 1.2
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat:pi"
        desc = "A test SmaxFloat"
        unit = "V"
        
        b = SmaxFloat(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=desc, unit=unit)
        
        assert b+c == pytest.approx(a+c)
        assert c+b == pytest.approx(c+a)
        
        assert type(c+b) is float
    
    
class TestSmaxFloat32:
    """Tests of SmaxFloat32"""
    def test_equality(self):
        a = np.float32(3.141259)
        b = SmaxFloat32(a)
        assert a == pytest.approx(b)
        
    def test_metadata(self):
        a = np.float32(3.141259)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat32:pi"
        description = "A test SmaxFloat32"
        unit = "V"
        
        b = SmaxFloat32(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == pytest.approx(b)
        assert a == pytest.approx(b.data)
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "float32" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = np.float32(3.1)
        c = np.float32(1.2)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat32:pi"
        description = "A test SmaxFloat32"
        unit = "V"
        
        b = SmaxFloat32(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == pytest.approx(a+c)
        assert c+b == pytest.approx(c+a)
        
        assert type(c+b) is np.float32
        

class TestSmaxFloat64:
    """Tests of SmaxFloat64"""
    def test_equality(self):
        a = np.float64(3.141259)
        b = SmaxFloat64(a)
        
        assert a == pytest.approx(b)
        
    def test_metadata(self):
        a = np.float64(3.141259)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat32:pi"
        description = "A test SmaxFloat32"
        unit = "V"
        
        b = SmaxFloat64(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == pytest.approx(b)
        assert a == pytest.approx(b.data)
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "float64" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = np.float64(3.1)
        c = np.float64(1.2)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat32:pi"
        description = "A test SmaxFloat32"
        unit = "V"
        
        b = SmaxFloat64(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == pytest.approx(a+c)
        assert c+b == pytest.approx(c+a)
        
        assert type(c+b) is np.float64
            

class TestSmaxInt:
    def test_equality(self):
        a = 3
        b = SmaxInt(a)
        
        assert a == b
        
    def test_compatibility(self):
        a = np.int32(2**16+2**10)
        b = SmaxInt(a)
        
        assert a == b
        
    def test_metadata(self):
        a = 3
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint:pi"
        description = "A test SmaxInt"
        unit = "Quahogs"
        
        b = SmaxInt(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "integer" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = 3
        c = 1
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint:pi"
        description = "A test SmaxInt"
        unit = "Quahogs"
        
        b = SmaxInt(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == a+c
        assert c+b == c+a
        
        assert type(c+b) is int
            
            
class TestSmaxInt8:
    """Tests of SmaxInt8"""
    def test_equality(self):
        a = np.int8(3)
        b = SmaxInt8(a)
        
        assert a == b
        
    def test_metadata(self):
        a = np.int8(3)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint8:pi"
        description = "A test SmaxInt8"
        unit = "Quahogs"
        
        b = SmaxInt8(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "int8" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = np.int8(3)
        c = np.int8(1)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint8:pi"
        description = "A test SmaxInt8"
        unit = "Quahogs"
        
        b = SmaxInt8(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == a+c
        assert c+b == c+a
        
        assert type(c+b) is np.int8
            

class TestSmaxInt16:
    """Tests of SmaxInt16"""
    def test_equality(self):
        a = np.int16(3)
        b = SmaxInt16(a)
        
        assert a == b
        
    def test_metadata(self):
        a = np.int16(3)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint16:pi"
        description = "A test SmaxInt16"
        unit = "Quahogs"
        
        b = SmaxInt16(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "int16" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = np.int16(3)
        c = np.int16(1)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint16:pi"
        description = "A test SmaxInt16"
        unit = "Quahogs"
        
        b = SmaxInt16(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == a+c
        assert c+b == c+a
        
        assert type(c+b) is np.int16
        
            
class TestSmaxInt32:
    """Tests of SmaxInt32"""
    def test_equality(self):
        a = np.int32(3)
        b = SmaxInt32(a)
        
        assert a == b
        
    def test_metadata(self):
        a = np.int32(3)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint32:pi"
        description = "A test SmaxInt32"
        unit = "Quahogs"
        
        b = SmaxInt32(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "int32" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = np.int32(3)
        c = np.int32(1)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint32:pi"
        description = "A test SmaxInt32"
        unit = "Quahogs"
        
        b = SmaxInt32(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == a+c
        assert c+b == c+a
        
        assert type(c+b) is np.int32
            

class TestSmaxInt64:
    """Tests of SmaxInt64"""
    def test_equality(self):
        a = np.int64(3)
        b = SmaxInt64(a)
        
        assert a == b
        
    def test_metadata(self):
        a = np.int64(3)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint64:pi"
        description = "A test SmaxInt64"
        unit = "Quahogs"
        
        b = SmaxInt64(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "int64" == b.type
        assert 1 == b.dim
        assert description == b.description
        assert unit == b.unit
        
    def test_addition(self):
        a = np.int64(3)
        c = np.int64(1)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint64:pi"
        description = "A test SmaxInt64"
        unit = "Quahogs"
        
        b = SmaxInt64(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description, unit=unit)
        
        assert b+c == a+c
        assert c+b == c+a
        
        assert type(c+b) is np.int64
        

class TestSmaxBool:
    """Tests of SmaxBool"""
    def test_equality(self):
        a = True
        b = SmaxBool(True)
        
        assert a == b
        
        c = False
        d = SmaxBool(False)
        
        assert c == d
        
    def test_conversion_from_int(self):
        a = True
        b = SmaxBool(int(a))
        
        assert a == b
        
        c = False
        d = SmaxBool(int(c))
        
        assert c == d
        
    def test_conversion_from_strint(self):
        a = True
        b = SmaxBool(str(int(a)))
        
        assert a == b
        
        c = False
        d = SmaxBool(str(int(c)))
        
        assert c == d

    def test_boolean_array_conversion(self):
        data = [False, 'False', 'false', 0, 'f', 'F', b'F']
        expected_data = np.array([False, False, False, False, False, False, False])
        expected_type = "boolean"
        expected_dim = len(data)
        table = "test_roundtrip_bool_list"
        key = "pytest"
        result = SmaxArray(data, type='boolean')
        assert np.array_equal(result.data, expected_data)
        assert result.type == expected_type
        assert result.dim == expected_dim
        
        data = [True, 'True', 'true', 1, 't', 'T', b't']
        expected_data = np.array([True, True, True, True, True, True, True])
        expected_type = "boolean"
        expected_dim = len(data)
        table = "test_roundtrip_bool_list"
        key = "pytest"
        result = SmaxArray(data, type='boolean')
        assert np.array_equal(result.data, expected_data)
        assert result.type == expected_type
        assert result.dim == expected_dim
        
    def test_boolean_2d_array_conversion(self):
        data = [[False, 'False', 'false'], [0, 'f', 'F']]
        expected_data = np.array([[False, False, False], [False, False, False]])
        expected_type = "boolean"
        expected_dim = expected_data.shape
        table = "test_roundtrip_bool_list"
        key = "pytest"
        result = SmaxArray(data, type='boolean', dim=expected_dim)
        assert np.array_equal(result.data, expected_data)
        assert result.type == expected_type
        assert result.dim == expected_dim
        
        data = [[True, 'True', 'true'], [1, 't', 'T']]
        expected_data = np.array([[True, True, True], [True, True, True]])
        expected_type = "boolean"
        expected_dim = expected_data.shape
        table = "test_roundtrip_bool_list"
        key = "pytest"
        result = SmaxArray(data, type='boolean', dim=expected_dim)
        assert np.array_equal(result.data, expected_data)
        assert result.type == expected_type
        assert result.dim == expected_dim
        
        
    def test_metadata(self):
        a = True
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxbool"
        description = "A test SmaxBool"
        
        b = SmaxBool(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "boolean" == b.type
        assert 1 == b.dim
        assert description == b.description
        
    def test_metadata_str(self):
        a = True
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxbool"
        description = "A test SmaxBool"
        
        b = SmaxBool(str(a), timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "boolean" == b.type
        assert 1 == b.dim
        assert description == b.description
        
    def test_logical_or(self):
        a = False
        c = False
        
        b = SmaxBool(a)
        
        assert (b or c) == (a or c)
        assert (c or b) == (c or a)
        
        assert (c or b) == False
        assert not (c or b) == True
        
    def test_logical_and(self):
        a = True
        c = False
        
        b = SmaxBool(a)
        
        assert (b and c) == (a and c)
        assert (c and b) == (c and a)
        
        assert (c and b) == False
        assert not(c and b) == True
        
    def test_logical_xor(self):
        a = True
        c = False
        
        b = SmaxBool(a)
        
        assert (b ^ c) == (a ^ c)
        assert (c ^ b) == (c ^ a)
        
        assert (c ^ b) == True
        assert (b ^ b) == False
        

class TestSmaxStr:
    """Tests of SmaxStr"""
    def test_equality(self):
        a = "A SMA-X string"
        b = SmaxStr(a)
        
        assert a == b

    def test_metadata(self):
        a = "A SMA-X string"
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxstr"
        description = "A test SmaxStr"
        
        b = SmaxStr(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "string" == b.type
        assert 1 == b.dim
        assert description == b.description
        
    def test_str_concat(self):
        a = "A SMA-X string"
        c = "Another string"
        
        b = SmaxStr(a)
        
        assert b + c == a + c
        assert c + b == c + a
    
        assert type(c + b) is str
        
    def test_str_method(self):
        a = "A SMA-X string"
        
        b = SmaxStr(a)
        
        assert b.lower() == a.lower()
        
        assert type(b.lower()) is str
        

class TestSmaxBytes:
    """Tests of SmaxBytes"""
    def test_equality(self):
        a = b"A SMA-X raw bytes"
        b = SmaxBytes(a)
        
        assert a == b

    def test_metadata(self):
        a = b"A SMA-X string"
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxstr"
        description = "A test SmaxStr"
        
        b = SmaxBytes(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=description)
        
        assert a == b
        assert a == b.data
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "raw" == b.type
        assert 1 == b.dim
        assert description == b.description
        
        
class TestSmaxFloatArray:
    """Tests of SmaxArray"""
    def test_equality(self):
        shape = (5, 4)
        pi = 3.141259
        
        a = np.full(shape, pi, dtype=np.float64)
        print(a)
        b = SmaxArray(a)
        print(b)
        
        assert a == pytest.approx(b)
        
    def test_metadata(self):
        shape = (5, 4)
        pi = 3.141259
        
        a = np.full(shape, pi, dtype=np.float64)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloat:pi"
        desc = "A test SmaxFloat"
        unit = "V"
        
        b = SmaxArray(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, type='float64', description=desc, unit=unit)
        
        assert a == pytest.approx(b)
        assert a == pytest.approx(b.data)
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert "float64" == b.type
        # b.dim should be automatically created if not supplied
        assert shape == b.dim
        assert desc == b.description
        assert unit == b.unit
        
    def test_reshaping(self):
        shape = (5, 4)
        pi = 3.141259
        
        a = np.arange(1*pi, 20*pi, pi)
        
        timestamp = datetime.datetime.fromtimestamp(100000000)
        desc = "A test SmaxFloat"
        
        c = SmaxArray(a, timestamp=timestamp, dim=shape, description=desc)
        
        assert c.shape == shape
        assert c.dim == shape
        assert type(c) == SmaxArray
        
    def test_addition(self):
        shape = (5, 4)
        pi = 3.141259
        
        a = np.arange(0, 20*pi, pi)
        c = np.arange(0, 20*1/pi, 1/pi)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxfloatarray:pi"
        desc = "A test SmaxFloatArray"
        unit = "V"
        
        b = SmaxArray(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=desc, unit=unit)
        
        assert b+c == pytest.approx(a+c)
        assert c+b == pytest.approx(c+a)
        
        
class TestSmaxIntArray:
    """Tests of SmaxArray"""
    def test_equality(self):
        shape = (5, 4)
        pi = 3
        
        a = np.full(shape, pi, dtype=np.int16)
        print(a)
        b = SmaxArray(a)
        print(b)
        
        assert np.array_equal(a, b)
        
    def test_metadata(self):
        shape = (5, 4)
        pi = 3
        
        a = np.full(shape, pi)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxintarray:pi"
        desc = "A test SmaxIntArray"
        
        b = SmaxArray(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, type='integer', description=desc)
        
        assert np.array_equal(a, b)
        assert np.array_equal(a, b.data)
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert 'integer' == b.type
        # b.dim should be automatically created if not supplied
        assert shape == b.dim
        assert desc == b.description
        
    def test_reshaping(self):
        shape = (5, 4)
        pi = 3
        
        a = np.arange(1*pi, 20*pi, pi)
        
        timestamp = datetime.datetime.fromtimestamp(100000000)
        desc = "A test SmaxIntArray"
        
        c = SmaxArray(a, timestamp=timestamp, dim=shape, description=desc)
        
        assert c.shape == shape
        assert c.dim == shape
        assert type(c) == SmaxArray
        
    def test_addition(self):
        shape = (5, 4)
        pi = 3
        
        a = np.arange(0, 20*pi, pi)
        c = np.arange(20*pi, 0, -pi)
        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxint:pi"
        desc = "A test SmaxInt"
        
        b = SmaxArray(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=desc)
        
        assert np.array_equal(b+c, a+c)
        assert np.array_equal(c+b, c+a)
        

class TestSmaxStrArray:
    """Tests of SmaxStrArray"""
    def test_equality(self):
        a = ["Hello","I am a", "string array"]
        b = SmaxStrArray(a)
        
        assert a == b
        
    def test_metadata(self):
        a = ["Hello","I am a", "string array"]

        timestamp = datetime.datetime.fromtimestamp(100000000)
        origin = "pytest"
        seq = 2
        smaxname = "test:smaxstrarray"
        desc = "A test SmaxStrArray"
        
        b = SmaxStrArray(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, type='string', description=desc)
        
        for i, bb in enumerate(b):
            assert a[i] == bb
        assert timestamp == b.timestamp
        assert origin == b.origin
        assert seq == b.seq
        assert smaxname == b.smaxname
        assert 'string' == b.type
        # b.dim should be automatically created if not supplied
        assert len(a) == b.dim
        assert desc == b.description
        
    