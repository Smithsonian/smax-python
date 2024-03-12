import datetime

import pytest

import numpy as np

from smax.smax_data_types import SmaxData, \
    SmaxFloat, SmaxFloat32, SmaxFloat64, SmaxInt, SmaxInt8, SmaxInt16, SmaxInt32, SmaxInt64, \
    SmaxBool, SmaxArray, SmaxStr, SmaxStrArray, SmaxStruct
    
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
        smaxname = "test:smaxfloat:pi"
        desc = "A test SmaxFloat"
        unit = "V"
        
        b = SmaxArray(a, timestamp=timestamp, origin=origin, seq=seq, smaxname=smaxname, description=desc, unit=unit)
        
        assert b+c == pytest.approx(a+c)
        assert c+b == pytest.approx(c+a)