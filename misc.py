import json
import datetime

class A:
    def __init__(self):
        self.a = ''
        self.b = ''
        self.c=AA()
        self.d=datetime.datetime.now()

class AA:
    def __init__(self):
        self.c='fddsfsdf'

A.JsonFields = ['a', 'b', 'c', 'd:dt']
AA.JsonFields = ['c']

CLASS_MAP = {A.__name__: A, AA.__name__: AA}

class B:
    def __init__(self):
        self.a = ''
        self.b = ''

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj.__class__, 'JsonFields'):
            fields = obj.__class__.JsonFields
            d = {'__class__': obj.__class__.__name__}
            for fld in fields:
                assert isinstance(fld, str)
                pos = fld.find(':')
                if pos > 0:
                    fld = fld[0:pos]
                if hasattr(obj, fld):
                    d[fld] = getattr(obj, fld)
            return d
        elif isinstance(obj, datetime.datetime):
            return obj.timestamp()
        else:
            return obj.__dict__


class CustomDecoder(json.JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=self.fdms_object_hook)

    @staticmethod
    def fdms_object_hook(d):
        obj = None
        if '__class__' in d:
            class_name = d['__class__']
            if class_name in CLASS_MAP:
                obj = CLASS_MAP[class_name]()
        if obj is None:
            return d
        if not hasattr(obj.__class__, 'JsonFields'):
            return d

        for fld in obj.__class__.JsonFields:
            assert isinstance(fld, str)
            pos = fld.find(':')
            type = None
            if pos > 0:
                type = fld[pos+1:]
                fld = fld[0:pos]
            if fld in d:
                value = d[fld]
                if type is not None:
                    if type == 'dt' and isinstance(value, float):
                        value = datetime.datetime.fromtimestamp(value)
                if hasattr(obj, fld):
                    setattr(obj, fld, value)

        return obj






a = A()
a.a = 'dfsdsfdsf'
a.b = 'fdsfdsfsdff'

j = json.dumps(a, cls=CustomEncoder)
print(j)

obj = json.loads(j, cls=CustomDecoder)
print(obj)