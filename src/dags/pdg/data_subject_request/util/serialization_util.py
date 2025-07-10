import json


# Serializes a list of class objects provided they support the to_dict method
# This is used we can pass a list of class objects through XCom
def serialize_list(items):
    return json.dumps([item.to_dict() for item in items])


# Deserializes a json string containing a list of objects by instantiating each
# one and adding them to a resulting list
def deserialize_list(json_str, cls):
    return [cls.from_dict(item) for item in json.loads(json_str)]
