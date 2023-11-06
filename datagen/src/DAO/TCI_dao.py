class TCI_dao(object):
    
    schema_str = """{
    "$id": "http://example.com/myURI.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "additionalProperties": false,
    "description": "Sample schema to help you get started.",
    "properties": {
        "myField1": {
        "description": "The integer type is used for integral numbers.",
        "type": "integer"
        },
        "myField2": {
        "description": "The number type is used for any numeric type, either integers or floating point numbers.",
        "type": "number"
        },
        "myField3": {
        "description": "The string type is used for strings of text.",
        "type": "string"
        }
    },
    "title": "SampleRecord",
    "type": "object"
    }"""
