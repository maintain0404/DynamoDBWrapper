from .base import *
import jsonschema
import functools
import os

schema_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schemas')
user_input_schema_file = open(os.path.join(schema_directory, 'user_input.json'), encoding = 'UTF-8')
user_input_schema = json.loads(user_input_schema_file.read())
user_input_validator = functools.partial(jsonschema.validate, 
    schema = user_input_schema
    )


class User(BaseItemWrapper):
    pass