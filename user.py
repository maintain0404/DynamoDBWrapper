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
    def __init__(self, sk = '', request_type = 'read'):
        super().__init__(pk = "USER", sk = sk, request_type = request_type)

    def to_internal(self, new_data, nickname = '00000000'):
        try:
            self._data['pk'] = 'USER'
            self._data['sk'] = 'google#' + new_data['sub']
            self._data['User'] = 'nickname'
            self._data['ClosedUserData'] = {}
            if new_data['email_verified']:
                self._data['ClosedUserData']['UserEmail'] = new_data['email']
            else:
                self._data['ClosedUserData']['UserEmail'] = ''
            self._data['ClosedUserData']['UserName'] = new_data['name']
            self._data['ClosedUserData']['UserLocale'] = new_data['locale']
            self._data['ClosedUserData']['RiotID'] = []
        except Exception as err:
            raise ValidationError    