import json
import boto3
from boto3.dynamodb.conditions import *
import os
from math import inf
from functools import partial, reduce
from datetime import datetime
from .dynamodb_settings import *

import secret_keys

dynamodb = boto3.resource('dynamodb',
    region_name = REGION_NAME, # 서울
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
)   

class ValidationError(Exception):
    def __init__(self):
        super().__init__('Invalid data')

class InvalidUpdateExpressionsError(Exception):
    def __init__(self):
        super().__init__('Invalid UpdateExpressions')

class RequestNotSetError(Exception):
    def __init__(self):
        super().__init__("Request is not set")

class SearchKey:
    keys = AVAILABLE_SEARCH_KEYS

    @classmethod
    def make_new(cls, data_type : str):
        type_prefix = cls.keys.get(data_type)
        if type_prefix:
            return type_prefix + datetime.now().strftime('%Y:%m:%d:%H:%M:%S:%f')
        else:
            raise ValueError('data_type "%s" is not valid data_type' % data_type)

class BaseItemWrapper:
    table = dynamodb.Table(TABLE_NAME)

    def __init__(self):
        self.request_type = None
        self.request = None

    @staticmethod
    def _init_value_key_generator(max_num: int = -1):
        count = 0
        if max_num < 0:
            max_num = inf    
        while count < max_num:
            count += 1
            yield reduce((lambda total, x: f'{total}{chr(int(x) + 65)}'), str(count), ':')
        
    def _add_update_expression(self, utype, path, value = None, overwrite = False):
        if value:
            value_key = next(self._value_key_generator)
            self._update_values[value_key] = value
            if utype == 'SET':
                if overwrite:
                    self._update_expressions['SET'] = f'{path} = {value_key}'
                else:
                    self._update_expressions['SET'] = f'{path} = if_not_exists({path}, {value_key})'
            elif utype == 'LIST_APPEND':
                self._update_expressions['SET'] = f'{path} = list_append({path}, {value_key})'
            elif utype == 'ADD':
                self._update_expressions['ADD'] = f'{path} {value_key}'
            elif utype == 'DELETE':
                self._update_expressions['DELETE'] = f'{path} {value_key}'
            else:
                raise InvalidUpdateExpressionsError
        elif utype == 'REMOVE':
            self._update_expressions['REMOVE'] = f'{path}'
        else:
            raise InvalidUpdateExpressionsError
        
    def create(self = None, data = {}, overwrite = False):
        if self is None:
            self = BaseItemWrapper()
        self.request = partial(self.table.put_item, Item = data)
        self.data = data
        self.overwrite = overwrite

        if not overwrite:
            self.request.keywords['ConditionExpression'] = And(Attr('sk').not_exists(), Attr('pk').ne(data['pk']))

        self.request_type = 'create'
        return self

    def read(self = None, pk = None, sk = None, attributes_to_get = []):
        if self is None:
            self = BaseItemWrapper()
        self.pk = pk
        self.sk = sk
        self.attributes_to_get = attributes_to_get
        self.request = partial(self.table.get_item,
            Key = {
                PARTITION_KEY : pk,
                SEARCH_KEY : sk
            }
        )
        if attributes_to_get:
            exp_atrb_names = [x for x in self._init_value_key_generator(len(attributes_to_get))]
            self.request.keywords['ExpressionAttributeNames'] = dict(zip(exp_atrb_names, attributes_to_get))
            self.request.keywords['ProjectionExpression'] = ', '.join(exp_atrb_names)


        self.request_type = 'read'
        return self

    def update(self = None, pk = None, sk = None, expressions = []):
        if self is None:
            self = BaseItemWrapper()
        self._value_key_generator = self._init_value_key_generator()
        self._update_expressions = {'SET':[],'ADD':[],'REMOVE':[],'DELETE':[]}
        self._update_values = {}
        
        for x in expressions:
            self._add_update_expression(
                x['utype'],
                x['path'],
                value = x.get('value'),
                overwrite = x.get('overwrite')
            )
        final_update_expression = ''
        for k, v in self._update_expressions:
            if v:
                final_update_expression = f'{final_update_expression} {k} {", ".join(v)} '

        self.request = partial( self.table.update_item,
            Key = {
                PARTITION_KEY : pk,
                SEARCH_KEY : sk
            },
            UpdateExpression = final_update_expression
        )
        if self._update_values:
            self.request.keywords['ExpressionAttributeValues'] = self._update_values
        
        self.request_type = 'update'
        return self

    def delete(self = None, pk = None, sk = None):
        if self is None:
            self = BaseItemWrapper()
        self.request = partial(selfhttps://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types.table.delete_item,
            Key = {
                PARTITION_KEY : pk,
                SEARCH_KEY : sk
            }
        )
        
        self.request_type = 'delete'
        return self

    def execute(self):
        if not callable(self.request) or self.request_type is None:
            raise RequestNotSetError
        result = {}
        try:
            result = self.request()
        except Exception as error:
            raise error
        else:
            return result.get('Item')

class QueryScanSetterMixin:
    def __init__(self, limit = 30, start_key = None, filter_expression = None):
        self.table = dynamodb.Table(TABLE_NAME)
        self.limit = limit
        self.exclusive_start_key = start_key
        self._attributes_to_get = []
        self.filter_expression = filter_expression
        self.consistent_read = CONSISTENT_READ

    @property
    def attributes_to_get(self):
        return self._attributes_to_get

    def add_attributes_to_get(self, *args):
        for attribute in args:
            self._attributes_to_get.append(attribute)

    def _execute(self, func):
        func.keywords['Limit'] = self.limit
        func.keywords['ConsistentRead'] = self.consistent_read

        if self.filter_expression:
            func.keywords['FilterExpression'] = self.filter_expression
        
        if self.exclusive_start_key:
            func.keywords['ExclusiveStartKey'] = self.exclusive_start_key

        if self.attributes_to_get:
            projection_expression = ', '.join(self.attributes_to_get)
            func.keywords['Select'] = 'SPECIFIC_ATTRIBUTES'
            func.keywords['ProjectionExpression'] = projection_expression
        
        try:
            result = func()
        except Exception as err:
            raise err
        else:
            return result.get('Items')

class BaseQueryWrapper(QueryScanSetterMixin):
    def __init__(self, pk, sk_condition = None, **kargs):
        super().__init__(**kargs)
        self.sk_condition = sk_condition
        self.pk = pk

    def execute(self):
        if self.sk_condition:
            key_condition = And(Key(PARTITION_KEY).eq(self.pk), self.sk_condition)
        else:
            key_condition = Key(PARTITION_KEY).eq(self.pk)
        return self._execute(
            partial(self.table.query,
                KeyConditionExpression = key_condition,
                ScanIndexForward = False,
            )
        )

class BaseScanWrapper(QueryScanSetterMixin):
    def execute(self):
        return self._execute(
            partial(self.table.scan,
            )
        )