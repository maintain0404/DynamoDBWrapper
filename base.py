import json
import boto3
from boto3.dynamodb.conditions import *
import os
from math import inf
from functools import partial, reduce
from datetime import datetime
from .dynamodb_settings import *
from typing import List, Dict, Optional

import secret_keys

dynamodb = boto3.resource('dynamodb',
    region_name = REGION_NAME, 
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

    def __init__(self, consistent_read = CONSISTENT_READ):
        self.request_type = None
        self.request = None
        self.consistent_read = consistent_read

    @staticmethod
    def _init_value_key_generator(max_num: int = -1):
        count = 0
        if max_num < 0:
            max_num = inf    
        while count < max_num:
            count += 1
            yield reduce((lambda total, x: f'{total}{chr(int(x) + 65)}'), str(count), ':')
        
    def _add_update_expression(self, utype: str, path: str, value: Optional[str] = None, overwrite: bool = False):
        if value:
            value_key = next(self._value_key_generator)
            self._update_values[value_key] = value
            if utype == 'SET':
                if overwrite:
                    self._update_expressions_dict['SET'] = f'{path} = {value_key}'
                else:
                    self._update_expressions_dict['SET'] = f'{path} = if_not_exists({path}, {value_key})'
            elif utype == 'LIST_APPEND':
                self._update_expressions_dict['SET'] = f'{path} = list_append({path}, {value_key})'
            elif utype == 'ADD':
                self._update_expressions_dict['ADD'] = f'{path} {value_key}'
            elif utype == 'DELETE':
                self._update_expressions_dict['DELETE'] = f'{path} {value_key}'
            else:
                raise InvalidUpdateExpressionsError
        elif utype == 'REMOVE':
            self._update_expressions_dict['REMOVE'] = f'{path}'
        else:
            raise InvalidUpdateExpressionsError
        
    def create(self = None, data: dict = {}, overwrite: bool = False):
        if self is None:
            self = BaseItemWrapper()
        self.data = data
        self.overwrite = overwrite
        self.request = partial(self.table.put_item, Item = self.data)

        self.request_type = 'create'
        return self

    def read(self = None, pk: str = None, sk: str = None, attributes_to_get: List[str] = []):
        if self is None:
            self = BaseItemWrapper()
        self.pk = pk
        self.sk = sk
        self.attributes_to_get = attributes_to_get
        self.request = partial(self.table.get_item,
            Key = {
                PARTITION_KEY : self.pk,
                SEARCH_KEY : self.sk
            }
        )

        self.request_type = 'read'
        return self

    def update(self = None, pk: str = None, sk: str = None, expressions: list = []):
        if self is None:
            self = BaseItemWrapper()
        self.pk = pk
        self.sk = sk
        self._value_key_generator = self._init_value_key_generator()
        self.update_expressions = expressions
        self._update_expressions_dict = {'SET':[],'ADD':[],'REMOVE':[],'DELETE':[]}
        self._update_values = {}
        
        self.request = partial( self.table.update_item,
            Key = {
                PARTITION_KEY : self.pk,
                SEARCH_KEY : self.sk
            },
        )
        
        self.request_type = 'update'
        return self

    def delete(self = None, pk: str = None, sk: str = None):
        if self is None:
            self = BaseItemWrapper()
        self.pk = pk
        self.sk = sk
        self.request = partial(self.table.delete_item,
            Key = {
                PARTITION_KEY : self.pk,
                SEARCH_KEY : self.sk
            }
        )
        
        self.request_type = 'delete'
        return self

    def execute(self):
        if not callable(self.request) or self.request_type not in ['create', 'read', 'update', 'delete']:
            raise RequestNotSetError
        result = {}

        self.request.keywords['ConsistentRead'] = self.consistent_read
        if self.request_type == 'create':
            if not self.overwrite:
                self.request.keywords['ConditionExpression'] = Not(And(Key(SEARCH_KEY).eq(self.data[SEARCH_KEY]), Key(PARTITION_KEY).eq(self.data[PARTITION_KEY])))
        
        elif self.request_type == 'read':
            if self.attributes_to_get:
                exp_atrb_names = [x for x in self._init_value_key_generator(len(self.attributes_to_get))]
                self.request.keywords['ExpressionAttributeNames'] = dict(zip(exp_atrb_names, self.attributes_to_get))
                self.request.keywords['ProjectionExpression'] = ', '.join(exp_atrb_names)
        
        elif self.request_type == 'update':
            for x in self.update_expressions:
                self._add_update_expression(
                    x.get('utype'),
                    x.get('path'),
                    value = x.get('value'),
                    overwrite = x.get('overwrite')
                )
            final_update_expression = ''
            for k, v in self._update_expressions_dict:
                if v:
                    final_update_expression = f'{final_update_expression} {k} {", ".join(v)} '
            self.request.keywords['UpdateExpression'] = final_update_expression
            if self._update_values:
                self.request.keywords['ExpressionAttributeValues'] = self._update_values 

        result = self.request()
        return result.get('Item')

class BaseQueryScanSetter:
    def __init__(self, limit: int = 30, start_key: Dict[str, str] = None, filter_expression = None, consistent_read = CONSISTENT_READ):
        self.table = dynamodb.Table(TABLE_NAME)
        self.limit = limit
        self.exclusive_start_key = start_key
        self._attributes_to_get = []
        self.filter_expression = filter_expression
        self.consistent_read = consistent_read

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
        
        result = func()
        return result.get('Items')

class BaseQueryWrapper(BaseQueryScanSetter):
    def __init__(self, pk: str, sk_condition = None, **kargs):
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

class BaseScanWrapper(BaseQueryScanSetter):
    def execute(self):
        return self._execute(
            partial(self.table.scan,
            )
        )