import json
import boto3
from boto3.dynamodb.conditions import *
import os
import datetime
import functools

import secret_keys

dynamodb = boto3.resource('dynamodb',
    region_name = 'ap-northeast-2', # 서울
    aws_access_key_id = secret_keys.aws_secret['AWS']['Access Key ID'],
    aws_secret_access_key = secret_keys.aws_secret['AWS']['Security Access Key'],
)   

class ValidationError(Exception):
    def __init__(self):
        super().__init__('Invalid data')

class InvaildPrimaryKeyError(Exception):
    def __init__(self):
        super().__init__("PK and SK must exist or must not exist simultaneously")

class DataAlreadyExistsError(Exception):
    def __init__(self):
        super().__init__("Data is already exists.")

class SK:
    VIDEO = "vid#"
    POST = "pst#"
    COMMENT = "cmt#"
    PLIKE = "plk#"
    CLIKE = "clk#"

def make_new_sk(data_type : str, time_code = None):
    if data_type == 'video':
        type_prefix = SK.VIDEO
    elif data_type == 'post':
        type_prefix = SK.POST
    elif data_type == 'comment':
        type_prefix = SK.COMMENT
    elif data_type == 'post_like':
        type_prefix = SK.PLIKE
    elif data_type == 'comment_like':
        type_prefix = SK.CLIKE
    else:
        raise ValueError('data_type "%s" is not valid data_type' % data_type)
    return type_prefix + datetime.datetime.now().strftime('%Y:%m:%d:%H:%M:%S:%f')

def sk_is_valid(input_sk):
    return True

class BaseItemWrapper:
    def __init__(self, table_name = 'Practice'):
        self.table_name = table_name
        self.table = dynamodb.Table(self.table_name)
        self._attributes_to_get = []
        self.request_type = None
        self._validators = []
        self._update_expressions = []
        self._update_values = {}
        self.request = None

    def _add_update_expression(self, utype, path, value = None, overwrite = False):
        # 26개 이하로 요청할것
        value_key = f':{chr(len(self._update_values) + 65)}'
        if utype == 'SET':
            self._update_values[value_key] = value
            uexp = f'SET {path} = {value_key}'
        elif utype == 'LIST_APPEND':
            self._update_values[value_key] = [value]
            uexp = f'SET {path} = list_append({path}, {value_key})'
        elif utype == 'ADD_NUMBER':
            self._update_values[value_key] = value
            uexp = f'ADD {path} {value_key}' 
        elif utype == 'REMOVE':
            uexp = f'REMOVE {path}'
        elif utype == 'DELETE':
            uexp = f'DELETE {path}'
        self._update_expressions.append(uexp)

    def create(self = None, data = {}, overwrite = False):
        if self is None:
            self = BaseItemWrapper()
        self.request = functools.partial(self.table.put_item, Item = data)
        if not overwrite:
            self.request.keywords['ConditionExpression'] = And(Attr('sk').not_exists(), Attr('pk').ne(data['pk']))

        self.reqeust_type = 'create'
        return self
        #     except Exception as err:
        #         if err.__class__.__name__ == 'ConditionalCheckFailedException':
        #             raise DataAlreadyExistsError
        #         else:
        #             raise err
        #     else:
        #         return result.get('Item')
        # else:
        #     return False

    def read(self = None, pk = None, sk = None, attributes_to_get = []):
        if self is None:
            print('none self')
            self = BaseItemWrapper()
        # Item에는 순전히 결과만 포함되어 있음, 추가 정보를 나중에 수정할 것
        self.request = functools.partial(self.table.get_item,
            Key = {
                'pk' : pk,
                'sk' : sk
            }
        )
        if attributes_to_get:
            self.request.keywords['ProjectionExpression'] = ', '.join(attributes_to_get)
        
        self.request_type = 'read'
        return self

    def update(self = None, pk = None, sk = None, expressions = []):
        # 현재 두 개 이상의 업데이트를 동시 진행하는 데에는 문제가 있음
        if self is None:
            self = BaseItemWrapper()
        for x in expressions:
            self._add_update_expression(
                x['utype'],
                x['path'],
                value = x.get('value'),
                overwrite = x.get('overwrite')
            )
        self.request = functools.partial( self.table.update_item,
            Key = {
                'pk' : pk,
                'sk' : sk
            },
            UpdateExpression = ' '.join(self._update_expressions)
        )
        if self._update_values:
            self.request.keywords['ExpressionAttributeValues'] = self._update_values
        self.request_type = 'update'
        return self

    def delete(self = None, pk = None, sk = None):
        if self is None:
            self = BaseItemWrapper()
        # 지워도 되는지 검증하는 절차가 필요하지 않을까?\'ResponseMetadata': {'RequestId': '44SN00VKKQRU17RQ7FV597JHL3VV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Thu, 03 Sep 2020 14:57:58 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '2', 'connection': 'keep-alive', 'x-amzn-requestid': '44SN00VKKQRU17RQ7FV597JHL3VV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '2745614147'}, 'RetryAttempts': 0}}
        self.request = functools.partial(self.table.delete_item,
            Key = {
                'pk' : pk,
                'sk' : sk
            }
        )
        self.request_type = 'delete'
        return self

    def execute(self):
        assert(callable(self.request),
            'Please set request by create, read, update and delete'
        )
        try:
            result = self.request()
        except Exception as error:
            if error.__class__ is "ConditionalCheckFailedException":
                if self.request_type is 'create':
                    raise DataAlreadyExistsError
        else:
            return result

    # def data_is_valid(self, raise_exception = False):
    #     assert (self.request_type == "create" or 'update', 
    #         "request_type must be 'create' or update to use data"
    #     )
    #     err = ''
    #     for validator in self._validators:
    #         try:
    #             validator(self._data)
    #         except Exception as error:
    #             err = error
    #             break
    #     if err and raise_exception:
    #         raise ValidationError
    #     else:
    #         return True

    # def add_validator(self, func):
    #     self._validators.append(func)


class QueryScanSetterMixin:
    def __init__(self, table_name="Practice", count = 30):
        self.table_name = table_name
        self.table = dynamodb.Table(table_name)
        self.count = count
        self.exclusive_start_key = None
        self._attributes_to_get = []
        self.filter_expression = None

    @property
    def attributes_to_get(self):
        return self._attributes_to_get

    def add_attributes_to_get(self, *args):
        for attribute in args:
            self._attributes_to_get.append(attribute)

class BaseQueryWrapper(QueryScanSetterMixin):
    def __init__(self, pk, table_name = "Practice", count = 30):
        super().__init__(table_name, count)
        self.pk = pk

    def go(self):
        final_query_func = functools.partial(self.table.query,
            Limit = self.count,
            KeyConditionExpression = self.pk,
            ScanIndexForward = False,
            ConsistentRead = False # True로 바꾸면 실시간 반영이 더 엄밀해짐
        )
        if self.filter_expression:
            final_query_func.keywords['FilterExpression'] = self.filter_expression
        
        if self.exclusive_start_key:
            final_query_func.keywords['ExclusiveStartKey'] = self.exclusive_start_key

        if self.attributes_to_get:
            projection_expression = ', '.join(self.attributes_to_get)
            final_query_func.keywords['Select'] = 'SPECIFIC_ATTRIBUTES'
            final_query_func.keywords['ProjectionExpression'] = projection_expression
        
        # 에러 핸들링 구현 필요
        try:
            result = final_query_func()
        except Exception as err:
            return None
        else:
            return result

class BaseScanWrapper(QueryScanSetterMixin):
    def __init__(self, table_name = "Practice", count = 30):
        super().__init__(table_name, count)

    def go(self):
        final_scan_func = functools.partial(self.table.scan,
            Limit = self.count,
            ConsistentRead = False # True로 바꾸면 실시간 반영이 더 엄밀해짐
        )
        if self.filter_expression:
            final_scan_func.keywords['FilterExpression'] = self.filter_expression
        
        if self.exclusive_start_key:
            final_scan_func.keywords['ExclusiveStartKey'] = self.exclusive_start_key

        if self.attributes_to_get:
            projection_expression = ', '.join(self.attributes_to_get)
            final_scan_func.keywords['Select'] = 'SPECIFIC_ATTRIBUTES'
            final_scan_func.keywords['ProjectionExpression'] = projection_expression
        
        # 에러 핸들링 구현 필요
        try:
            result = final_scan_func()
        except Exception as err:
            print(err)
            return None
        else:
            return result