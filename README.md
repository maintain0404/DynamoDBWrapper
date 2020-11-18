# 소개
AWS DynamoDB를 좀 더 편하게 사용할 수 있게 작성한 라이브러리입니다.

# 사용법
### 공통
DynamoDB에서는 데이터 해싱을 위해 primary key를 사용합니다. 그리고 primary key는 partition key(hash), search key(range)로 나뉘어지거나 나뉘어지지 않을 수 있습니다. 나누어 쓰는 경우가 많기 때문에 여기서는 나누어 쓰는 것을 전제로 하고 있습니다.

### 단일 아이템 요청
BaseItemWrapper 클래스의 CRUD 메서드들을 사용합니다. CRUD 메서드들 자체가 생성자(``__init__``)을 겸하고 있으므로 따로 생성자를 사용할 필요는 없습니다. CRUD 메서드로 요청을 생성하고 execute 메서드로 최종적으로 요청을 실행합니다.
```python
# 예시
# create 실행
BaseItemWrapper.create(data = {
    'pk':'USER',
    'sk':'google#1233249310901'
}).execute()

# read 실행
BaseItemWrapper.read(pk = 'USER', sk = 'google#2331314231',
    attributes_to_get = [
        'pk', 'sk', 'User.Name', 'User.Email'
    ]).execute()

# update 실행
# utype, path, value, overwrite를 반드시 지켜야 하며, value와 overwrite는 연산 종류에 따라 없을 수도 있음
BaseItemWrapper.update(
    pk = 'USER',
    sk = 'google#1233249310901',
    expressions = [
        {'utype':'SET', 'path':'User.Name', 'value':'USER123','overwrite':False}
    ]
).execute()

# delete 실행
BaseItemWrapper.delete(
    pk = 'USER',
    sk = 'google#1231412313'
)
```

### 쿼리
BaseQueryWrapper 클래스로 요청을 생성하고 execute 메서드로 요청을 실행합니다.
sk_condition, filter_expression, start_key는 다음 표현식을 사용합니다.
[참고](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types)
```python
BaseQueryWrapper('USER' # pk 하나를 반드시 받음, 
    sk_condition = Key('sk').begins_with('google'),
    filter_expression = Attr('User.Name').eq('USER123'),
    limit = 20,
    start_key = Key('pk').eq('USER') and Key('sk').eq('USER000')
)
```

### 스캔
BaseScanWrapper 클래스로 요청을 생성하고 execute 메서드로 요청을 실행합니다. BaseQueryWrapper와 기본적인 Key를 지정할 필요가 없습니다.
```python
BaseScanWrapper().execute()
```

### 참조
[Dynamodb Docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html?highlight=dynamodb)
[]