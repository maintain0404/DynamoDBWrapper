import secret_keys

REGION_NAME = 'ap-northeast-2' # 서울
AWS_ACCESS_KEY_ID = secret_keys.aws_secret['AWS']['Access Key ID']
AWS_SECRET_ACCESS_KEY = secret_keys.aws_secret['AWS']['Security Access Key']

PARTITION_KEY = 'pk'
SEARCH_KEY = 'sk'
TABLE_NAME = 'Practice'

AVAILABLE_SEARCH_KEYS = {
    'VIDEO':"vid#", 'POST':"pst#", 'COMMENT':"cmt#",
    'PLIKE':"plk#", 'CLIKE':"clk#"}

CONSISTENT_READ = False