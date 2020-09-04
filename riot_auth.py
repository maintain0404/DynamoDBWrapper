import requests
import random
from django.conf import settings

import sys
sys.path.append(settings.BASE_DIR)
import secret_keys

def get_riot_id(name):
    header = {
        "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": secret_keys.riot_secret_key
    }
    url = 'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/'
    result = requests.get(url + name, headers = header)
    print(result)
    if result.status_code == 200:
        return result.json()
    else:
        return None

def get_riot_id_icon(name):
    res = get_riot_id(name)
    if res:
        return res['profileIconId']
    else:
        return None

def set_random_icon(riot_id_info):
    if riot_id_info['profileIconId'] < 29:
        nums = list(range(0, 29))
        nums.remove(riot_id_info['profileIconId'])
        return random.choice(nums)
    else:
        return random.randint(0, 28)
