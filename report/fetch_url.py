# -*- coding: utf-8 -*-
import requests
import urllib3
from fake_useragent import UserAgent

### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import time
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

user_agent = UserAgent()


def fetch(url, retries=5):
    for i in range(retries):
        print("FETCH:", url, flush=True)
        try:
            r = requests.get(url, headers={'user-agent': user_agent.random},
                             verify=False, timeout=(10, 60))
            r.raise_for_status()
            time.sleep(3)          # 避免連續請求被限流
            return r
        except (ChunkedEncodingError, ConnectionError, Timeout) as e:
            wait = 10 * (2 ** i)   # 10, 20, 40, 80, 160 秒
            print(f"  失敗 [{i+1}/{retries}]：{e}，{wait} 秒後重試", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"重試 {retries} 次仍失敗：{url}")


"""
if __name__ == '__main__':
   
   url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=' + date_sii + '&type=ALLBUT0999' 
   r = fetch(url)
"""   
