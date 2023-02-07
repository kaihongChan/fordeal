import json
import logging
from http import cookiejar

import pandas as pd
import redis
import requests
from requests.utils import dict_from_cookiejar
from sqlalchemy import create_engine, text

from conf import db_cfg, redis_cfg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename="my_info.txt"
)
logger = logging.getLogger('myInfo')


class MyInfo:
    def __init__(self):
        conn = (
            f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
            f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
        )
        db_engine = create_engine(conn)
        self._db_conn = db_engine.connect()

        self._redis = redis.Redis(**redis_cfg)

    def __del__(self):
        self._db_conn.close()
        self._redis.close()

    def _parse_and_save(self, resp, account_id):
        """ 数据解析及保存 """
        data = resp['data']['user']
        print(data)
        return
        del data['privilege'], data['mtoken']
        exist_sql = (
            f"select shopId from my_info where shopId={data['shopId']}"
        )
        exist = pd.read_sql(sql=text(exist_sql), con=self._db_conn)
        if exist.empty:
            # 插入
            columns = ",".join(data.keys())
            vals = tuple(data.values())

            insert_sql = (
                f"insert into `my_info` ({columns}) values {vals}"
            )
            self._db_conn.execute(text(insert_sql))
            self._db_conn.commit()
        else:
            k_v = ""
            for column, val in data.items():
                k_v += f"{column}='{val}',"
            k_v = k_v[0:-1]
            update_sql = (
                f"update `my_info` set {k_v} where shopId={data['shopId']}"
            )
            self._db_conn.execute(text(update_sql))
            self._db_conn.commit()

        # 更新accounts表
        update_sql = (
            f"update `accounts` set shop_id={data['shopId']} where id={account_id}"
        )
        self._db_conn.execute(text(update_sql))
        self._db_conn.commit()

    def request_handle(self):
        """ 发送请求 """
        url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.myInfo/1?data=&gw_ver=1&plat=pc&ct=1675662549492&appname=matrix&sign=cd92b5106e65f4201afa3c36f866a724'
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
            'accept': 'application/json, text/plain, */*',
        }
        while True:
            json_str = self._redis.brpop(['fordeal:my_info_jobs'], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    cookie_file_name = f"./cookie_files/cookie_{job_dict['username']}.txt"
                    session = requests.session()
                    cookie = cookiejar.LWPCookieJar()
                    cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
                    cookie = dict_from_cookiejar(cookie)
                    session.cookies = requests.utils.cookiejar_from_dict(cookie)
                    session.headers = header
                    resp = session.get(url)
                    logger.info(f"【{job_dict['username']}】响应数据：{resp.text}")

                    resp_json = resp.json()
                    if resp.status_code == 200 and resp_json['code'] == 1001:
                        self._parse_and_save(resp_json, job_dict['id'])
                    else:
                        raise Exception("数据采集失败！")
                except Exception as e:
                    logger.error(f"【{job_dict['username']}】数据采集异常")
                    logger.error(e)
            else:
                print("无任务列表数据。。。")
                break


if __name__ == '__main__':
    MyInfo().request_handle()
