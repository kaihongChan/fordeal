# 订单数据
import datetime
import hashlib
import json
import logging
import math
import time
import traceback
from http import cookiejar

import pandas as pd
import redis
import requests
from requests.utils import dict_from_cookiejar
from sqlalchemy import create_engine, text

from conf import db_cfg, redis_cfg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename="./logs/order.txt"
)
logger = logging.getLogger('order')


class Order:
    def __init__(self):
        conn = (
            f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
            f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
        )
        db_engine = create_engine(conn)
        self._db_conn = db_engine.connect()

        self._redis = redis.Redis(**redis_cfg)

        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listSaleOrder/1"

        self._page_size = 100
        self._order_total = 0

    def __del__(self):
        self._db_conn.close()
        self._redis.close()

    def _parse_and_save(self, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            self._order_total = resp_json['data']['total']
            rows = resp_json['data']['rows']
            sku_list = []
            for row in rows:
                for sku in row['skus']:
                    sku['order_id'] = row['id']
                    sku_list.append(sku)
                del row['skus']

                exist_sql = (
                    f"select `id` from `orders` where `id`={row['id']}"
                )
                exist = pd.read_sql(sql=text(exist_sql), con=self._db_conn)
                if exist.empty:
                    # 插入
                    columns = ",".join(row.keys())
                    vals = tuple(row.values())

                    insert_sql = (
                        f"insert into `orders` ({columns}) values {vals}"
                    )
                    print(insert_sql)
                    self._db_conn.execute(text(insert_sql))
                    self._db_conn.commit()
                else:
                    # 更新
                    k_v = ""
                    for column, val in row.items():
                        k_v += f"{column}='{val}',"
                    k_v = k_v[0:-1]
                    update_sql = (
                        f"update `orders` set {k_v} where `id`={row['id']}"
                    )
                    self._db_conn.execute(text(update_sql))
                    self._db_conn.commit()

            sku_list = pd.DataFrame(sku_list)
            sku_list.to_sql(name="order_skus", con=self._db_conn, index=False, if_exists='append')


    def _date_pre_handle(self, create_date):
        """ 订单采集日期预处理 """
        create_date = datetime.datetime.strptime(create_date, '%Y-%m-%d').date()
        today = datetime.date.today()
        sub_days = (today - create_date).days
        # 步长90天
        date_list = []
        for i in range(0, sub_days + 1, 90):
            day = create_date + datetime.timedelta(days=i)
            date_list.append(day)

        if today not in date_list:
            date_list.append(today)

        if len(date_list) == 1 and date_list[0] == today:
            date_list.append(today)

        return date_list

    def _request_handle(self, username, request_data):
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }
        try:
            cookie_file_name = f"./cookie_files/cookie_{username}.txt"
            session = requests.session()
            cookie = cookiejar.LWPCookieJar()
            cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
            cookie = dict_from_cookiejar(cookie)
            session.cookies = requests.utils.cookiejar_from_dict(cookie)
            session.headers = header
            resp = session.get(self._url, params=request_data)
            logger.info(f"【{username}】响应数据：{resp.text}")
            return resp
        except Exception as e:
            logger.error(f"【{username}】请求错误")
            logger.error(e)

    def exec_handle(self):
        """ 监听任务列表 """
        while True:
            json_str = self._redis.brpop(['fordeal:order_jobs'], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    # 采集数据
                    date_list = self._date_pre_handle(job_dict['start_date'])
                    len_date = len(date_list)
                    for i in range(len_date):
                        if i == len_date - 1:
                            break

                        start_timestamp = int(round(time.mktime(time.strptime(str(date_list[i]), "%Y-%m-%d")) * 1000))
                        end_timestamp = int(round(time.mktime(time.strptime(str(date_list[i + 1]), "%Y-%m-%d")) * 1000)) + 86399000
                        search_params = {
                            "page": 1,
                            "pageSize": self._page_size,
                            "status": "-1",
                            "deliverModel": "FAP",
                            "placedOrderAtBegin": start_timestamp,
                            "placedOrderAtEnd": end_timestamp
                        }
                        timestamp = int(round(time.time() * 1000))
                        request_data = {
                            "data": json.dumps(search_params),
                            "ct": timestamp,
                            "plat": "h5",
                            "appname": "fordeal",
                            "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                        }
                        resp = self._request_handle(job_dict['username'], request_data)
                        self._parse_and_save(resp)
                        # 处理分页
                        if self._order_total > self._page_size:
                            page_num = math.ceil(self._order_total / self._page_size)
                            print(page_num)
                            for page_index in range(1, page_num):
                                p_time_stamp = int(round(time.time() * 1000))
                                search_params['page'] = page_index + 1
                                request_data['data'] = json.dumps(search_params)
                                request_data['ct'] = p_time_stamp
                                request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                                resp = self._request_handle(job_dict['username'], request_data)
                                self._parse_and_save(resp)

                        today = datetime.date.today()
                        self._redis.hset("fordeal:last_request_at", job_dict['username'], str(today))
                except Exception as e:
                    logger.error(f"【{job_dict['username']}】数据采集异常")
                    logger.error(e)
                    traceback.print_exc()
            else:
                print("无任务列表数据。。。")
                break


if __name__ == '__main__':
    Order().exec_handle()
