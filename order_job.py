import datetime
import json

import pandas as pd
import redis as redis
from sqlalchemy import create_engine, text

from conf import db_cfg, redis_cfg

if __name__ == '__main__':
    conn = (
        f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
        f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
    )
    db_conn = create_engine(conn).connect()

    redis_client = redis.Redis(**redis_cfg)

    query_sql = (
        "select i.`shopId`, i.`shopName`, a.`username`, date_format(i.`createAt`, '%Y-%m-%d') as `start_date`"
        " from accounts as a left join my_info as i on a.`shop_id`=i.`shopId`"
        " where a.`status`=1"
    )
    accounts = pd.read_sql(text(query_sql), con=db_conn)
    for _, account in accounts.iterrows():
        # 获取最新采集日期
        last_request = redis_client.hget("fordeal:last_request_at", account['username'])
        if last_request is not None:
            last_request = datetime.datetime.strptime(last_request, '%Y-%m-%d')
            account['start_date'] = (last_request - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        redis_client.lpush("fordeal:order_jobs", json.dumps(account.to_dict()))
