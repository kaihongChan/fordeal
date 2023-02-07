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
        "select `id`, `username`, `password` from accounts where `status`=1"
    )
    accounts = pd.read_sql(text(query_sql), con=db_conn)
    for _, account in accounts.iterrows():
        redis_client.lpush("fordeal:order_jobs", json.dumps(account.to_dict()))
