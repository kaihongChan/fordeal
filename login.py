import json
from http import cookiejar
import logging
import requests
import pandas as pd
from conf import db_cfg
from sqlalchemy import create_engine, text

logger = logging.getLogger()

if __name__ == '__main__':

    conn = (
        f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
        f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
    )
    db_engine = create_engine(conn)

    query_sql = (
        "select * from accounts where `status`=1"
    )
    accounts = pd.read_sql(text(query_sql), con=db_engine.connect())

    headers = {
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
        "Referer": "https://business.fordeal.com/",
        "Host": "cn-ali-gw.fordeal.com"
    }

    login_url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.login/2"

    for _, account in accounts.iterrows():
        account_json = json.dumps({"loginName": account['username'], "password": account['password']})
        try:
            login_data = {
                "data": account_json
            }
            session = requests.session()
            session.cookies = cookiejar.LWPCookieJar(filename=f"cookies_{account['username']}.txt")
            resp = session.post(url=login_url, data=login_data, headers=headers)
            logger.info(f"登录返回：{resp.text}")

            if resp.status_code == 200 and json.loads(resp.text)['code'] == 1001:
                session.cookies.save(ignore_discard=True, ignore_expires=True)
        except Exception as e:
            logger.info(f"【{account['username']}】登录异常")
            logger.info(e)
