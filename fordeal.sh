#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH
#
python3 /login.py
python3 /myInfo.py
python3 /logout.py