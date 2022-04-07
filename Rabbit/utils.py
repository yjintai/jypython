# -*- coding: UTF-8 -*-
import datetime
from pandas.tseries.offsets import *
import pandas as pd
import tushare as ts

# 是否是工作日
def is_weekday():
    return datetime.datetime.today().weekday() < 5

def next_weekday(date):
    return pd.to_datetime(date) + BDay()

def is_business_day(date):
    return bool(len(pd.bdate_range(date, date)))

def business_day_offset (date,offset):
    day_offset = BusinessDay(n = offset) 
    return date+day_offset
#获取str类型的最近交易日期
def get_today_str():
    today = datetime.datetime.now()
    now = datetime.datetime.now().strftime("%H:%M")
    if now < '17:00':
        today = today + datetime.timedelta(days=-1)
    today_str = get_trade_date_str(today)
    return today_str
 
def get_trade_date_str(date):
    date_str = date.strftime("%Y%m%d")
    while not pro.query('trade_cal', start_date=date_str, end_date=date_str, fields="is_open").iloc[0,0]:
        date = date + datetime.timedelta(days=-1)
    return date.strftime("%Y%m%d")