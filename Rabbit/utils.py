# -*- coding: UTF-8 -*-
import datetime
from pandas.tseries.offsets import *
import pandas as pd
import tushare as ts
#tushare token
tushare_token='e239683c699765e4e49b43dff2cf7ed7fc232cc49f7992dab1ab7624'

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





#获取date类型的最近交易日期
def get_trade_date(date):
    ts.set_token(tushare_token)
    pro = ts.pro_api()
    date_str = date.strftime("%Y%m%d")
    while not pro.query('trade_cal', start_date=date_str, end_date=date_str, fields="is_open").iloc[0,0]:
        date = date + datetime.timedelta(days=-1)
        date_str = date.strftime("%Y%m%d")
    return date

#获取date类型的上一个交易日期
def get_previous_date(date):
    previous_date = date + datetime.timedelta(days=-1)
    previous_cal_date = get_trade_date(previous_date)
    return previous_cal_date

#获取str类型的今天的最近交易日期
def get_today_str():
    today = datetime.datetime.now()
    now = datetime.datetime.now().strftime("%H:%M")
    if now < '17:00': #5点前取前一个交易日
        today = today + datetime.timedelta(days=-1)
    today = get_trade_date(today)
    return today.strftime("%Y%m%d")