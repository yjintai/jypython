# -*- coding: UTF-8 -*-
import datetime
import time
from pandas.tseries.offsets import *
import pandas as pd
import tushare as ts
#tushare token
tushare_token='e239683c699765e4e49b43dff2cf7ed7fc232cc49f7992dab1ab7624'
import sqlalchemy
from sqlalchemy import exc
import pymysql

databasename = 'msstock'
sqlenginestr='mysql+pymysql://pyuser:Pyuser18@127.0.0.1/'+databasename+'?charset=utf8mb4'

def initiate():
    #初始化tushare
    ts.set_token(tushare_token)
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

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
def get_trade_date(date_str):
    engine = initiate()
    date=datetime.datetime.strptime(date_str,'%Y%m%d')
    while (1):
    #pro = ts.pro_api()
    #while not pro.query('trade_cal', start_date=date_str, end_date=date_str, fields="is_open").iloc[0,0]:
        sql = '''SELECT * FROM msstock.tb_trade_cal where cal_date = '%s' and is_open = '1';''' %date_str
        df = pd.read_sql_query(sql, engine)
        if df.empty:
            date = date + datetime.timedelta(days=-1)
            date_str = date.strftime("%Y%m%d")
        else:
            break
    return date_str

#获取str类型的上一个交易日期
def get_previous_trade_date(date_str, delta=1):
    previous_cal_date=datetime.datetime.strptime(date_str,'%Y%m%d')
    i = delta
    while i >0 :
        i = i-1
        previous_date = previous_cal_date - datetime.timedelta(days=1)
        previous_date_str = previous_date.strftime("%Y%m%d")
        previous_cal_date_str = get_trade_date(previous_date_str)
        previous_cal_date = datetime.datetime.strptime(previous_cal_date_str,'%Y%m%d')
    return previous_cal_date_str

#获取str类型的前n个自然日期
def get_previous_date(date_str,delta=1):
    date=datetime.datetime.strptime(date_str,'%Y%m%d')
    previous_date = date - datetime.timedelta(days=delta)
    previous_date_str = previous_date.strftime("%Y%m%d")
    return previous_date_str

#获取str类型的今天的最近交易日期
def get_today_str():
    today = datetime.datetime.now()
    now = datetime.datetime.now().strftime("%H:%M")
    if now < '17:00': #5点前取前一个交易日
        today = today + datetime.timedelta(days=-1)
    today_str = today.strftime("%Y%m%d")
    today_str = get_trade_date(today_str)
    return today_str

# 获取指定日期的股票的连板数
def get_qty_limit_up(code,date_str):
    qty_limit_up = 1
    end_date = date_str
    start_date = get_previous_trade_date(date_str,20)
    pro = ts.pro_api()
    df = pro.limit_list(start_date=start_date, end_date= end_date, ts_code = code, limit_type = 'U')
    df.sort_values(by=['trade_date'],ascending=False)
    previous_date = date_str
    i = df.shape[0]
    while i >0 :
        i = i-1
        previous_date = get_previous_trade_date(previous_date)
        df1 = df[df['trade_date']== previous_date]
        if not df1.empty:
            qty_limit_up = qty_limit_up+1
        else:
            break
    print  ("%s:%d" %(code,qty_limit_up))
    return qty_limit_up