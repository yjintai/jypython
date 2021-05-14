# -*- coding: UTF-8 -*-
import datetime
from pandas.tseries.offsets import *
import pandas as pd


ONE_HOUR_SECONDS = 60 * 60

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

