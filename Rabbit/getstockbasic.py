#!/usr/bin/python3
# coding:utf-8
# -*- coding: utf-8 -*-

import time
import datetime
import random

import tushare
import pandas
#import pymssql
#import sqlalchemy
#import mysql.connector
import sqlalchemy
import pymysql



#需修改的参数
stock_list_file = 'd:/stock_list.csv'
sqlenginestr='mysql+pymysql://root:root@127.0.0.1/msstock?charset=utf8mb4'
databasename = 'msstock'
#tushare token
tushare_token='e239683c699765e4e49b43dff2cf7ed7fc232cc49f7992dab1ab7624'

#股票列表
def initiate():
    #初始化tushare
    tushare.set_token(tushare_token)
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

def get_stock_basic(engine = sqlenginestr,schema = databasename):
    print('start to download stock_basic data') 
    pro = tushare.pro_api()
    df = pro.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    try:
        pandas.io.sql.to_sql(frame=df, name='tb_stock_basic', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download stock_basic data successed!')
    return 1

def get_trade_cal(engine = sqlenginestr,schema = databasename):
    print('start to download trade_cal data') 
    date_now = datetime.datetime.now().strftime('%Y%m%d')
    pro = tushare.pro_api()
    df = pro.trade_cal(start_date='20200101', end_date=date_now, fields='exchange,cal_date,is_open')
    try:
        pandas.io.sql.to_sql(frame=df, name='tb_trade_cal', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download trade_cal data successed!')
    return 1


#全量下载所有股票列表数据
if __name__ == '__main__':
    print('开始')
    engine = initiate()
    print('获取列表...')
    get_stock_basic(engine,databasename)
    get_trade_cal(engine,databasename)
    print('结束')
