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

#tushare token
tushare_token='e239683c699765e4e49b43dff2cf7ed7fc232cc49f7992dab1ab7624'




#股票列表
def get_stock_basic() :
    print('start to download data') 
    pro = tushare.pro_api()
    data = pro.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    #保存到csv文件
    data.to_csv(stock_list_file)

    engine=sqlalchemy.create_engine('mysql+pymysql://root:root@127.0.0.1/test?charset=utf8mb4')
    try:
         #先一次性入库，异常后逐条入库
        pandas.io.sql.to_sql(frame=data, name='tb_stock_basic', con=engine, schema='test', if_exists='replace', index=False) 
    except:
        print('股票列表数据入库异常!')
    finally:
        pass

    
    '''
    #数据库参数
    db_host = '127.0.0.1'
    db_user = 'sa'
    db_password = '!Yjt148218'
    db_db = 'quantum'
    db_charset = 'utf8'
    db_url = 'mssql+pymssql://sa:pwd@127.0.0.1:1433/quantum'
    
    #入库    
    engine = sqlalchemy.create_engine(db_url)
    try:
         #先一次性入库，异常后逐条入库
        pandas.io.sql.to_sql(data, 'stock_basic', engine, schema='quantum.dbo', if_exists='append', index=False)
    except :
        #逐行入库
        print('批量入库异常，开始逐条入库.')
        for indexs in data.index :
            line = data.iloc[indexs:indexs+1]
            try:
                pandas.io.sql.to_sql(line, 'stock_basic', engine, schema='quantum.dbo', if_exists='append', index=False, chunksize=1)
            except:
                print('股票列表数据入库异常：')
                print(line)
            finally:
                pass
    finally:
        pass
    '''
    


    
    print('完成下载股票列表数据')
    return 1


#全量下载所有股票列表数据
if __name__ == '__main__':
   print('开始...')
   
   #初始化tushare
   tushare.set_token(tushare_token)
   print('获取股票列表')
   get_stock_basic()
   print('结束')
