#!/usr/bin/python3
# coding:utf-8
# -*- coding: utf-8 -*-

import time
import datetime


import tushare as ts
import pandas as pd
import sqlalchemy
from sqlalchemy import exc
import pymysql

stock_list_file = 'd:/stock_list.csv'
databasename = 'msstock'
sqlenginestr='mysql+pymysql://root:root@127.0.0.1/'+databasename+'?charset=utf8mb4'

#tushare token
tushare_token='e239683c699765e4e49b43dff2cf7ed7fc232cc49f7992dab1ab7624'

#股票列表
def initiate():
    #初始化tushare
    ts.set_token(tushare_token)
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

def get_stock_basic(engine = sqlenginestr,schema = databasename):
    print('start to download stock_basic data') 
    pro = ts.pro_api()
    df = pro.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    try:
        pd.io.sql.to_sql(frame=df, name='tb_stock_basic', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download stock_basic data successed!')
    return 1

def get_trade_cal(engine = sqlenginestr,schema = databasename,start_date='20200101', end_date='20210412'):
    print('start to download trade_cal data') 
    pro = ts.pro_api()
    df = pro.trade_cal(start_date='20200101', end_date=end_date, fields='exchange,cal_date,is_open')
    try:
        pd.io.sql.to_sql(frame=df, name='tb_trade_cal', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download trade_cal data successed!')
    return 1

def get_daily_data(engine = sqlenginestr,schema = databasename,start_date='20210412', end_date='20210412'):
    print('start to download daily data') 
    pro = ts.pro_api()
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)
        df_daily = pro.daily(trade_date=date_str,fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
        df_daily_adjfactor = pro.adj_factor(trade_date=date_str, fields='ts_code,adj_factor')
        df_daily_basic = pro.daily_basic(trade_date=date_str,fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
        df_all = pd.merge(left=df_daily, right=df_daily_adjfactor, on='ts_code', how='left')
        df_all = pd.merge(left=df_all, right=df_daily_basic, on='ts_code', how='left')
        
        if df_all.empty:
            print ('Empty data in ' + date_str)
        else:
            try:
                pd.io.sql.to_sql(frame=df_all, name='tb_daily_data', con=engine, schema= schema, if_exists='append', index=False) 
                print('download ' +date_str+ ' daily data successed!')
            except exc.IntegrityError:
                    #Ignore duplicates
                    print ("duplicated data " + date_str)
                    pass
            except:
                print('To SQL Database Failed')
            finally:
                pass
        '''
        #insert into SQL for each line in df_all, ignore duplicates
        for indexs in df_all.index :
            line = df_all.iloc[indexs:indexs+1]
            try:
                pd.io.sql.to_sql(frame=line, name='tb_daily_data', con=engine, schema= schema, if_exists='append', index=False, chunksize=1)
            except exc.IntegrityError:
                #Ignore duplicates
                print ("duplicated line")
                pass
            except:
                print('To SQL Database Failed')
            finally:
                pass
        '''

    return 1

#全量下载所有股票列表数据
if __name__ == '__main__':
    print('开始')
    engine = initiate()
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = '20201001'
    end_date = '20210101'
    
    
    print('获取列表...')
    #get stock basic info
    #get_stock_basic(engine,databasename)

    #get stock trade calender from start_date to end_date

    #get_trade_cal(engine,databasename,start_date,end_date)

    #get stock daily data from start_date to end_date
    get_daily_data(engine,databasename,start_date,end_date)

    print('结束')
