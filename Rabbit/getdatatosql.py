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

#Get stock basic info for each code.
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
#Get Hu Sheng Gu Tong list
def get_hs_const(engine = sqlenginestr,schema = databasename):
    print('start to download hs_const data') 
    pro = ts.pro_api()
    dfsh = pro.hs_const(hs_type='SH', fields='ts_code,hs_type,in_date,out_date,is_new')
    dfsz = pro.hs_const(hs_type='SZ', fields='ts_code,hs_type,in_date,out_date,is_new')
    df = dfsh.append(dfsz,ignore_index=True)
    try:
        pd.io.sql.to_sql(frame=df, name='tb_hs_const', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download hs_const data successed!')
    return 1

# Get trade calendar
def get_trade_cal(engine = sqlenginestr,schema = databasename,start_date='20200101', end_date='20210412'):
    print('start to download trade_cal data') 
    pro = ts.pro_api()
    df = pro.trade_cal(start_date=start_date, end_date=end_date, fields='exchange,cal_date,is_open')
    try:
        pd.io.sql.to_sql(frame=df, name='tb_trade_cal', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download trade_cal data successed!')
    return 1
# Get stock daily data for all code.
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
        
        for retry in range(3):
            try:
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
                break
            except:
                if retry < 2:
                    print('failed. retry...')
                else:
                    print('failed.')
            finally:
                pass
    return 1

# Get Hu Sheng Gu Tong money flow every day
def get_moneyflow_hsgt(engine = sqlenginestr,schema = databasename,start_date='20210101', end_date='20210412'):
    print('start to download moneyflow_hsgt data') 
    pro = ts.pro_api()
    df = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
    try:
        pd.io.sql.to_sql(frame=df, name='tb_moneyflow_hsgt', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download moneyflow_hsgt data successed!')
    return 1

# Get Rong Zi Rong Quan margin every day
def get_rzrq_margin(engine = sqlenginestr,schema = databasename,start_date='20210101', end_date='20210412'):
    print('start to download rzrq_margin data') 
    pro = ts.pro_api()
    df = pro.margin(start_date=start_date, end_date=end_date)
    try:
        pd.io.sql.to_sql(frame=df, name='tb_rzrq_margin', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download rzrq_margin data successed!')
    return 1

# Get 龙虎榜机构成交明细
def get_daily_top_list_data(engine = sqlenginestr,schema = databasename,start_date='20210412', end_date='20210412'):
    print('start to download daily_top_list_data data') 
    pro = ts.pro_api()
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)
        df_top_list = pro.top_list(trade_date=date_str)  
        df_top_inst = pro.top_inst(trade_date=date_str)        
        if df_top_list.empty:
            print ('Empty data in ' + date_str)
        else:
            try:
                pd.io.sql.to_sql(frame=df_top_list, name='tb_daily_top_list', con=engine, schema= schema, if_exists='append', index=False) 
                pd.io.sql.to_sql(frame=df_top_inst, name='tb_daily_top_inst', con=engine, schema= schema, if_exists='append', index=False) 
                print('download ' +date_str+ ' daily_top_list_data successed!')
            except exc.IntegrityError:
                    #Ignore duplicates
                    print ("duplicated data " + date_str)
                    pass
            except:
                print('To SQL Database Failed')
            finally:
                pass
    return 1
# Get 概念股分类
def get_concept(engine = sqlenginestr,schema = databasename):
    print('start to download concept data') 
    pro = ts.pro_api()
    try:
        df_concept = pro.concept()
        pd.io.sql.to_sql(frame=df_concept, name='tb_concept', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download concept data successed!')

    for i in range(0, len(df_concept)): 
        code = df_concept.iloc[i]['code']
        if i == 0:
            ifexist = 'replace'
        else:
            ifexist = 'append'
        for retry in range(3):
            try:
                df_concept_detail = pro.concept_detail(id = code)
                print ('Got concept:'+code)
                time.sleep(0.4)
                pd.io.sql.to_sql(frame=df_concept_detail, name='tb_concept_detail', con=engine, schema = schema, if_exists = ifexist, index=True) 
                print ('To SQL:'+code)
                break
            except:
                if retry < 2:
                    print('failed. retry...')
                else:
                    print('failed.')
            finally:
                pass
    print('download concept data detail successed!')
    return 1
# 获取指数基本信息
def get_index_basic(engine = sqlenginestr,schema = databasename):
    print('start to download index_basic data') 
    pro = ts.pro_api()
    df = pro.index_basic()
    try:
        pd.io.sql.to_sql(frame=df, name='tb_index_basic', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download index_basic data successed!')
    return 1
#获取指数每日行情
def get_index_daily(engine = sqlenginestr,schema = databasename,start_date='20210412', end_date='20210412'):
    print('start to download index_daily')  
    ls = ['000001.SH','000005.SH','000300.SH','000905.SH','399001.SZ','399006.SZ']
    pro = ts.pro_api()  
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)  
        for tscode in ls:
            print(tscode)
            try:
                dfid = pro.index_daily(ts_code=tscode,trade_date=date_str)
                if dfid.empty:
                    print ('Empty data in ' + date_str)
                else:
                    try:
                        pd.io.sql.to_sql(frame=dfid, name='tb_index_daily', con=engine, schema= schema, if_exists='append', index=False) 
                        print('download ' +date_str+ ' index_daily data successed!')
                    except exc.IntegrityError:
                            print ("duplicated data " + date_str)
                            pass
                    except:
                        print('To SQL Database Failed')
                    finally:
                        pass
            except:
                print('From Tushare failed.')
            finally:
                pass           
    return 1
#获取指数每日指标数据，包括总市值，换手率，市盈率等
def get_index_dailybasic(engine = sqlenginestr,schema = databasename,start_date='20210412', end_date='20210412'):
    print('start to download index_dailybasic')  
    ls = ['000001.SH','000005.SH','000300.SH','000905.SH','399001.SZ','399006.SZ']
    pro = ts.pro_api()  
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)  
        for tscode in ls:
            print(tscode)
            try:
                dfidb = pro.index_dailybasic(ts_code=tscode,trade_date=date_str)
                if dfidb.empty:
                    print ('Empty data in ' + date_str)
                else:
                    try:
                        pd.io.sql.to_sql(frame=dfidb, name='tb_index_dailybasic', con=engine, schema= schema, if_exists='append', index=False)           
                        print('download ' +date_str+ ' index_dailybasic data successed!')
                    except exc.IntegrityError:
                            print ("duplicated data " + date_str)
                    except:
                        print('To SQL Database Failed')
                    finally:
                        pass
            except:
                print('From Tushare failed.')
            finally:
                pass           
    return 1
#获取同花顺板块指数参数
def get_ths_index(engine = sqlenginestr,schema = databasename):
    print('start to download ths_index data') 
    pro = ts.pro_api()
    df = pro.ths_index()
    try:
        pd.io.sql.to_sql(frame=df, name='tb_ths_index', con=engine, schema= schema, if_exists='replace', index=True) 
    except:
        print('To SQL Database Failed')
    finally:
        pass
    print('download ths_index data successed!')
    return 1
#获取同花顺板块每日指标数据
def get_ths_daily(engine = sqlenginestr,schema = databasename,start_date='20210412', end_date='20210412'):
    print('start to download ths_daily data') 
    pro = ts.pro_api()
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        for retry in range(3):
            try:
                df_daily = pro.ths_daily(trade_date=date_str)
                if df_daily.empty:
                    print ('Empty data in ' + date_str)
                else:
                    try:
                        pd.io.sql.to_sql(frame=df_daily, name='tb_ths_daily', con=engine, schema= schema, if_exists='append', index=False) 
                        print('download ' +date_str+ ' daily data successed!')
                    except exc.IntegrityError:
                            #Ignore duplicates
                            print ("duplicated data " + date_str)
                            pass
                    except:
                        print('To SQL Database Failed')
                    finally:
                        pass
                break
            except:
                if retry < 2:
                    print('failed. retry...')
                else:
                    print('failed.')
            finally:
                pass
        
        
    return 1
def process_weekly(start_date,end_date):
    engine = initiate()
    #May be updated weekly.
    '''
    get_trade_cal(engine,databasename,'20170101',end_date)
    
    get_hs_const(engine,databasename)
    get_trade_cal(engine,databasename,'20200101',end_date)
    #get_concept(engine,databasename)
    get_index_basic(engine,databasename)
    get_ths_index(engine,databasename)
    '''
def process_daily(start_date,end_date):
    engine = initiate()
    #updated after 24:00AM every day 
 
    get_moneyflow_hsgt(engine,databasename,'20200101',end_date)
    get_rzrq_margin(engine,databasename,'20200101',end_date)
    get_daily_top_list_data(engine,databasename,start_date,end_date)
    
    #updated after 5:00PM every day
    get_stock_basic(engine,databasename)
    get_daily_data(engine,databasename,start_date,end_date)
    get_index_daily(engine,databasename,start_date,end_date)
    get_index_dailybasic(engine,databasename,start_date,end_date)
    get_ths_daily(engine,databasename,start_date,end_date)
    

if __name__ == '__main__':
    print('开始')
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start=datetime.datetime.now() -datetime.timedelta(days = 2)
    start_date = start.strftime('%Y%m%d')
    #start_date = '20150515'
    #end_date = '20150630'
    
    print('获取列表...')
    process_weekly(start_date,end_date)
    process_daily(start_date,end_date)
    
    print('结束')
