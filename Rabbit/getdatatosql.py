#!/usr/bin/python3
# coding:utf-8
# -*- coding: utf-8 -*-

import time
import datetime
from tkinter.ttk import Frame


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

def save_to_sql(frame,name,con,schema,if_exists='append',index=False):
    try:
        pd.io.sql.to_sql(frame, name, con, schema, if_exists, index) 
        print('save to sql table %s successed!' %name)
    except exc.IntegrityError:
        #Ignore duplicates
        print ("duplicated data in " + name)
        return False
    except:
        print('To SQL Database Failed')
        return False
    finally:
        pass
    return True

#Get stock basic info for each code.
def get_stock_basic(engine = sqlenginestr,schema = databasename):
    print('start to download stock_basic data') 
    pro = ts.pro_api()
    df = pro.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    save_to_sql(frame=df, name='tb_stock_basic', con=engine, schema= schema, if_exists='replace', index=True) 
    return True

#Get Hu Sheng Gu Tong list
def get_hs_const(engine = sqlenginestr,schema = databasename):
    print('start to download hs_const data') 
    pro = ts.pro_api()
    dfsh = pro.hs_const(hs_type='SH', fields='ts_code,hs_type,in_date,out_date,is_new')
    dfsz = pro.hs_const(hs_type='SZ', fields='ts_code,hs_type,in_date,out_date,is_new')
    df = dfsh.append(dfsz,ignore_index=True)
    save_to_sql(frame=df, name='tb_hs_const', con=engine, schema= schema, if_exists='replace', index=True) 
    return True

# Get trade calendar
def get_trade_cal(engine = sqlenginestr,schema = databasename,start_date='20200101', end_date='20210412'):
    print('start to download trade_cal data') 
    pro = ts.pro_api()
    df = pro.trade_cal(start_date=start_date, end_date=end_date, fields='exchange,cal_date,is_open')
    save_to_sql(frame=df, name='tb_trade_cal', con=engine, schema= schema, if_exists='replace', index=True) 
    return True

# Get stock daily data for all code.
def get_daily_data(engine = sqlenginestr,schema = databasename,date_str='20210412'):
    print('start to download daily data') 
    pro = ts.pro_api()
    for retry in range(3):
        try:
            df_daily = pro.daily(trade_date=date_str,fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
            df_daily_adjfactor = pro.adj_factor(trade_date=date_str, fields='ts_code,adj_factor')
            df_daily_basic = pro.daily_basic(trade_date=date_str,fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
            df_all = pd.merge(left=df_daily, right=df_daily_adjfactor, on='ts_code', how='left')
            df_all = pd.merge(left=df_all, right=df_daily_basic, on='ts_code', how='left')
            if df_all.empty:
                print ('Empty data in ' + date_str)
                return False
            else:
                ret=save_to_sql(frame=df_all, name='tb_daily_data', con=engine, schema= schema, if_exists='append', index=False) 
                if ret == False:
                    return False
            break
        except:
            if retry < 2:
                print('failed. retry...')
            else:
                print('failed.')
                return False
        finally:
            pass
    return True

# Get Hu Sheng Gu Tong money flow every day
def get_moneyflow_hsgt(engine = sqlenginestr,schema = databasename,start_date='20210101', end_date='20210412'):
    print('start to download moneyflow_hsgt data') 
    pro = ts.pro_api()
    df = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
    save_to_sql(frame=df, name='tb_moneyflow_hsgt', con=engine, schema= schema, if_exists='replace', index=True)
    return True

# Get Rong Zi Rong Quan margin every day
def get_rzrq_margin(engine = sqlenginestr,schema = databasename,start_date='20210101', end_date='20210412'):
    print('start to download rzrq_margin data') 
    pro = ts.pro_api()
    df = pro.margin(start_date=start_date, end_date=end_date)
    save_to_sql(frame=df, name='tb_rzrq_margin', con=engine, schema= schema, if_exists='replace', index=True)
    return True

# Get 龙虎榜机构成交明细
def get_daily_top_list_data(engine = sqlenginestr,schema = databasename,date_str='20210412'):
    print('start to download daily_top_list_data data') 
    pro = ts.pro_api()
    df_top_list = pro.top_list(trade_date=date_str)  
    df_top_inst = pro.top_inst(trade_date=date_str)        
    if df_top_list.empty:
        print ('Empty data in ' + date_str)
    else:
        save_to_sql(frame=df_top_list, name='tb_daily_top_list', con=engine, schema= schema, if_exists='append', index=False) 
        save_to_sql(frame=df_top_inst, name='tb_daily_top_inst', con=engine, schema= schema, if_exists='append', index=False) 
    return True

# Get 概念股分类
def get_concept(engine = sqlenginestr,schema = databasename):
    print('start to download concept data') 
    pro = ts.pro_api()
    df_concept = pro.concept()
    save_to_sql(frame=df_concept, name='tb_concept', con=engine, schema= schema, if_exists='replace', index=True) 

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
                save_to_sql(frame=df_concept_detail, name='tb_concept_detail', con=engine, schema = schema, if_exists = ifexist, index=True) 
                break
            except:
                if retry < 2:
                    print('failed. retry...')
                else:
                    print('failed.')
            finally:
                pass
    print('download concept data detail successed!')
    return True

# 获取指数基本信息
def get_index_basic(engine = sqlenginestr,schema = databasename):
    print('start to download index_basic data') 
    pro = ts.pro_api()
    df = pro.index_basic()
    save_to_sql(frame=df, name='tb_index_basic', con=engine, schema= schema, if_exists='replace', index=True)
    return True

#获取指数每日行情
def get_index_daily(engine = sqlenginestr,schema = databasename,date_str='20210412'):
    print('start to download index_daily')  
    ls = ['000001.SH','000005.SH','000300.SH','000905.SH','399001.SZ','399006.SZ']
    pro = ts.pro_api()    
    for tscode in ls:
        print(tscode)
        try:
            dfid = pro.index_daily(ts_code=tscode,trade_date=date_str)
            if dfid.empty:
                print ('Empty data in ' + date_str)
            else:
                save_to_sql(frame=dfid, name='tb_index_daily', con=engine, schema= schema, if_exists='append', index=False) 
        except:
            print('From Tushare failed.')
        finally:
            pass           
    return True

#获取指数每日指标数据，包括总市值，换手率，市盈率等
def get_index_dailybasic(engine = sqlenginestr,schema = databasename,date_str='20210412'):
    print('start to download index_dailybasic')  
    ls = ['000001.SH','000005.SH','000300.SH','000905.SH','399001.SZ','399006.SZ']
    pro = ts.pro_api()   
    for tscode in ls:
        print(tscode)
        try:
            dfidb = pro.index_dailybasic(ts_code=tscode,trade_date=date_str)
            if dfidb.empty:
                print ('Empty data in ' + date_str)
            else:
                save_to_sql(frame=dfidb, name='tb_index_dailybasic', con=engine, schema= schema, if_exists='append', index=False) 
        except:
            print('From Tushare failed.')
        finally:
            pass           
    return True

#获取同花顺板块指数参数
def get_ths_index(engine = sqlenginestr,schema = databasename):
    print('start to download ths_index data') 
    pro = ts.pro_api()
    df = pro.ths_index()
    save_to_sql(frame=df, name='tb_ths_index', con=engine, schema= schema, if_exists='replace', index=True)
    return True

#获取同花顺板块每日指标数据
def get_ths_daily(engine = sqlenginestr,schema = databasename,date_str='20210412'):
    print('start to download ths_daily data') 
    pro = ts.pro_api()
    for retry in range(3):
        try:
            df_daily = pro.ths_daily(trade_date=date_str)
            if df_daily.empty:
                print ('Empty data in ' + date_str)
            else:
                save_to_sql(frame=df_daily, name='tb_ths_daily', con=engine, schema= schema, if_exists='append', index=False) 
            break
        except:
            if retry < 2:
                print('failed. retry...')
            else:
                print('failed.')
        finally:
            pass
    return True

#计算保存每日涨停数据
def get_group_output(df, date, columns='industry', name='_limit_up'):
    df = df.copy()
    output = pd.DataFrame()
    output = pd.DataFrame(df.groupby(columns)['ts_code'].count())
    output['pe_mean'] = df.groupby(columns)['pe'].mean()
    output['pe_median'] = df.groupby(columns)['pe'].median()
    output['pb_mean'] = df.groupby(columns)['pb'].mean()
    output['pb_median'] = df.groupby(columns)['pb'].median()
    output['trade_date'] = date
    output.sort_values('ts_code', ascending=False, inplace=True)
    output.rename(columns={'ts_code': 'count'}, inplace=True)
    return output
def save_TDX_daily (engine = sqlenginestr,schema = databasename, date_str='20210412'):
    # TDX的每日数据
    print('start to compute and save TDX data') 
    sql = '''SELECT tb_daily_data.ts_code,
            tb_daily_data.trade_date,
            tb_stock_basic.name,
            tb_stock_basic.industry, 
            tb_stock_basic.market,
            tb_daily_data.open,
            tb_daily_data.close,
            tb_daily_data.high,
            tb_daily_data.low,
            tb_daily_data.pre_close,
            tb_daily_data.change,
            tb_daily_data.pct_chg,
            tb_daily_data.turnover_rate,
            tb_daily_data.volume_ratio,
            tb_daily_data.pe,
            tb_daily_data.pb,
            tb_daily_data.amount,
            tb_stock_basic.list_date
            FROM msstock.tb_daily_data 
            left join msstock.tb_stock_basic 
            on (tb_daily_data.ts_code = tb_stock_basic.ts_code) 
            where tb_daily_data.trade_date = "%s" 
            order by tb_daily_data.pct_chg desc;''' %date_str
    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)

    df_up = df[df['pct_chg'] > 0.00]
    df_down = df[df['pct_chg'] < 0.00]
    df_limit_up = df[(((df['market']=='创业板') | (df['market']=='科创板')) & (df['pct_chg'] >= 19.7) & (df['pct_chg'] < 20.1)) | (((df['market']!='创业板') & (df['market']!='科创板')) & (df['pct_chg'] >= 9.7) & (df['pct_chg'] < 10.1))]
    df_limit_up_new = df_limit_up[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_main = df[((df['market']!='创业板') & (df['market']!='科创板'))]
    df_up_main = df_up[((df_up['market']!='创业板') & (df_up['market']!='科创板'))]
    df_down_main = df_down[((df_down['market']!='创业板') & (df_down['market']!='科创板'))]
    df_limit_up_main = df_up_main[(df_up_main['pct_chg'] >= 9.7) & (df_up_main['pct_chg'] < 10.1)]
    df_limit_up_new_main = df_limit_up_main[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up_main['list_date']) <= pd.Timedelta(365)]

    df_chuangye = df[(df['market']=='创业板')]
    df_up_chuangye = df_up[(df_up['market']=='创业板')]
    df_down_chuangye = df_down[(df_down['market']=='创业板')]
    df_limit_up_chuangye = df_up_chuangye[(df_up_chuangye['pct_chg'] >= 19.7) & (df_up_chuangye['pct_chg'] < 20.1)]
    df_limit_up_new_chuangye= df_limit_up_chuangye[(pd.to_datetime(date_str) - pd.to_datetime(df_limit_up_chuangye['list_date']) <= pd.Timedelta(365))]

    df_kechuang = df[(df['market']=='科创板')]
    df_up_kechuang = df_up[(df_up['market']=='科创板')]
    df_down_kechuang = df_down[(df_down['market']=='科创板')]
    df_limit_up_kechuang = df_up_kechuang[(df_up_kechuang['pct_chg'] >= 19.7) & (df_up_kechuang['pct_chg'] < 20.1)]
    df_limit_up_new_kechuang= df_limit_up_kechuang[(pd.to_datetime(date_str) - pd.to_datetime(df_limit_up_kechuang['list_date'])) <= pd.Timedelta(365)]

    df_output_general = pd.DataFrame(columns=('market','trade_date','up', 'down', 'amount', 'limit_up', 'new_limit_up'))
    df_output_general.loc[1] = ['全部',date_str,df_up.shape[0],df_down.shape[0],df['amount'].sum(),df_limit_up.shape[0],df_limit_up_new.shape[0]]
    df_output_general.loc[2] = ['主板',date_str,df_up_main.shape[0],df_down_main.shape[0],df_main['amount'].sum(),df_limit_up_main.shape[0],df_limit_up_new_main.shape[0]]
    df_output_general.loc[3] = ['创业板',date_str,df_up_chuangye.shape[0],df_down_chuangye.shape[0],df_chuangye['amount'].sum(),df_limit_up_chuangye.shape[0],df_limit_up_new_chuangye.shape[0]]
    df_output_general.loc[4] = ['科创板',date_str,df_up_kechuang.shape[0],df_down_kechuang.shape[0],df_kechuang['amount'].sum(),df_limit_up_kechuang.shape[0],df_limit_up_new_kechuang.shape[0]]
    df_output_general[['up', 'down', 'amount', 'limit_up', 'new_limit_up']] = df_output_general[['up', 'down', 'amount', 'limit_up', 'new_limit_up']].astype(int)
        
    print('General Data:')
    if df_output_general['amount'].sum() == 0:
        print ('Empty data in ' + date_str)
    else:
        save_to_sql(frame=df_output_general, name='tb_daily_general_data', con=engine, schema= schema, if_exists='append', index=False)

    print('Limit up:')
    if df_limit_up.empty:
        print ('Empty data in ' + date_str)
    else:
        save_to_sql(frame=df_limit_up, name='tb_daily_limit_up', con=engine, schema= schema, if_exists='append', index=False)
        print('Limit up Group:')
        for group in ['industry']:
            group_limit_up = get_group_output(df_limit_up, date_str, columns=group, name='limit_up')
            save_to_sql(frame=group_limit_up, name='tb_daily_group_by_%s_limit_up' %group, con=engine, schema= schema, if_exists='append', index=True)


def process_weekly(start_date,end_date):
    print('start process weekly data') 
    engine = initiate()
    #May be updated weekly.
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)
        '''
        get_hs_const(engine,databasename)
        get_trade_cal(engine,databasename,'20200101',date_str)
        #get_concept(engine,databasename)
        get_index_basic(engine,databasename)
        get_ths_index(engine,databasename)
    '''
def process_daily(start_date,end_date):
    engine = initiate()
    print('start process daily data') 
    #updated after 24:00AM every day 
    fmt = '%Y%m%d'
    begin=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    for i in range((end - begin).days+1):
        date = begin + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)
        if get_daily_data(engine,databasename,date_str) != False:
            get_moneyflow_hsgt(engine,databasename,'20200101',date_str)
            get_rzrq_margin(engine,databasename,'20200101',date_str)
            get_daily_top_list_data(engine,databasename,date_str)
            
            #updated after 5:00PM every day
            get_stock_basic(engine,databasename)
            get_index_daily(engine,databasename,date_str)
            get_index_dailybasic(engine,databasename,date_str)
            get_ths_daily(engine,databasename,date_str)
            save_TDX_daily(engine,databasename,date_str)
            return True
        else:
            return False
    

if __name__ == '__main__':
    print('开始')
    end = datetime.datetime.now() -datetime.timedelta(days = 23)
    start=datetime.datetime.now() -datetime.timedelta(days = 23)
    end_date = end.strftime('%Y%m%d')
    start_date = start.strftime('%Y%m%d')
    #start_date = '20220315'
    #end_date = '20220315'
    
    process_daily(start_date,end_date)
    process_weekly(start_date,end_date)
    print('结束')
