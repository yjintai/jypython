import time
import datetime
import pandas as pd
import requests
import os
import warnings

import sqlalchemy
from sqlalchemy import exc
import pymysql

databasename = 'msstock'
sqlenginestr='mysql+pymysql://root:root@127.0.0.1/'+databasename+'?charset=utf8mb4'

pd.set_option('expand_frame_repr', False)
basedir = 'D:/Works/python/jypython/rabbit'

def initiate():
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

def myprint (str,filename, mode = 'a'):
    print (str)

def save_to_sql(dataframe,engine,databasename,tablename,index=False):
    try:
        pd.io.sql.to_sql(frame=dataframe, name=tablename, con=engine, schema= databasename, if_exists='append', index=index) 
        print('save to sql table %s successed!' %tablename)
    except exc.IntegrityError:
        #Ignore duplicates
        print ("duplicated data in " + tablename)
        pass
    except:
        print('To SQL Database Failed')
    finally:
        pass
    return 1

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

# 获取指定日期的分析统计结果
def jiaolongchulai (date):
    date_now = date
    analysisfilename = basedir+'/dailyanalysis/'+ str(date_now) + '_jiaolongchulai.csv'
    engine = sqlalchemy.create_engine(sqlenginestr)
    
    # 读取limit up ts_code
    sql = '''SELECT * FROM msstock.tb_daily_limit_up where trade_date = '%s';''' %date_now
    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)

    for tscode in df['ts_code']:
        
        df_up = df[df['pct_chg'] > 0.00]
        df_down = df[df['pct_chg'] < 0.00]
        df_limit_up = df[(((df['market']=='创业板') | (df['market']=='科创板')) & (df['pct_chg'] >= 19.7) & (df['pct_chg'] < 20.1)) | (((df['market']!='创业板') & (df['market']!='科创板')) & (df['pct_chg'] >= 9.7) & (df['pct_chg'] < 10.1))]
        df_limit_up_new = df_limit_up[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]


        myprint('General Data:',analysisfilename)
        if df_output_general['amount'].sum() == 0:
            print ('Empty data in ' + date_now)
        else:
            df_output_general.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')
            save_to_sql(df_output_general,engine,databasename,'tb_daily_general_data')
        
        myprint('Limit up:',analysisfilename)
        if df_limit_up.empty:
            print ('Empty data in ' + date_now)
        else:
            df_limit_up.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')
            save_to_sql(df_limit_up,engine,databasename,'tb_daily_limit_up')

            myprint('Limit up Group:',analysisfilename)
            for group in ['industry']:
                group_limit_up = get_group_output(df_limit_up, date_now, columns=group, name='limit_up')
                group_limit_up.to_csv(analysisfilename, index=True, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')
                save_to_sql(group_limit_up,engine,databasename,'tb_daily_group_by_%s_limit_up' %group,True)

if __name__ == '__main__':
    print('start...')
    print('analyze daily data')
    fmt = '%Y%m%d'
    #start_date = '20210422'
    #end_date = '20210422'
    #start=datetime.datetime.strptime(start_date,fmt)
    #end=datetime.datetime.strptime(end_date,fmt)
    end = datetime.datetime.now()
    start=datetime.datetime.now() -datetime.timedelta(days = 1)

    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)  
        daily_analysis(date_str)
    print('end')