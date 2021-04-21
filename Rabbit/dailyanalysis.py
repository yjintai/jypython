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

warnings.filterwarnings('ignore')
pd.set_option('expand_frame_repr', False)
basedir = 'D:/Works/python/jypython/rabbit'

def initiate():
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

def myprint (str,filename, mode = 'a'):
    print (str)

def get_group_output(df, columns='industry', name='_limit_up'):
    df = df.copy()
    output = pd.DataFrame()
    output = pd.DataFrame(df.groupby(columns)['ts_code'].count())
    output['pe_mean'] = df.groupby(columns)['pe'].mean()
    output['pe_median'] = df.groupby(columns)['pe'].median()
    output['pb_mean'] = df.groupby(columns)['pb'].mean()
    output['pb_median'] = df.groupby(columns)['pb'].median()
    output.sort_values('ts_code', ascending=False, inplace=True)
    output.rename(columns={'ts_code': 'count'}, inplace=True)
    return output

# 获取指定日期的分析统计结果
def daily_analysis (date):
    date_now = date
    analysisfilename = basedir+'/dailyanalysis/'+ str(date) + '_report.csv'
    
    myprint(str(date_now),analysisfilename, 'wt')
    engine = sqlalchemy.create_engine(sqlenginestr)
    
    # 读取指数信息
    sql = '''SELECT tb_index_daily.*,
    tb_index_dailybasic.pe,
    tb_index_dailybasic.pb
    FROM msstock.tb_index_daily
    left join msstock.tb_index_dailybasic 
    on (tb_index_daily.ts_code = tb_index_dailybasic.ts_code and tb_index_daily.trade_date = tb_index_dailybasic.trade_date) 
    where tb_index_daily.trade_date = "%s"
    order by tb_index_daily.ts_code;'''%date_now
    df_index = pd.read_sql_query(sql, engine)
    df_index.fillna(0, inplace=True)
    df_index.replace('nan ', 0, inplace=True)
    myprint('Index Data：',analysisfilename)
    df_index.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'w', float_format = '%.2f')

    # 读取TDX的每日数据
    sql = '''SELECT tb_daily_data.ts_code,
            tb_daily_data.trade_date,
            tb_stock_basic.name,
            tb_stock_basic.industry, 
            tb_stock_basic.market,
            tb_daily_data.open,
            tb_daily_data.close,
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
            order by tb_daily_data.pct_chg desc;''' %date_now
    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)

    df_up = df[df['pct_chg'] > 0.00]
    df_down = df[df['pct_chg'] < 0.00]
    df_limit_up = df[(((df['market']=='创业板') | (df['market']=='科创板')) & (df['pct_chg'] >= 19.7)) | (((df['market']!='创业板') & (df['market']!='科创板')) & (df['pct_chg'] >= 9.7))]
    df_limit_up_new = df_limit_up[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_main = df[((df['market']!='创业板') & (df['market']!='科创板'))]
    df_up_main = df_up[((df_up['market']!='创业板') & (df_up['market']!='科创板'))]
    df_down_main = df_down[((df_down['market']!='创业板') & (df_down['market']!='科创板'))]
    df_limit_up_main = df_up_main[(df_up_main['pct_chg'] >= 9.7)]
    df_limit_up_new_main = df_limit_up_main[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_chuangye = df[(df['market']=='创业板')]
    df_up_chuangye = df_up[(df_up['market']=='创业板')]
    df_down_chuangye = df_down[(df_down['market']=='创业板')]
    df_limit_up_chuangye = df_up_chuangye[(df_up_chuangye['pct_chg'] >= 19.7)]
    df_limit_up_new_chuangye= df_limit_up_chuangye[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_kechuang = df[(df['market']=='科创板')]
    df_up_kechuang = df_up[(df_up['market']=='科创板')]
    df_down_kechuang = df_down[(df_down['market']=='科创板')]
    df_limit_up_kechuang = df_up_kechuang[(df_up_kechuang['pct_chg'] >= 19.7)]
    df_limit_up_new_kechuang= df_limit_up_kechuang[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_output_general = pd.DataFrame(columns=('market','up', 'down', 'amount', 'limit up', 'new limit up'))
    df_output_general.loc[1] = ['全部',df_up.shape[0],df_down.shape[0],df['amount'].sum()/10,df_limit_up.shape[0],df_limit_up_new.shape[0]]
    df_output_general.loc[2] = ['主板',df_up_main.shape[0],df_down_main.shape[0],df_main['amount'].sum()/10,df_limit_up_main.shape[0],df_limit_up_new_main.shape[0]]
    df_output_general.loc[3] = ['创业板',df_up_chuangye.shape[0],df_down_chuangye.shape[0],df_chuangye['amount'].sum()/10,df_limit_up_chuangye.shape[0],df_limit_up_new_chuangye.shape[0]]
    df_output_general.loc[4] = ['科创板',df_up_kechuang.shape[0],df_down_kechuang.shape[0],df_kechuang['amount'].sum()/10,df_limit_up_kechuang.shape[0],df_limit_up_new_kechuang.shape[0]]
    df_output_general[['up', 'down', 'amount', 'limit up', 'new limit up']] = df_output_general[['up', 'down', 'amount', 'limit up', 'new limit up']].astype(int)
    

    
    myprint('General Data:',analysisfilename)
    df_output_general.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')
    
    myprint('Limit up:',analysisfilename)

    df_limit_up.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')

    myprint('Limit up Group:',analysisfilename)
    for i in ['industry']:
        output_limit_up = get_group_output(df_limit_up, columns=i, name='limit_up')
        output_limit_up.to_csv(analysisfilename, index=True, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')

if __name__ == '__main__':
    print('start...')
    print('analyze daily data')
    date = datetime.datetime.now().strftime('%Y%m%d')
    daily_analysis(date=date)
    print('end')