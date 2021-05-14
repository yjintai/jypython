import time
import datetime
import pandas as pd
import logging

import sqlalchemy
from sqlalchemy import exc
import pymysql
import utils

databasename = 'msstock'
sqlenginestr='mysql+pymysql://root:root@127.0.0.1/'+databasename+'?charset=utf8mb4'

pd.set_option('expand_frame_repr', False)

# 获取指定日期的分析统计结果
def chaodiefantan (date_str):

    #analysisfilename = basedir+'/dailyanalysis/'+ str(date_now) + '_chaodiefantan.csv'
    engine = sqlalchemy.create_engine(sqlenginestr)
    fmt = '%Y%m%d'
    date_now=datetime.datetime.strptime(date_str,fmt)
    date_base=utils.business_day_offset (date_now, -250)
    date_base_str = date_base.strftime(fmt)
    print ('base date:%s' %date_base_str)

    # 读取limit up ts_code
    sql = '''SELECT tb_daily_data.ts_code,tb_stock_basic.name,tb_daily_data.trade_date,tb_daily_data.close,tb_daily_data.adj_factor 
            FROM msstock.tb_daily_data 
            left join msstock.tb_stock_basic 
            on (tb_daily_data.ts_code = tb_stock_basic.ts_code) 
            where tb_daily_data.trade_date = '%s' 
            order by tb_daily_data.ts_code;''' %(date_str)
    df = pd.read_sql_query(sql, engine)
    df_output = pd.DataFrame()
    for index, row in df.iterrows():
        sql1='''SELECT ts_code,trade_date,close,adj_factor FROM msstock.tb_daily_data where ts_code = '%s' and trade_date < '%s' order by trade_date desc limit 5;''' %(row['ts_code'],date_str)
        df1 = pd.read_sql_query(sql1, engine)
        if not df1.empty:
            i = df1.shape[0] -1
            calculator = (row['close']*row['adj_factor'])/(df1['close'][i]*df1['adj_factor'][i])
            if (calculator< 0.70):
                df_output = df_output.append(df.loc[[index]])
    print (df_output)
    df_output1 = pd.DataFrame()
    for index, row in df_output.iterrows():
        sql1='''SELECT ts_code,trade_date,close,adj_factor FROM msstock.tb_daily_data where ts_code = '%s' and trade_date < '%s' order by trade_date desc limit 5;''' %(row['ts_code'],date_base_str)
        df1 = pd.read_sql_query(sql1, engine)
        if not df1.empty:
            calculator = (row['close']*row['adj_factor'])/(df1['close'][0]*df1['adj_factor'][0])
            if (calculator< 1):
                df_output1 = df_output1.append(df_output.loc[[index]])
    print (df_output1)
    return df_output1

def measuremnet (data):
    if data.shape[0] == 0:
        print ('Empty Data!')
        return
    engine = sqlalchemy.create_engine(sqlenginestr)
    df_output = pd.DataFrame(columns=('ts_code','name','trade_date','pct_chg_date_1', 'pct_chg_date_2', 'pct_chg_date_3', 'pct_chg_date_4', 'pct_chg_date_5'))
    i =0
    for index, row in data.iterrows():
        sql1='''SELECT * FROM msstock.tb_daily_data where ts_code = '%s' and trade_date > '%s' order by trade_date limit 5;''' %(row['ts_code'],row['trade_date'])
        df1 = pd.read_sql_query(sql1,engine)
        df1.fillna(0, inplace=True)
        df1.replace('nan ', 0, inplace=True)
        df_output = df_output.append({'ts_code':row['ts_code'],'name':row['name'],'trade_date':row['trade_date']}, ignore_index= True)   
        for j in range(df1.shape[0]):
            df_output['pct_chg_date_%d' %(j+1)][i] = df1['pct_chg'][j]
        i +=1
    print(df_output)
    logging.debug (df_output)

if __name__ == '__main__':
    logging.debug('start...')
    print('analyze daily data')

    start_date = '20210427'
    end_date = '20210427'
    start=datetime.datetime.strptime(start_date,'%Y%m%d')
    end=datetime.datetime.strptime(end_date,'%Y%m%d')

    #end = datetime.datetime.now()
    #start=end -datetime.timedelta(days = 2)
    
    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str) 
        if utils.is_business_day(date):
            data=chaodiefantan(date_str)
            measuremnet(data)
        else:
            print ('Not Business day!')
    print('end')