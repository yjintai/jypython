import time
import datetime
import pandas as pd
import logging

import sqlalchemy
from sqlalchemy import exc
import pymysql

databasename = 'msstock'
sqlenginestr='mysql+pymysql://pyuser:Pyuser18@127.0.0.1/'+databasename+'?charset=utf8mb4'

pd.set_option('expand_frame_repr', False)

# 获取指定日期的分析统计结果
def jiaolongchuhai (date_str):

    #analysisfilename = basedir+'/dailyanalysis/'+ str(date_now) + '_jiaolongchulai.csv'
    engine = sqlalchemy.create_engine(sqlenginestr)
    fmt = '%Y%m%d'
    date=datetime.datetime.strptime(date_str,fmt)
    date_list=date -datetime.timedelta(days = 5)
    date_list_str = date_list.strftime(fmt)

    # 读取limit up ts_code
    sql='''select tb_daily_limit_list.ts_code,
    tb_daily_data.low,
    tb_daily_data.amount,    
    tb_stock_basic.name as 名称,
    tb_stock_basic.industry as 板块,
    tb_stock_basic.market as 市场,
    round(tb_daily_data.close,2) as 收盘,
    concat(round(tb_daily_data.pct_chg,2),'%s') as 涨跌幅,
    concat(round(tb_daily_data.turnover_rate,2),'%s') as 换手率,
    round(tb_daily_data.pe,2) as 市盈率,
    round(tb_daily_data.total_mv/1e4,2) as 总市值_亿
    from msstock.tb_daily_limit_list 
    left join msstock.tb_stock_basic 
    on (tb_daily_limit_list.ts_code = tb_stock_basic.ts_code) 
    left join msstock.tb_daily_data
    on (tb_daily_limit_list.ts_code = tb_daily_data.ts_code and tb_daily_limit_list.trade_date = tb_daily_data.trade_date)
    where tb_daily_limit_list.trade_date = '%s' and tb_daily_limit_list.limit = 'U' and tb_daily_limit_list.pct_chg > 6.0 ;''' %('%%','%%',date_str)

    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)
    df_output = pd.DataFrame()
    if df.empty:
        print ('Empty data in ' + date_str)
    else:
        for index, row in df.iterrows():
            sql1='''SELECT * FROM msstock.tb_daily_data
            where ts_code = '%s' and trade_date < '%s' 
            order by trade_date desc limit 10;''' %(row['ts_code'],date_str)
            df1 = pd.read_sql_query(sql1, engine)
            df1.fillna(0, inplace=True)
            df1.replace('nan ', 0, inplace=True)
            try:
                if (row['low'] >  df1['high'][0])  & (row['amount'] < df1['amount'][0]):
                    df_output = df_output.append(df.loc[[index]])
            except:
                print("exception for %s" %row['名称'])
            finally:
                pass
    print ("数量：%d" %df_output.shape[0])
    return df_output

def meas_jiaolongchuhai (data):
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
    '''
    fmt = '%Y%m%d'
    start_date = '20210422'
    end_date = '20210425'
    start=datetime.datetime.strptime(start_date,fmt)
    end=datetime.datetime.strptime(end_date,fmt)
    '''
    end = datetime.datetime.now() -datetime.timedelta(days = 5)
    start=datetime.datetime.now() -datetime.timedelta(days = 5)
    
    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        date_str = '20220408'
        print(date_str)  
        data=jiaolongchuhai(date_str)
        #meas_jiaolongchuhai(data)
    print('end')