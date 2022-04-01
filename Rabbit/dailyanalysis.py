from pickle import FALSE, TRUE
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
basedir = 'D:/Works/python/report/dailyanalysis/'

def initiate():
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

def myprint (str):
    print (str)

def savetoreport (date_now,df,tablename,mode = 'w',end = FALSE):
    filename = basedir+ str(date_now) + '_report'
    #csvfilename = filename+'.csv'
    #df.to_csv(csvfilename, index=False, encoding='utf_8_sig',mode = mode, float_format = '%.2f')
    htmlfilename = filename+'.html'
    html_start = ""
    html_end = ""
    if mode == 'w':
        html_start = '''
        <html>
        <head><title></title></head>
        <body>
        <DIV align="center">
        <TABLE CELLSPACING="0" CELLPADDING="0" LANG="en-US" style="">
        <TR><TD>
        <font size="6" color = "BLUE">%s Report</font>
        </TD></TR>
        </TABLE>
        </DIV>
        <HR/><TABLE border="0"><TR><TD><font size="4" color = "BLUE">%s</font></TD></TR></TABLE>
        '''%(date_now,tablename)
    else:
        html_start = '''
        <HR/><TABLE border="0"><TR><TD><font size="4" color = "BLUE">%s</font></TD></TR></TABLE>
        '''%tablename
    if end == TRUE:
        html_end = '''
        </body></html>'''
    html_string = html_start + df.to_html(border = '0') + html_end
    with open(htmlfilename, mode = mode) as f:
        f.write(html_string)
    return TRUE


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

def get_ths_daily_top(engine,date):
    sql = '''SELECT tb_ths_index.name,tb_ths_daily.pct_change
        FROM msstock.tb_ths_daily
        join msstock.tb_ths_index on (tb_ths_daily.ts_code = tb_ths_index.ts_code) 
        where tb_ths_index.exchange = 'A' and tb_ths_index.type = 'N' and tb_ths_daily.trade_date = '%s'
        order by tb_ths_daily.pct_change desc limit 30;'''%date
    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)
    return df


def report_daily_index(engine,date):
    # 基本指数信息
    date_now = date
    sql = '''SELECT 
	tb_index_basic.name as 名称,
    round(tb_index_daily.close,2) as 收盘,
    concat(round(tb_index_daily.pct_chg,2),'%s') as 涨跌幅,
    round(tb_index_dailybasic.total_mv/10e12,2) as 总股本,
    concat(tb_index_dailybasic.turnover_rate_f,'%s') as 换手率,
    tb_index_dailybasic.pe as 市盈率,
    tb_index_dailybasic.pb as 市净率
    FROM msstock.tb_index_daily
    left join msstock.tb_index_basic
    on (tb_index_daily.ts_code = tb_index_basic.ts_code)
    left join msstock.tb_index_dailybasic 
    on (tb_index_daily.ts_code = tb_index_dailybasic.ts_code and tb_index_daily.trade_date = tb_index_dailybasic.trade_date) 
    where tb_index_daily.ts_code!= '000005.SH' and tb_index_daily.trade_date = "%s"
    order by tb_index_daily.ts_code;'''%('%%','%%',date_now)
    df_index = pd.read_sql_query(sql, engine)
    df_index.fillna(0, inplace=True)
    df_index.replace('nan ', 0, inplace=True)
    myprint('Index Data：')
    if df_index.empty:
        print ('Empty data in ' + date_now)
    else:
        savetoreport (date_now,df_index,"指数信息",mode = 'w')
    return df_index
    
def report_ths_daily (engine,date):
    # 同花顺指数热度
    date_now = date
    df_ths_days = pd.DataFrame(columns=('Trade_Date','Top1','Top2','Top3','Top4','Top5','Top6','Top7','Top8','Top9','Top10','Top11','Top12','Top13','Top14','Top15','Top16','Top17','Top18','Top19','Top20'))
    end=datetime.datetime.strptime(date,'%Y%m%d')
    begin=end - datetime.timedelta(days = 30)
    for i in range((end - begin).days+1):
        date_i = begin + datetime.timedelta(days=i)
        date_str = date_i.strftime('%Y%m%d')
        df = get_ths_daily_top(engine,date_str)
        print(date_str)  
        if not df.empty:
            df_ths_days = df_ths_days.append({'Trade_Date':date_str}, ignore_index= True)   
            #df_ths_days = df_ths_days.append({'Trade_Date':date_str}, ignore_index= True) 
            for j in range(20):
                #df_ths_days['Top%d' %(j+1)][df_ths_days.shape[0]-2] = df['name'][j]
                df_ths_days['Top%d' %(j+1)][df_ths_days.shape[0]-1] = '%s(%.2f)'%(df['name'][j],(df['pct_change'][j]))
    myprint('THS Group:')
    if df_ths_days.empty:
        print ('Empty data in ' + date_now)
    else:
        savetoreport (date,df_ths_days,"同花顺板块热度",mode = 'a', end = TRUE)
    return df_ths_days

def report_TDX_daily (engine,date):
    date_now = date
    # TDX的每日数据
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
            order by tb_daily_data.pct_chg desc;''' %date_now
    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)

    df_up = df[df['pct_chg'] > 0.00]
    df_down = df[df['pct_chg'] < 0.00]
    df_limit_up = df[(((df['market']=='创业板') | (df['market']=='科创板')) & (df['pct_chg'] >= 19.7) & (df['pct_chg'] < 20.1)) | (((df['market']!='创业板') & (df['market']!='科创板')) & (df['pct_chg'] >= 9.7) & (df['pct_chg'] < 10.1))]
    df_limit_up_new = df_limit_up[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_main = df[((df['market']!='创业板') & (df['market']!='科创板'))]
    df_up_main = df_up[((df_up['market']!='创业板') & (df_up['market']!='科创板'))]
    df_down_main = df_down[((df_down['market']!='创业板') & (df_down['market']!='科创板'))]
    df_limit_up_main = df_up_main[(df_up_main['pct_chg'] >= 9.7) & (df_up_main['pct_chg'] < 10.1)]
    df_limit_up_new_main = df_limit_up_main[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_chuangye = df[(df['market']=='创业板')]
    df_up_chuangye = df_up[(df_up['market']=='创业板')]
    df_down_chuangye = df_down[(df_down['market']=='创业板')]
    df_limit_up_chuangye = df_up_chuangye[(df_up_chuangye['pct_chg'] >= 19.7) & (df_up_chuangye['pct_chg'] < 20.1)]
    df_limit_up_new_chuangye= df_limit_up_chuangye[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_kechuang = df[(df['market']=='科创板')]
    df_up_kechuang = df_up[(df_up['market']=='科创板')]
    df_down_kechuang = df_down[(df_down['market']=='科创板')]
    df_limit_up_kechuang = df_up_kechuang[(df_up_kechuang['pct_chg'] >= 19.7) & (df_up_kechuang['pct_chg'] < 20.1)]
    df_limit_up_new_kechuang= df_limit_up_kechuang[pd.to_datetime(date_now) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(365)]

    df_output_general = pd.DataFrame(columns=('market','trade_date','up', 'down', 'amount', 'limit up', 'new limit up'))
    df_output_general.loc[1] = ['全部',date_now,df_up.shape[0],df_down.shape[0],df['amount'].sum(),df_limit_up.shape[0],df_limit_up_new.shape[0]]
    df_output_general.loc[2] = ['主板',date_now,df_up_main.shape[0],df_down_main.shape[0],df_main['amount'].sum(),df_limit_up_main.shape[0],df_limit_up_new_main.shape[0]]
    df_output_general.loc[3] = ['创业板',date_now,df_up_chuangye.shape[0],df_down_chuangye.shape[0],df_chuangye['amount'].sum(),df_limit_up_chuangye.shape[0],df_limit_up_new_chuangye.shape[0]]
    df_output_general.loc[4] = ['科创板',date_now,df_up_kechuang.shape[0],df_down_kechuang.shape[0],df_kechuang['amount'].sum(),df_limit_up_kechuang.shape[0],df_limit_up_new_kechuang.shape[0]]
    df_output_general[['up', 'down', 'amount', 'limit up', 'new limit up']] = df_output_general[['up', 'down', 'amount', 'limit up', 'new limit up']].astype(int)
        
    myprint('General Data:')
    if df_output_general['amount'].sum() == 0:
        print ('Empty data in ' + date_now)
    else:
        save_to_sql(df_output_general,engine,databasename,'tb_daily_general_data')
        df_output_general_report = df_output_general.loc[:,['market','up', 'down', 'limit up', 'new limit up']]
        df_output_general_report.columns=['市场','涨', '跌', '涨停', '新股涨停']
        savetoreport (date_now,df_output_general_report,"每日涨跌基本数据",mode = 'a')
    
    myprint('Limit up:')
    if df_limit_up.empty:
        print ('Empty data in ' + date_now)
    else:
        save_to_sql(df_limit_up,engine,databasename,'tb_daily_limit_up')
        savetoreport (date_now,df_limit_up,"每日涨停",mode = 'a')
        

        myprint('Limit up Group:')
        for group in ['industry']:
            group_limit_up = get_group_output(df_limit_up, date_now, columns=group, name='limit_up')
            save_to_sql(group_limit_up,engine,databasename,'tb_daily_group_by_%s_limit_up' %group,True)
            savetoreport (date_now,group_limit_up,"涨停板块统计",mode = 'a')
            

# 获取指定日期的分析统计结果
def daily_analysis (date):
    engine = sqlalchemy.create_engine(sqlenginestr)
    df_index = report_daily_index(engine,date)
    if not df_index.empty:
        report_TDX_daily (engine,date)
        report_ths_daily(engine,date)

if __name__ == '__main__':
    print('start...')
    print('analyze daily data')
    fmt = '%Y%m%d'
    #start_date = '20210422'
    #end_date = '20210422'
    #start=datetime.datetime.strptime(start_date,fmt)
    #end=datetime.datetime.strptime(end_date,fmt)
    end = datetime.datetime.now() -datetime.timedelta(days = 1)
    start=datetime.datetime.now() -datetime.timedelta(days = 1)

    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)  
        daily_analysis(date_str)
    print('end')