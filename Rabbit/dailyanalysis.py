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
    pd.set_option('display.max_colwidth', 180)
    html_table = df.to_html(border = '1', index = True)
    html_table1 = html_table.replace('class', 'cellspacing=\"1\" cellpadding=\"3\" style="border-color: red" class')
    html_table2 = html_table1.replace('<tr', '<tr style=\"text-align: center;\"')

    html_string = html_start + html_table2 + html_end
    with open(htmlfilename, mode = mode) as f:
        f.write(html_string)
    return TRUE


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
    myprint('General Data:')
    sql = '''SELECT market as 市场,
    up as 涨,
    down as 跌,
    limit_up as 涨停,
    new_limit_up as 新股涨停
    FROM msstock.tb_daily_general_data where trade_date = "%s";''' %date_now
    df_output_general = pd.read_sql_query(sql, engine)
    df_output_general.fillna(0, inplace=True)
    df_output_general.replace('nan ', 0, inplace=True)
    savetoreport (date_now,df_output_general,"每日涨跌基本数据",mode = 'a')
    
    myprint('Limit up:')
    sql = '''
    select name as 名称,
    industry as 板块,
    market as 市场,
    round(close,2) as 收盘,
    concat(round(pct_chg,2),'%s') as 涨跌幅,
    concat(round(turnover_rate,2),'%s') as 换手率,
    round(pe,2) as 市盈率
    from msstock.tb_daily_limit_up where trade_date = "%s"
    order by pct_chg desc;''' %('%%','%%',date_now)
    df_limit_up = pd.read_sql_query(sql, engine)
    df_limit_up.fillna(0, inplace=True)
    df_limit_up.replace('nan ', 0, inplace=True)
    savetoreport (date_now,df_limit_up,"每日涨停",mode = 'a')
        
    myprint('Limit up Group:')
    for group in ['industry']:
        sql = '''
        SELECT industry as 工业板块,
        count as 涨停数
        FROM msstock.tb_daily_group_by_%s_limit_up
        where trade_date = "%s"
        order by count desc;''' %(group,date_now)
        group_limit_up = pd.read_sql_query(sql, engine)
        group_limit_up.fillna(0, inplace=True)
        group_limit_up.replace('nan ', 0, inplace=True)
        savetoreport (date_now,group_limit_up,"每日涨停",mode = 'a')   

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
    end = datetime.datetime.now() -datetime.timedelta(days = 1)
    start=datetime.datetime.now() -datetime.timedelta(days = 1)

    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)  
        daily_analysis(date_str)
    print('end')