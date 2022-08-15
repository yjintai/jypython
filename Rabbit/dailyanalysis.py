from pickle import FALSE, TRUE
import time
import datetime
import pandas as pd
import requests
import os
import warnings

import utils
import sqlalchemy
from sqlalchemy import engine_from_config, exc
import pymysql
import jiaolongchuhai as jlch

databasename = 'msstock'
sqlenginestr='mysql+pymysql://pyuser:Pyuser18@127.0.0.1/'+databasename+'?charset=utf8mb4'

warnings.filterwarnings('ignore')
pd.set_option('expand_frame_repr', False)
basedir = 'D:/Works/python/report/dailyanalysis/'

def initiate():
    engine=sqlalchemy.create_engine(sqlenginestr)
    return engine

def myprint (str):
    print (str)

def savetoreport (date_now,df,tablename,mode = 'w',end = False):
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
        <style>
            table {
                background-color: gray;
                text-align: center;
            }
            
            table th {
                background-color: #ffffff;
            }
            
            table td {
                background-color: #ffffff;
            }
        </style>
        <body>
        <DIV align="center">
        <TABLE CELLSPACING="0" CELLPADDING="0" LANG="en-US">
        <TR><TD>
        <font size="6" color = "BLUE">%s Report</font>
        </TD></TR>
        </TABLE>
        </DIV>
        <HR/><TABLE cellspacing="0" border="0"><TR><TD><font size="4" color = "BLUE">%s</font></TD></TR></TABLE>
        '''%(date_now,tablename)
    else:
        html_start = '''
        <HR/><TABLE cellspacing="0" border="0"><TR><TD><font size="4" color = "BLUE">%s</font></TD></TR></TABLE>
        '''%tablename
    if end == True:
        html_end = '''
        </body></html>'''
    pd.set_option('display.max_colwidth', 300)
    html_table = df.to_html(border = '0', index = False)
    html_table1 = html_table.replace('class', 'cellspacing=\"1\" cellpadding=\"5\" class')
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

def get_ind_daily_top(engine,date):
    sql = '''SELECT industry,count
        FROM msstock.tb_daily_group_by_industry_limit_up
        where trade_date = "%s"
        order by count desc;''' %(date)
    df = pd.read_sql_query(sql, engine)
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)
    return df


def report_daily_index(engine,date,end = False):
    # 基本指数信息
    date_now = date
    sql = '''SELECT 
	tb_index_basic.name as 名称,
    round(tb_index_daily.close,2) as 收盘,
    concat(round(tb_index_daily.pct_chg,2),'%s') as 涨跌幅,
    round(tb_index_daily.amount/1e5,0) as 成交额_亿,
    round(tb_index_dailybasic.total_mv/1e12,2) as '总市值_万亿',
    tb_index_dailybasic.pe as 市盈率,
    tb_index_dailybasic.pb as 市净率
    FROM msstock.tb_index_daily
    left join msstock.tb_index_basic
    on (tb_index_daily.ts_code = tb_index_basic.ts_code)
    left join msstock.tb_index_dailybasic 
    on (tb_index_daily.ts_code = tb_index_dailybasic.ts_code and tb_index_daily.trade_date = tb_index_dailybasic.trade_date) 
    where tb_index_daily.ts_code!= '000005.SH' and tb_index_daily.trade_date = "%s"
    order by tb_index_daily.ts_code;'''%('%%',date_now)
    df_index = pd.read_sql_query(sql, engine)
    df_index.fillna(0, inplace=True)
    df_index.replace('nan ', 0, inplace=True)
    myprint('Index Data：')
    if df_index.empty:
        print ('Empty data in ' + date_now)
    else:
        savetoreport (date_now,df_index,"指数信息",mode = 'w', end = end)
    return df_index
    
def report_ths_daily (engine,date,end = False):
    # 同花顺指数热度
    date_now = date
    df_ths_days = pd.DataFrame(columns=('Trade_Date','Top1','Top2','Top3','Top4','Top5','Top6','Top7','Top8','Top9','Top10'))
    end=datetime.datetime.strptime(date,'%Y%m%d')
    begin=end - datetime.timedelta(days = 20)
    myprint('THS Group:')
    for i in range((end - begin).days+1):
        date_i = begin + datetime.timedelta(days=i)
        date_str = date_i.strftime('%Y%m%d')
        df = get_ths_daily_top(engine,date_str)
        print(date_str)  
        if not df.empty:
            df_ths_days = df_ths_days.append({'Trade_Date':date_str}, ignore_index= True)   
            for j in range(10):
                #df_ths_days['Top%d' %(j+1)][df_ths_days.shape[0]-2] = df['name'][j]
                df_ths_days['Top%d' %(j+1)][df_ths_days.shape[0]-1] = '%s(%.2f)'%(df['name'][j],(df['pct_change'][j]))
    if df_ths_days.empty:
        print ('Empty data in ' + date_now)
    else:
        savetoreport (date,df_ths_days,"同花顺板块热度",mode = 'a', end = end)
    return df_ths_days

def report_ind_daily (engine,date,end = False):
    # 工业板块热度
    date_now = date
    df_ind_days = pd.DataFrame(columns=('Trade_Date','Top1','Top2','Top3','Top4','Top5','Top6','Top7','Top8','Top9','Top10'))
    end=datetime.datetime.strptime(date,'%Y%m%d')
    begin=end - datetime.timedelta(days = 20)
    myprint('Industry Group:')
    for i in range((end - begin).days+1):
        date_i = begin + datetime.timedelta(days=i)
        date_str = date_i.strftime('%Y%m%d')
        df = get_ind_daily_top(engine,date_str)
        print(date_str)  
        if not df.empty:
            df_ind_days = df_ind_days.append({'Trade_Date':date_str}, ignore_index= True)   
            for j in range(10):
                df_ind_days['Top%d' %(j+1)][df_ind_days.shape[0]-1] = '%s(%d)'%(df['industry'][j],(df['count'][j]))
    if df_ind_days.empty:
        print ('Empty data in ' + date_now)
    else:
        savetoreport (date,df_ind_days,"工业板块热度",mode = 'a', end = end)
    return df_ind_days

def report_market_daily(engine,date,end = False):
    date_str = date
    # TDX的每日数据
    print('General Data:')
    sql = '''SELECT tb_daily_data.ts_code, 
        tb_daily_data.trade_date, 
        tb_daily_data.pct_chg,
        tb_stock_basic.industry, 
        tb_stock_basic.market,
        tb_stock_basic.list_date
        from msstock.tb_daily_data 
        left join msstock.tb_stock_basic 
        on (tb_daily_data.ts_code = tb_stock_basic.ts_code) 
        where tb_daily_data.trade_date = "%s" ;''' %date_str
    df = pd.read_sql_query(sql, engine)
    df_up = df[df['pct_chg'] > 0.00]
    df_down = df[df['pct_chg'] < 0.00]
    sql = '''SELECT tb_daily_limit_list.ts_code,
        tb_daily_limit_list.trade_date,
        tb_stock_basic.industry, 
        tb_stock_basic.market, 
        tb_stock_basic.list_date
        FROM msstock.tb_daily_limit_list 
        left join msstock.tb_stock_basic 
        on (tb_daily_limit_list.ts_code = tb_stock_basic.ts_code) 
        where tb_daily_limit_list.trade_date = '%s' and tb_daily_limit_list.limit = 'U' and tb_daily_limit_list.pct_chg > 6.0;
        ''' %date_str
    df_limit_up = pd.read_sql_query(sql, engine)
    df_limit_up.fillna(0, inplace=True)
    df_limit_up.replace('nan ', 0, inplace=True)
    df_limit_up_new = df_limit_up[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(30)]
    df_limit_up = df_limit_up[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up['list_date']) > pd.Timedelta(30)]

    sql = '''SELECT tb_daily_limit_list.ts_code,
        tb_daily_limit_list.trade_date,
        tb_stock_basic.industry, 
        tb_stock_basic.market, 
        tb_stock_basic.list_date
        FROM msstock.tb_daily_limit_list 
        left join msstock.tb_stock_basic 
        on (tb_daily_limit_list.ts_code = tb_stock_basic.ts_code) 
        where tb_daily_limit_list.trade_date = '%s' and tb_daily_limit_list.limit = 'D' and tb_daily_limit_list.pct_chg < 6.0;
        ''' %date_str
    df_limit_down = pd.read_sql_query(sql, engine)
    df_limit_down.fillna(0, inplace=True)
    df_limit_down.replace('nan ', 0, inplace=True)
    df_limit_down_new = df_limit_down[pd.to_datetime(date_str) - pd.to_datetime(df_limit_down['list_date']) <= pd.Timedelta(30)]
    df_limit_down = df_limit_down[pd.to_datetime(date_str) - pd.to_datetime(df_limit_down['list_date']) > pd.Timedelta(30)]
    

    df_up_main = df_up[((df_up['market']!='创业板') & (df_up['market']!='科创板'))]
    df_down_main = df_down[((df_down['market']!='创业板') & (df_down['market']!='科创板'))]
    df_limit_up_main = df_limit_up[((df_limit_up['market']!='创业板') & (df_limit_up['market']!='科创板'))]
    df_limit_down_main = df_limit_down[((df_limit_down['market']!='创业板') & (df_limit_down['market']!='科创板'))]
    
    df_limit_up_new_main = df_limit_up_main[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up_main['list_date']) <= pd.Timedelta(365)]

    df_up_chuangye = df_up[(df_up['market']=='创业板')]
    df_down_chuangye = df_down[(df_down['market']=='创业板')]
    df_limit_up_chuangye = df_limit_up[(df_limit_up['market']=='创业板')]
    df_limit_down_chuangye = df_limit_down[(df_limit_down['market']=='创业板')]
    df_limit_up_new_chuangye= df_limit_up_chuangye[(pd.to_datetime(date_str) - pd.to_datetime(df_limit_up_chuangye['list_date']) <= pd.Timedelta(365))]

    df_up_kechuang = df_up[(df_up['market']=='科创板')]
    df_down_kechuang = df_down[(df_down['market']=='科创板')]
    df_limit_up_kechuang = df_limit_up[(df_limit_up['market']=='科创板')]
    df_limit_down_kechuang = df_limit_down[(df_limit_down['market']=='科创板')]
    df_limit_up_new_kechuang= df_limit_up_kechuang[(pd.to_datetime(date_str) - pd.to_datetime(df_limit_up_kechuang['list_date'])) <= pd.Timedelta(365)]

    df_output_general = pd.DataFrame(columns=('市场','张', '跌', '涨停', '跌停' ))
    df_output_general.loc[1] = ['全部',df_up.shape[0],df_down.shape[0],df_limit_up.shape[0],df_limit_down.shape[0]]
    df_output_general.loc[2] = ['主板',df_up_main.shape[0],df_down_main.shape[0],df_limit_up_main.shape[0],df_limit_down_main.shape[0]]
    df_output_general.loc[3] = ['创业板',df_up_chuangye.shape[0],df_down_chuangye.shape[0],df_limit_up_chuangye.shape[0],df_limit_down_chuangye.shape[0]]
    df_output_general.loc[4] = ['科创板',df_up_kechuang.shape[0],df_down_kechuang.shape[0],df_limit_up_kechuang.shape[0],df_limit_down_kechuang.shape[0]]
    
    savetoreport (date_str,df_output_general,"每日涨跌基本数据",mode = 'a', end = end)
        
def report_market_daily_from_sql(engine,date,end = False):
    date_now = date
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
    savetoreport (date_now,df_output_general,"每日涨跌基本数据",mode = 'a', end = end)

#获取指定日期涨停最多的4个工业板块的涨停股票和它们的连扳数。
def report_limit_daily (engine,date,end = False):
    date_now = date
    myprint('Limit up list:')
    df_ind_daily_top = get_ind_daily_top(engine,date_now) 
    #if not df.empty:
    sql = '''
        select tb_daily_limit_list.ts_code,
        tb_daily_limit_list.name as 名称,
        tb_stock_basic.industry as 板块, 
        round(tb_daily_limit_list.close,2) as 收盘,
        concat(round(tb_daily_limit_list.pct_chg,2),'%s') as 涨跌幅,
        concat(round(tb_daily_limit_list.fl_ratio,2),'%s') as 封单量比,
        round(tb_daily_limit_list.strth,2) as 强度,
        round(tb_daily_data.pe,2) as 市盈率,
        round(tb_daily_data.total_mv/1e4,2) as 总市值_亿
        from msstock.tb_daily_limit_list 
        left join msstock.tb_stock_basic 
        on (tb_daily_limit_list.ts_code = tb_stock_basic.ts_code) 
        left join msstock.tb_daily_data
        on (tb_daily_limit_list.ts_code = tb_daily_data.ts_code and tb_daily_limit_list.trade_date = tb_daily_data.trade_date)
        where tb_daily_limit_list.trade_date = '%s' and tb_daily_limit_list.limit = 'U' and tb_daily_limit_list.pct_chg > 6.0
        and (tb_stock_basic.industry = '%s' or tb_stock_basic.industry = '%s' or tb_stock_basic.industry = '%s' or tb_stock_basic.industry = '%s') 
        order by tb_stock_basic.industry;
        ''' %('%%','%%',date_now,df_ind_daily_top.iloc[0]['industry'],df_ind_daily_top.iloc[1]['industry'],df_ind_daily_top.iloc[2]['industry'],df_ind_daily_top.iloc[3]['industry'] )


    df_limit_up = pd.read_sql_query(sql, engine)
    df_limit_up.fillna(0, inplace=True)
    df_limit_up.replace('nan ', 0, inplace=True)
    df_limit_up = df_limit_up[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(30)]
    

    df_limit_up.insert(loc =3, column = '连板', value = '')
    for i in range(0, len(df_limit_up)): 
        ts_code = df_limit_up.iloc[i]['ts_code']
        qty_limit_up = utils.get_qty_limit_up(ts_code,date)
        df_limit_up['连板'][i] = qty_limit_up
    df_limit_up = df_limit_up.sort_values(by=['板块','连板'],ascending=False)
    savetoreport (date_now,df_limit_up,"每日涨停",mode = 'a', end = end)

#获取指定日期涨停连扳数的TOP5。
def report_limit_TOPn_daily (engine,date,end = False):
    date_now = date
    myprint('Limit up TOP n list:')
    #if not df.empty:
    sql = '''
        select tb_daily_limit_list.ts_code,
        tb_daily_limit_list.name as 名称,
        tb_stock_basic.industry as 板块, 
        round(tb_daily_limit_list.close,2) as 收盘,
        concat(round(tb_daily_limit_list.pct_chg,2),'%s') as 涨跌幅,
        concat(round(tb_daily_limit_list.fl_ratio,2),'%s') as 封单量比,
        round(tb_daily_limit_list.strth,2) as 强度,
        round(tb_daily_data.pe,2) as 市盈率,
        round(tb_daily_data.total_mv/1e4,2) as 总市值_亿
        from msstock.tb_daily_limit_list 
        left join msstock.tb_stock_basic 
        on (tb_daily_limit_list.ts_code = tb_stock_basic.ts_code) 
        left join msstock.tb_daily_data
        on (tb_daily_limit_list.ts_code = tb_daily_data.ts_code and tb_daily_limit_list.trade_date = tb_daily_data.trade_date)
        where tb_daily_limit_list.trade_date = '%s' and tb_daily_limit_list.limit = 'U' and tb_daily_limit_list.pct_chg > 6.0
        and tb_daily_data.;
        ''' %('%%','%%',date_now )

    df_limit_up = pd.read_sql_query(sql, engine)
    df_limit_up.fillna(0, inplace=True)
    df_limit_up.replace('nan ', 0, inplace=True)
    df_limit_up = df_limit_up[pd.to_datetime(date_str) - pd.to_datetime(df_limit_up['list_date']) <= pd.Timedelta(30)]
    df_limit_up.insert(loc =3, column = '连板', value = '')
    for i in range(0, len(df_limit_up)): 
        ts_code = df_limit_up.iloc[i]['ts_code']
        qty_limit_up = utils.get_qty_limit_up(ts_code,date)
        df_limit_up['连板'][i] = qty_limit_up
    df_limit_up = df_limit_up.sort_values(by=['连板'],ascending=False)
    savetoreport (date_now,df_limit_up.head(10),"每日涨停板高度",mode = 'a', end = end)

# 获取指定日期的股票的连板数
def get_qty_limit_up(engine,code,date_str):
    qty_limit_up = 1
    previous_date = date_str
    for i in range(20):
        previous_date = utils.get_previous_date(previous_date)
        sql = '''SELECT ts_code,trade_date FROM msstock.tb_daily_limit_list 
            where ts_code = '%s' and trade_date = '%s' and tb_daily_limit_list.limit = 'U';'''%(code,previous_date)
        df = pd.read_sql_query(sql, engine)
        if not df.empty:
            qty_limit_up = qty_limit_up+1
        else:
            break  
    print  ("%s:%d" %(code,qty_limit_up))
    return qty_limit_up

# 获取指定日期的分析统计结果
def jiaolongchuhai (engine,date,end = False):
    date_now = date
    myprint('jiaolongchuhai:')
    df = jlch.jiaolongchuhai(date)
    if not df.empty:
        dfsub = df[['名称','板块','市场','收盘','涨跌幅','换手率','市盈率','总市值_亿']]
        savetoreport (date_now,dfsub,"蛟龙出海",mode = 'a', end = end)   

def daily_analysis (date):
    engine = sqlalchemy.create_engine(sqlenginestr)

    df_index = report_daily_index(engine,date)
    
    if not df_index.empty:
        report_market_daily (engine,date)
        report_limit_daily (engine,date)
        report_limit_TOPn_daily (engine,date)
        report_ind_daily(engine,date)
        #report_ths_daily(engine,date)
        jiaolongchuhai(engine,date, True)
    # 关闭连接
    engine.dispose()

if __name__ == '__main__':
    print('start...')
    print('analyze daily data')
    fmt = '%Y%m%d'
    end = datetime.datetime.now() -datetime.timedelta(days = 0)
    start=datetime.datetime.now() -datetime.timedelta(days = 0)

    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        date_str = '20220718'
        print(date_str)  
        daily_analysis(date_str)
    print('end')