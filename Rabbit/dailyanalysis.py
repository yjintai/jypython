import pandas as pd
import requests
import tushare as ts
import os
import time
import warnings

warnings.filterwarnings('ignore')
pd.set_option('expand_frame_repr', False)
basedir = 'D:/Works/python/test'

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

# 获取指定列的分析统计结果
def daily_analysis (date):
    date_now = date
    analysisfilename = basedir+'/dailyanalysis/'+ str(date) + '_report.csv'
    
    myprint(str(date_now),analysisfilename, 'wt')
    # 读取TDX的每日数据
    df = pd.read_csv(basedir + '/input_data/{}_ts.csv'.format(str(date_now)), encoding='utf_8_sig')
    df.fillna(0, inplace=True)
    df.replace('nan ', 0, inplace=True)

    df_up = df[df['pct_chg'].astype(float) > 0.00]
    df_down = df[df['pct_chg'].astype(float)< 0.00]
    df_limit_up = df[(((df['market']=='创业板') | (df['market']=='科创板')) & (df['pct_chg'] >= 19.7)) | (((df['market']!='创业板') & (df['market']!='科创板')) & (df['pct_chg'] >= 9.7))]
    df_limit_up_new = df_limit_up[pd.to_datetime(pd.to_datetime(df_limit_up['trade_date_x'])) - pd.to_datetime(pd.to_datetime(df_limit_up['list_date'])) <= pd.Timedelta(30)]

    df_main = df[((df['market']!='创业板') & (df['market']!='科创板'))]
    df_up_main = df_up[((df_up['market']!='创业板') & (df_up['market']!='科创板'))]
    df_down_main = df_down[((df_down['market']!='创业板') & (df_down['market']!='科创板'))]
    df_limit_up_main = df_up_main[(df_up_main['pct_chg'] >= 9.7)]
    df_limit_up_new_main = df_limit_up_main[pd.to_datetime(pd.to_datetime(df_limit_up_main['trade_date_x'])) - pd.to_datetime(pd.to_datetime(df_limit_up_main['list_date'])) <= pd.Timedelta(30)]

    df_chuangye = df[(df['market']=='创业板')]
    df_up_chuangye = df_up[(df_up['market']=='创业板')]
    df_down_chuangye = df_down[(df_down['market']=='创业板')]
    df_limit_up_chuangye = df_up_chuangye[(df_up_chuangye['pct_chg'] >= 19.7)]
    df_limit_up_new_chuangye= df_limit_up_chuangye[pd.to_datetime(pd.to_datetime(df_limit_up_chuangye['trade_date_x'])) - pd.to_datetime(pd.to_datetime(df_limit_up_chuangye['list_date'])) <= pd.Timedelta(30)]

    df_kechuang = df[(df['market']=='科创板')]
    df_up_kechuang = df_up[(df_up['market']=='科创板')]
    df_down_kechuang = df_down[(df_down['market']=='科创板')]
    df_limit_up_kechuang = df_up_kechuang[(df_up_kechuang['pct_chg'] >= 19.7)]
    df_limit_up_new_kechuang= df_limit_up_kechuang[pd.to_datetime(pd.to_datetime(df_limit_up_kechuang['trade_date_x'])) - pd.to_datetime(pd.to_datetime(df_limit_up_kechuang['list_date'])) <= pd.Timedelta(30)]



    df_output_general = pd.DataFrame(columns=('market','up', 'down', 'amount', 'limit up', 'new limit up'))
    df_output_general.loc[1] = ['All',df_up.shape[0],df_down.shape[0],df['amount'].sum()/10,df_limit_up.shape[0],df_limit_up_new.shape[0]]
    df_output_general.loc[2] = ['ZhuBan',df_up_main.shape[0],df_down_main.shape[0],df_main['amount'].sum()/10,df_limit_up_main.shape[0],df_limit_up_new_main.shape[0]]
    df_output_general.loc[3] = ['ChuangYe',df_up_chuangye.shape[0],df_down_chuangye.shape[0],df_chuangye['amount'].sum()/10,df_limit_up_chuangye.shape[0],df_limit_up_new_chuangye.shape[0]]
    df_output_general.loc[4] = ['KeChuang',df_up_kechuang.shape[0],df_down_kechuang.shape[0],df_kechuang['amount'].sum()/10,df_limit_up_kechuang.shape[0],df_limit_up_new_kechuang.shape[0]]

    df_output_general[['up', 'down', 'amount', 'limit up', 'new limit up']] = df_output_general[['up', 'down', 'amount', 'limit up', 'new limit up']].astype(int)
    myprint('General Data 盘面：',analysisfilename)
    df_output_general.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'w', float_format = '%.2f')
    

    myprint('Limit up:',analysisfilename)
    df_limit_up.sort_values('pct_chg', ascending=False, inplace=True)
    df_limit_up_log = df_limit_up.loc[:,['ts_code','name','close_x','pct_chg','turnover_rate','volume_ratio','pe','total_mv','circ_mv','area','industry','market','list_date']]
    #df_limit_up_log = df_limit_up.loc[:,['symbol','name','close_x','pct_chg']]
    df_limit_up_log.rename(columns={'close_x': 'close'}, inplace=True)
    #df_limit_up_log['symbol'] = df['symbol'].astype(str)

    df_limit_up_log.to_csv(analysisfilename, index=False, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')

    myprint('Limit up Group:',analysisfilename)
    for i in ['industry']:
        output_limit_up = get_group_output(df_limit_up, columns=i, name='limit_up')
        output_limit_up.to_csv(analysisfilename, index=True, encoding='utf_8_sig',mode = 'a', float_format = '%.2f')
    
if __name__ == '__main__':
    print('start...')
    print('analyze daily data')
    daily_analysis(date='20210414')
    print('end')