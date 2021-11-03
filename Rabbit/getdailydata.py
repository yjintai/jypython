import tushare as ts
import pandas as pd
import time
import os

inputdatadir = 'D:/Works/python/test/input_data'
pd.set_option('expand_frame_repr', False)

# 从tushare获取指定日期的数据
def get_today_all_ts(date):
    date_now = date
    pro = ts.pro_api('e239683c699765e4e49b43dff2cf7ed7fc232cc49f7992dab1ab7624')
    df_daily = pro.daily(trade_date=date_now)
    df_daily_basic = pro.daily_basic(trade_date=date_now)
    df_basics = pro.stock_basic()
    df_all = pd.merge(left=df_daily, right=df_daily_basic, on='ts_code', how='outer')
    df_all = pd.merge(left=df_all, right=df_basics, on='ts_code', how='outer')
    df_all['ts_code'] = df_all['ts_code'].astype(str) + ' '

    # 保存数据
    df_all.to_csv(inputdatadir+'/'+ str(date_now) + '_ts.csv', index=False, encoding='utf_8_sig')
    print('%sis downloaded.' % (str(date_now)))
    print(df_all)
    return df_all

if __name__ == '__main__':
    print('start...')
    print('get daily data')
    get_today_all_ts(date='20210909')
    print('end')
