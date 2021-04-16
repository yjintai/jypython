import tushare as ts
import os

import getdailydata as gdd
import dailyanalysis as da

if __name__ == '__main__':
    print('start...')
    '''
    print('get daily data')
    get_today_all_ts(date='20210412')
    '''
    print('analyze daily data')
    da.daily_analysis(date='20210412')

    print('end')
