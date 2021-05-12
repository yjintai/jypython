import tushare as ts
import os
import utils
import logging
import work_flow
import settings
import schedule
import time

import getdailydata as gdd
import dailyanalysis as da


logging.basicConfig(format='%(asctime)s %(message)s', filename='Rabbit.log')
logging.getLogger().setLevel(logging.DEBUG)

def job():
    if utils.is_weekday():
        work_flow.process()

if __name__ == '__main__':
    print('start...')
    '''
    print('get daily data')
    get_today_all_ts(date='20210412')
    '''
    print('analyze daily data')
    da.daily_analysis(date='20210412')

    print('end')






#         
#
#
# settings.init()
# EXEC_TIME = "15:15"
# schedule.every().day.at(EXEC_TIME).do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)

settings.init()
work_flow.process()