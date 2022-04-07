#!/usr/bin/python3
# coding:utf-8
# -*- coding: utf-8 -*-
import os
from pickle import TRUE
import utils
import logging
import settings
import schedule
import time
import datetime

import getdatatosql as gdts
import dailyanalysis as da
import mail as mail



def job():
    if utils.is_weekday():
        #work_flow.process()
        return TRUE

if __name__ == '__main__':
    
    print('开始')
    end = datetime.datetime.now() -datetime.timedelta(days = 0)
    start=datetime.datetime.now() -datetime.timedelta(days = 0)
    end_date = end.strftime('%Y%m%d')
    start_date = start.strftime('%Y%m%d')
    #start_date = '20150515'
    #end_date = '20150630'
    
    print('获取列表...')
    gdts.process_daily(start_date,end_date)
    gdts.process_weekly(start_date,end_date)
    print('处理数据...')
    for i in range((end - start).days+1):
        date = start + datetime.timedelta(days=i)
        date_str = date.strftime('%Y%m%d')
        print(date_str)  
        da.daily_analysis(date_str)
        reportfilename = mail.get_report_filename(date_str)
        content = mail.get_content_from_file(reportfilename)
        if mail.send_mail("Daily Report",content):
            print ("发送成功")
        else:
            print ("发送失败")
    print('结束')