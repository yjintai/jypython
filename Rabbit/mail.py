#!/usr/bin/python3
# coding:utf-8
# -*- coding: utf-8 -*-

from email.mime.text import MIMEText
import smtplib
basedir = 'D:/Works/python/report/dailyanalysis/'

#发件人列表
to_list=["228442236@qq.com","yjintai@126.com"]
mail_server="smtp.126.com" # 126的邮件服务器
mail_user="yjintai@126.com" #必须是真实存在的用户
mail_passwd="STBLWUMPTXXVWQWP" #
def get_content_from_file(filename):
    content = ""
    try:
        if filename =="":
            print("The file does not exist.")
        else:
            file = open(filename,'rt')
            content = file.read()
    except IOError:
        print("Read file failed.")
    return content
def send_mail(subject,content):
    if content == "":
        print("No report file today.")
        return False
    else:
        me=mail_user+"<"+mail_user+">"
        msg = MIMEText(content,'html','utf-8')
        msg['Subject'] = subject
        msg['From'] = me
        msg['To'] = ";".join(to_list)
        try:
            s = smtplib.SMTP()
            s.connect(mail_server)
            s.login(mail_user,mail_passwd)
            s.sendmail(me, to_list, msg.as_string())
            s.close()
            return True
        except Exception as e:
            print ("Error:",e)
            return False
def get_report_filename(date):
    filename = basedir+date+'_report.html'
    return filename
if __name__ == '__main__':
    filename = basedir+ '20220408_report.html'
    content = get_content_from_file(filename)
    if send_mail("Daily Report",content):
        print ("发送成功")
    else:
        print ("发送失败")