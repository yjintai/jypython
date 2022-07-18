#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from numpy import str0
import pymssql
import time
import csv

class SQLServer:   
    def __init__(self,server,user,password,database):
    # 类的构造函数，初始化DBC连接信息
        self.server = server
        self.user = user
        self.password = password
        self.database = database

    def __GetConnect(self):
    # 得到数据库连接信息，返回conn.cursor()
        if not self.database:
             raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(server=self.server,user=self.user,password=self.password,database=self.database)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")  # 将DBC信息赋值给cur
        else:
            print ("SQL Connect Successfully!")
            return cur
    def ExecQuery(self,sql):
    #执行查询语句    返回一个包含tuple的list，list是元素的记录行，tuple记录每行的字段数值
        cur = self.__GetConnect()
        cur.execute(sql) # 执行查询语句
        result = cur.fetchall() # fetchall()获取查询结果
        # 查询完毕关闭数据库连接
        self.conn.close()
        return result
    def save_to_file(self,folder,filename,filevalue):
        try:
            fo = open(folder+filename,'w')
            fo.write(filevalue)
            fo.close()
        except IOError:
            print("Cannot open the file.")
        return
def getfsnfromcsv(csvfile):
    #打开csv文件
    with open(csvfile,'r') as f:
        reader = csv.reader(f)      # 生成阅读器，f对象传入
        header_row = next(reader)   # 查看文件第一行，reader是可迭代对象
        fsns = []
        for row in reader:
            fsn = row[0]
            fsns.append(fsn)
    return fsns

def getFACTdata(conn):
        sql = '''SELECT TOP(10000)
        [TBL_GENERAL_DATA].[G_Test_RunID]
        ,[TBL_GENERAL_DATA].[G_FSN]
        ,[TBL_GENERAL_DATA].[DB_Date_Time_Inserted]
        ,[TBL_GENERAL_DATA].[G_IMEI]
        ,[TBL_GENERAL_DATA].[G_Error_1_Code]
        ,[TBL_GENERAL_DATA].[G_Station_Type]
	,[TBL_FACT_LOG_DATA].[G_Test_LogFile_Text]
	,[TBL_GENERAL_DATA].[G_Computer_Name]
      FROM mfgt_db_Production.[dbo].[TBL_GENERAL_DATA]
      INNER JOIN mfgt_db_Production.[dbo].[TBL_FACT_LOG_DATA]
      ON [TBL_GENERAL_DATA].[G_Test_RunID] = [TBL_FACT_LOG_DATA].[G_Test_RunID] 
      WHERE [TBL_GENERAL_DATA].[G_Station_Type] = 'FACT'
      AND  [TBL_GENERAL_DATA].[G_Error_1_Code] = '20'
      AND [TBL_GENERAL_DATA].[DB_Date_Time_Inserted] < '2022-7-20' AND [TBL_GENERAL_DATA].[DB_Date_Time_Inserted] > '2022-6-13'
      order by [TBL_GENERAL_DATA].[DB_Date_Time_Inserted]'''
        rows = conn.ExecQuery(sql)
        for row in rows:
            fsn = row[1]
            dt = row[2]
            pc = row[7]
            #timearray = time.strptime(str(dt), "%Y-%m-%d %H:%M:%S")
            #timestamp = time.mktime(timearray)
            date_str = dt.strftime('%Y%m%d%H%M%S%f')
            stationsubtype = row[5]
            filename = str(date_str) + "_" + pc + "_" + fsn +".log" #CNSUZ-OD-CFG086_2022070701543189UTC_1522740211041018.log
            print(filename)
            if row[6] != "":
                try:
                    conn.save_to_file("Z:\\report\\CpkReport\\Analysis\\20220707_CFG_FPY_Issue\\Logs_SQL\\",filename,row[6]) #"Z:\\report\\CpkReport\\Analysis\\20220707_CFG_FPY_Issue\\Logs_SQL\\"
                except:
                    print ("write to file error")

def main():
    print("start")
    conn = SQLServer(server="10.21.1.1",user="mft-adm",password="MfgTest#5",database="mfgt_db_Production")
    getFACTdata(conn)
    
if __name__ == '__main__':
    main()
