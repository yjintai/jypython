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
def getBFTdata(msg,FSNs):
    for FSN in FSNs:
        #For BFT:
        sql = '''SELECT [TBL_GENERAL_DATA].[DB_Date_Time_Inserted]
            ,[TBL_GENERAL_DATA].[G_Station_Type]
            ,[TBL_GENERAL_DATA].[G_Station_SubType]
            ,[TBL_GENERAL_DATA].[G_Test_RunID]
            ,[TBL_GENERAL_DATA].[G_Test_Result]
            ,[TBL_GENERAL_DATA].[G_Test_Duration]
            ,[TBL_GENERAL_DATA].[G_DUT_Type]
            ,[TBL_GENERAL_DATA].[G_FSN]
            ,[TBL_BFT_DATA].[G_Test_DataFile_XML]
        FROM [dba_mfgt_archive_SW_2019_1_RW].[dbo].[TBL_GENERAL_DATA]
        LEFT JOIN [dba_mfgt_archive_SW_2019_1_RW].[dbo].[TBL_BFT_DATA]
        ON [TBL_GENERAL_DATA].[G_Test_RunID] = [TBL_BFT_DATA].[G_Test_RunID] 
        WHERE [TBL_GENERAL_DATA].[G_DUT_Type] = 'AR7586-UM'
        AND [TBL_GENERAL_DATA].[G_Station_SubType] = 'BFT'
        AND [TBL_GENERAL_DATA].[G_FSN] = '%s26' '''%FSN
        rows = msg.ExecQuery(sql)
        for row in rows:
            fsn = row[7]
            dt = row[0]
            #timearray = time.strptime(str(dt), "%Y-%m-%d %H:%M:%S")
            #timestamp = time.mktime(timearray)
            date_str = dt.strftime('%Y%m%d%H%M%S%f')
            stationsubtype = row[2]
            filename = fsn + "_" + str(date_str) + "_XDB_" + stationsubtype +"_TestData.xml" #UM910100070510XX_1551666223_XDB_RFV_TestData.xml
            print(filename)
            if row[8] != "":
                try:
                    msg.save_to_file("Z:\\rawdata\\AR7586-UM\\2019_March\\dbg_from_sql\\",filename,row[8])
                except:
                    print ("write to file error")

def getRFTdata(msg,FSNs):
    for FSN in FSNs:
        
        #For RFT:
        sql = '''SELECT [TBL_GENERAL_DATA].[DB_Date_Time_Inserted]
            ,[TBL_GENERAL_DATA].[G_Station_Type]
            ,[TBL_GENERAL_DATA].[G_Station_SubType]
            ,[TBL_GENERAL_DATA].[G_Test_RunID]
            ,[TBL_GENERAL_DATA].[G_Test_Result]
            ,[TBL_GENERAL_DATA].[G_Test_Duration]
            ,[TBL_GENERAL_DATA].[G_DUT_Type]
            ,[TBL_GENERAL_DATA].[G_FSN]
        	,[TBL_SFT_DATA].[G_Test_DataFile_XML]
        FROM [dba_mfgt_archive_SW_2019_1_RW].[dbo].[TBL_GENERAL_DATA]
        INNER JOIN [dba_mfgt_archive_SW_2019_1_RW].[dbo].[TBL_SFT_DATA]
        ON [TBL_GENERAL_DATA].[G_Test_RunID] = [TBL_SFT_DATA].[G_Test_RunID] 
        WHERE [TBL_GENERAL_DATA].[G_DUT_Type] = 'AR7586-UM'
        AND ([TBL_GENERAL_DATA].[G_Station_SubType] = 'RFV' OR [TBL_GENERAL_DATA].[G_Station_SubType] = 'RFS' 
        OR [TBL_GENERAL_DATA].[G_Station_SubType] = 'TTH' OR [TBL_GENERAL_DATA].[G_Station_SubType] = 'TTL' )
            AND [TBL_GENERAL_DATA].[G_FSN] = '%s26' '''%FSN
        rows = msg.ExecQuery(sql)
        for row in rows:
            fsn = row[7]
            dt = row[0]
            #timearray = time.strptime(str(dt), "%Y-%m-%d %H:%M:%S")
            #timestamp = time.mktime(timearray)
            date_str = dt.strftime('%Y%m%d%H%M%S%f')
            stationsubtype = row[2]
            filename = fsn + "_" + str(date_str) + "_XDB_" + stationsubtype +"_TestData.xml" #UM910100070510XX_1551666223_XDB_RFV_TestData.xml
            print(filename)
            if row[8] != "":
                try:
                    msg.save_to_file("Z:\\rawdata\\AR7586-UM\\2019_March\\dbg_from_sql\\",filename,row[8])
                except:
                    print ("write to file error")

def main():
    print("start")
    msg = SQLServer(server="10.21.1.1",user="mft-adm",password="MfgTest#5",database="mfgt_db_Production")
    FSNs = getfsnfromcsv("D:\\Works\\python\\jypython\\missingfsn.csv")
    getBFTdata(msg,FSNs)
    getRFTdata(msg,FSNs)
    
if __name__ == '__main__':
    main()
