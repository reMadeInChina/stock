# -*- coding: UTF-8 -*- 
import tushare as ts
import pandas as pd
from pandas import DataFrame, Series
from datetime import datetime, timedelta
import pymysql

from bs4 import BeautifulSoup
from urllib.request import Request,urlopen
import re
import codecs

from os import path,makedirs

adr = 'E:/Mega/python/notebook/ST/'
fenhong_adr = adr+'fuquan/fenhong/'
peigu_adr = adr+'fuquan/peigu/'
k_day_adr = adr+datetime.now().strftime('%Y%m%d')+'/'
if not path.exists(k_day_adr):
    makedirs(K_day_adr)

today = datetime.now().strftime('%Y-%m-%d')
input_date = input('请输入开始更新日期，格式为：2017-07-01')
start_date_input = today if input_date == '' else input_date

def datekey2date(x):
    from datetime import datetime 
    if x>10^7:
        y = x
    else:
        y = 99991231
    return datetime.strptime(str(y), '%Y%m%d').date()

#连接mariadb数据库:port=3307
conn = pymysql.connect(host='localhost',user='root',passwd='125521',port=3307,db='stock',local_infile=True,charset='utf8')
cur = conn.cursor()

#股票列表更新
basics = ts.get_stock_basics()
basics['date_to_market'] = basics['timeToMarket'].apply(datekey2date)
basics.to_csv(adr+'basics.txt',index=True, encoding='utf-8')



#复权列表更新
def get_table(url,tableid): #获取表格
    req = urlopen(url).read()
    soup = BeautifulSoup(req,"lxml")
    table = soup.find("table", {"id" : tableid})
    tr=table.find_all('tr')[1:]
    return tr

def get_fuquan(code): #分红函数
    url = 'http://money.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/'+code+'.phtml'
    table1 = get_table(url,'sharebonus_1')
    table2 = get_table(url,'sharebonus_2')
    new = 0
    if str(table1).find("暂时没有数据")<0:
        f= codecs.open(fenhong_adr+code+'.txt', 'w', "utf-8")  # 打开输出文件
        for i in range(2,len(table1)): #逐行读取table
            row = table1[i].get_text().strip().split('\n')
            if row[4] == '实施' and row[3] <= today and row[3] >= start_date_input: #只保留当天实施的数据
                new = 1
            #保存本地
            try:
                f.write(table1[i].get_text().strip().replace('\n',','))
                f.write('\n')
            except:
                print(code,table1[i])
                continue
        f.close()
        print(code,'fenhong')
    if str(table2).find("暂时没有数据")<0:
        f= codecs.open(peigu_adr+code+'.txt', 'w', "utf-8")  # 打开输出文件
        for i in range(len(table2)):
            try:
                f.write(table2[i].get_text().strip().replace('\n',','))
                f.write('\n')
            except:
                print(code,table2[i])
                continue
        f.close()
        print(code,'peigu')
    return new   
   
for code in basics[(basics['timeToMarket']>0)].index:
    fuquan = get_fenhong(code)
    date_to_market = datetime.strptime(str(basics.ix[code]['timeToMarket']), '%Y%m%d').strftime('%Y-%m-%d') 
    start_date = date_to_market if fuquan > 0 else start_date_input
    end_date = today
    try:
        get_his = ts.get_k_data(code=code, start=start_date, end=end_date,
                               autype='qfq', retry_count=100,  pause=1)
        get_his.to_csv(k_day_adr+code+'.txt',encoding='utf-8',index=0)
        print(code,'is done')
    except:
        print(code,'got error')
        continue