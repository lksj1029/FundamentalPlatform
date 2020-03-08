import tushare as ts
import pymysql
import time
import random
import datetime
from sqlalchemy import create_engine

thistbname = 'tb_stkaccount'
thistbinfo = '股票开户数'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()

con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())

'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.stk_account(start_date='20160101', end_date='20181231')
print(df)
df.to_sql(name=thistbname, con=con, if_exists='append', index=False)
'''

try:
    cu.execute('select lastdate from tb_index where tbname = "{tbname}"'.format(tbname=thistbname))
    lastdate = cu.fetchall()[0][0]
    print('表格 ' + thistbname + ' 开始获取日期：' + lastdate)
    df = pro.stk_account(start_date=lastdate, end_date=nowdate)
    for index, row in df.iterrows():
        sqlstr = 'insert ignore into {tbname} values("{date}","{weekly_new}","{total}","{weekly_hold}",' \
                 '"{weekly_trade}")'.format(tbname=thistbname, date=row['date'], weekly_new=row['weekly_new'],
                                            total=row['total'], weekly_hold=row['weekly_hold'],
                                            weekly_trade=row['weekly_trade'])
        cu.execute(sqlstr)
        con.commit()
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'
               .format(nowdate=nowdate, tbname=thistbname))
    con.commit()
    con.close()
    print(nowtime + ' - 更新{tbinfo}({tbname})成功！'.format(tbinfo=thistbinfo, tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更新{tbinfo}({tbname})失败！'.format(tbinfo=thistbinfo, tbname=thistbname))