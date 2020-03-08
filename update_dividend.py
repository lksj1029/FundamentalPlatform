import tushare as ts
import pymysql
import time
import datetime
from sqlalchemy import create_engine

thistbname = 'tb_dividend'
thistbinfo = '分红送股数据'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()

con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())
'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.dividend(ts_code='600848.SH')
print(df)
df.to_sql(name=thistbname, con=con, if_exists='append', index=False)
'''

try:
    cu.execute('select lastdate from tb_index where tbname = "{tbname}"'.format(tbname=thistbname))
    lastdate = cu.fetchall()[0][0]
    print('表格 ' + thistbname + ' 开始获取日期：' + lastdate)
    stockListNum = cu.execute('select ts_code from tb_stockbasic')
    stockList = cu.fetchall()
    for i in range(len(stockList)):
        ts_code = stockList[i][0]
        df = pro.dividend(ts_code=ts_code)
        for index, row in df.iterrows():
            sqlstr = 'insert ignore into {tbname} values("{ts_code}","{end_date}","{ann_date}","{div_proc}",' \
                     '"{stk_div}","{stk_bo_rate}","{stk_co_rate}","{cash_div}","{cash_div_tax}","{record_date}",' \
                     '"{ex_date}","{pay_date}","{div_listdate}","{imp_ann_date}")' \
                     ''.format(tbname=thistbname,ts_code=row['ts_code'], end_date=row['end_date'],
                               ann_date=row['ann_date'], div_proc=row['div_proc'], stk_div=row['stk_div'],
                               stk_bo_rate=row['stk_bo_rate'], stk_co_rate=row['stk_co_rate'], cash_div=row['cash_div'],
                               cash_div_tax=row['cash_div_tax'],record_date=row['record_date'],ex_date=row['ex_date'],
                               pay_date=row['pay_date'],div_listdate=row['div_listdate'],imp_ann_date=row['imp_ann_date'])

            # print(sqlstr)
            # time.sleep(10000)
            cu.execute(sqlstr)
            con.commit()
        time.sleep(20)
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'
               .format(nowdate=nowdate, tbname=thistbname))
    con.commit()
    con.close()
    print(nowtime + ' - 更新{tbinfo}({tbname})成功！'.format(tbinfo=thistbinfo, tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更新{tbinfo}({tbname})失败！'.format(tbinfo=thistbinfo, tbname=thistbname))
