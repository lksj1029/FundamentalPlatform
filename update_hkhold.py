import tushare as ts
import pymysql
import time
import datetime
from sqlalchemy import create_engine

thistbname = 'tb_hkhold'
thistbinfo = '沪深港股通持股明细'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()

con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())
'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.hk_hold(trade_date='20190625')
print(df)
df.to_sql(name=thistbname, con=con, if_exists='append', index=False)
'''

try:
    cu.execute('select lastdate from tb_index where tbname = "{tbname}"'.format(tbname=thistbname))
    lastdate = cu.fetchall()[0][0]
    print('表格 ' + thistbname + ' 开始获取日期：' + lastdate)
    while lastdate <= nowdate:
        df = pro.hk_hold(trade_date=lastdate)
        for index, row in df.iterrows():
            sqlstr = 'insert ignore into tb_hkhold(code,trade_date,ts_code,name,vol,ratio,exchange) ' \
                     'values("{code}","{trade_date}","{ts_code}","{name}","{vol}","{ratio}","{exchange}")'\
                .format(code=row['code'], trade_date=row['trade_date'], ts_code=row['ts_code'], name=row['name'],
                        vol=row['vol'],ratio=row['ratio'], exchange=row['exchange'])
            print(sqlstr)
            cu.execute(sqlstr)
            con.commit()
        lastdate = (datetime.datetime.strptime(lastdate, '%Y%m%d').date() +
                    datetime.timedelta(days=1)).strftime("%Y%m%d")
        time.sleep(60)
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'
               .format(nowdate=nowdate, tbname=thistbname))
    con.commit()
    con.close()
    print(nowtime + ' - 更新{tbinfo}({tbname})成功！'.format(tbinfo=thistbinfo, tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更新{tbinfo}({tbname})失败！'.format(tbinfo=thistbinfo, tbname=thistbname))

