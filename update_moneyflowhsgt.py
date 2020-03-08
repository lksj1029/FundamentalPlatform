import tushare as ts
import pymysql
import time
import datetime
from sqlalchemy import create_engine

thistbname = 'tb_moneyflowhsgt'
thistbinfo = '沪深港通资金流向'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()

con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())
'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.moneyflow_hsgt(trade_date='20190315')
print(df)
df.to_sql(name=thistbname, con=con, if_exists='append', index=False)
'''

try:
    cu.execute('select lastdate from tb_index where tbname = "{tbname}"'.format(tbname=thistbname))
    lastdate = cu.fetchall()[0][0]
    print('表格 ' + thistbname + ' 开始获取日期：' + lastdate)
    while lastdate <= nowdate:
        df = pro.moneyflow_hsgt(trade_date=lastdate)
        for index, row in df.iterrows():
            sqlstr = 'insert ignore into tb_moneyflowhsgt(trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money) ' \
                     'values("{trade_date}","{ggt_ss}","{ggt_sz}","{hgt}","{sgt}","{north_money}","{south_money}")'\
                .format(trade_date=lastdate, ggt_ss=row['ggt_ss'], ggt_sz=row['ggt_sz'], hgt=row['hgt'], sgt=row['sgt'],
                        north_money=row['north_money'], south_money=row['south_money'])
            print(sqlstr)
            cu.execute(sqlstr)
            con.commit()
        lastdate = (datetime.datetime.strptime(lastdate, '%Y%m%d').date() +
                    datetime.timedelta(days=1)).strftime("%Y%m%d")
        time.sleep(1)
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'
               .format(nowdate=nowdate,tbname=thistbname))
    con.commit()
    con.close()
    print(nowtime + ' - 更新{tbinfo}({tbname})成功！'.format(tbinfo=thistbinfo, tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更新{tbinfo}({tbname})失败！'.format(tbinfo=thistbinfo, tbname=thistbname))
