import tushare as ts
import pymysql
import time
from sqlalchemy import create_engine

thistbname = 'tb_daily'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()
con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())

'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()

df = pro.stock_company()
print(df)
df.to_sql(name='tb_stockcompany', con=con, if_exists='append', index=False)
'''


try:
    cu.execute('select lastdate from tb_index where tbname = "{tbname}"'.format(tbname=thistbname))
    lastdate = cu.fetchall()[0][0]
    print('表格 ' + thistbname + ' 开始获取日期：' + lastdate)
    stockListNum = cu.execute('select ts_code from tb_stockbasic')
    stockList = cu.fetchall()
    for i in range(len(stockList)):
        ts_code = stockList[i][0]
        # print('正在获取 ' + ts_code + ' 数据')
        dailyK = pro.daily(ts_code=ts_code, start_date=lastdate, end_date=nowdate)
        for index, row in dailyK.iterrows():
            trade_date = row['trade_date']
            open = row['open']
            high = row['high']
            low = row['low']
            close = row['close']
            pre_close = row['pre_close']
            change_ = row['change']
            pct_chg = row['pct_chg']
            vol = row['vol']
            amount = row['amount']
            sqlstr = 'insert ignore into tb_daily(ts_code,trade_date,open,high,low,close,pre_close,change_,pct_chg,' \
                     'vol,amount) values("{ts_code}","{trade_date}","{open}","{high}","{low}","{close}","{pre_close}",' \
                     '"{change_}","{pct_chg}","{vol}","{amount}")'.format(ts_code=ts_code,trade_date=trade_date,
                                                                          open=open,high=high,low=low,close=close,
                                                                          pre_close=pre_close,change_=change_,
                                                                          pct_chg=pct_chg,vol=vol,amount=amount)
            # print(sqlstr)
            cu.execute(sqlstr)
            con.commit()
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'.format(nowdate=nowdate,
                                                                                         tbname=thistbname))
    con.commit()
    con.close()
    print(nowtime + ' - 更日线数据{tbname}成功！'.format(tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更日线数据{tbname}失败！'.format(tbname=thistbname))

