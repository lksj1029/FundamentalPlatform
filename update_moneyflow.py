import tushare as ts
import pymysql
import time
from sqlalchemy import create_engine

thistbname = 'tb_moneyflow'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()

con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())
'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.moneyflow(trade_date='20190315')
print(df)
df.to_sql(name=thistbname, con=con, if_exists='append', index=False)
'''
try:
    cu.execute('select lastdate from tb_index where tbname = "{tbname}"'.format(tbname=thistbname))
    lastdate = cu.fetchall()[0][0]
    print('表格 ' + thistbname + ' 开始获取日期：' + lastdate)
    stockListNum = cu.execute('select ts_code from tb_stockbasic')
    stockList = cu.fetchall()
    num = 0
    for i in range(len(stockList)):
        ts_code = stockList[i][0]
        print('正在获取 ' + ts_code + ' 数据')
        moneyflow = pro.moneyflow(ts_code=ts_code, start_date=lastdate, end_date=nowdate)
        for index, row in moneyflow.iterrows():
            trade_date = row['trade_date']
            sqlstr = 'insert ignore into tb_moneyflow(ts_code,trade_date,buy_sm_vol,buy_sm_amount,' \
                     'sell_sm_vol,sell_sm_amount,buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,' \
                     'buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,buy_elg_vol,buy_elg_amount,' \
                     'sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount) values("{ts_code}","{trade_date}",' \
                     '"{buy_sm_vol}","{buy_sm_amount}","{sell_sm_vol}","{sell_sm_amount}","{buy_md_vol}",' \
                     '"{buy_md_amount}","{sell_md_vol}","{sell_md_amount}","{buy_lg_vol}","{buy_lg_amount}",' \
                     '"{sell_lg_vol}","{sell_lg_amount}","{buy_elg_vol}","{buy_elg_amount}","{sell_elg_vol}",' \
                     '"{sell_elg_amount}","{net_mf_vol}","{net_mf_amount}")'.format(
                ts_code=ts_code, trade_date=trade_date,buy_sm_vol=row['buy_sm_vol'],
                buy_sm_amount=row['buy_sm_amount'],sell_sm_vol=row['sell_sm_vol'],
                sell_sm_amount=row['sell_sm_amount'],buy_md_vol=row['buy_md_vol'],
                buy_md_amount=row['buy_md_amount'],sell_md_vol=row['sell_md_amount'],
                sell_md_amount=row['sell_md_amount'],buy_lg_vol=row['buy_lg_vol'],
                buy_lg_amount=row['buy_lg_amount'],sell_lg_vol=row['sell_lg_vol'],
                sell_lg_amount=row['sell_lg_amount'],buy_elg_vol=row['buy_elg_vol'],
                buy_elg_amount=row['buy_elg_amount'],sell_elg_vol=row['sell_elg_vol'],
                sell_elg_amount=row['sell_elg_amount'],net_mf_vol=row['net_mf_vol'],
                net_mf_amount=row['net_mf_amount'])
            # print(sqlstr)
            cu.execute(sqlstr)
            con.commit()
        time.sleep(1)
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'.format(nowdate=nowdate,
                                                                                         tbname=thistbname))
    con.commit()
    con.close()

    print(nowtime + ' - 更新个股资金流向{tbname}成功！'.format(tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更新个股资金流向{tbname}失败！'.format(tbname=thistbname))
