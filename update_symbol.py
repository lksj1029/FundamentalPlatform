import tushare as ts
import pymysql
import time
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
try:
    cu.execute('delete from tb_stockbasic')
    for index, row in data.iterrows():
        ts_code = str(row['ts_code'])
        symbol = row['symbol']
        name = row['name']
        if not row['area']:
            area = ' '
        else:
            area = row['area']
        if not row['industry']:
            industry = ' '
        else:
            industry = row['industry']
        list_date = row['list_date']
        sqlstr = 'insert into tb_stockbasic(ts_code,symbol,name,area,industry,list_date) ' \
                 'values("{ts_code}","{symbol}","{name}","{area}","{industry}","{list_date}")' \
            .format(ts_code=ts_code, symbol=symbol, name=name, area=area, industry=industry, list_date=list_date)
        cu.execute(sqlstr)
    con.commit()
    con.close()
    print(nowtime + ' - 更新股票列表成功！')
except:
    print(nowtime + ' - 更新股票列表失败！')
    