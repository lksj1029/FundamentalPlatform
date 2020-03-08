import tushare as ts
import pymysql
import time
from sqlalchemy import create_engine

ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()
con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.stock_company()
print(df)
df.to_sql(name='tb_stockcompany', con=con, if_exists='append', index=False)
'''

try:
    cu.execute('delete from tb_stockcompany')
    for exchange in ['SSE', 'SZSE']:
        data = pro.stock_company(exchange=exchange)
        for index, row in data.iterrows():
            ts_code = str(row['ts_code'])
            exchange = row['exchange']
            chairman = row['chairman']
            manager = row['manager']
            secretary = row['secretary']
            reg_capital = row['reg_capital']
            setup_date = row['setup_date']
            province = row['province']
            city = row['city']
            website = row['website']
            email = row['email']
            employees = row['employees']
            sqlstr = 'insert into tb_stockcompany(ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,' \
                     'province,city,website,email,employees) values("{ts_code}","{exchange}","{chairman}","{manager}",' \
                     '"{secretary}","{reg_capital}","{setup_date}","{province}","{city}","{website}","{email}",' \
                     '"{employees}")'.format(ts_code=ts_code, exchange=exchange,chairman=chairman,manager=manager,
                                             secretary=secretary,reg_capital=reg_capital,setup_date=setup_date,
                                             province=province,city=city,website=website,email=email,employees=employees)
            # print(sqlstr)
            cu.execute(sqlstr)
            con.commit()
    con.close()
    print(nowtime + ' - 更新上市公司基本信息成功！')
except:
    print(nowtime + ' - 更新上市公司基本信息失败！')
