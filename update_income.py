import tushare as ts
import pymysql
import time
import datetime
from sqlalchemy import create_engine

thistbname = 'tb_income'
thistbinfo = '利润表'
ts.set_token('7eb4bc05a48bb2704d76c1b79c501053b58ad1b190b505faa9009d5c')
pro = ts.pro_api()

con = pymysql.connect(user='root', password='lksjlksj', database='fundamentalplatform', charset='utf8')
cu = con.cursor()
nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
nowdate = time.strftime("%Y%m%d", time.localtime())

'''
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', 'lksjlksj', 'localhost', 'fundamentalplatform'))
con = engine.connect()
df = pro.income(ts_code='600000.SH', start_date='20100101', end_date='20200730')
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
        df = pro.income(ts_code=ts_code, start_date=lastdate, end_date=nowdate)
        for index, row in df.iterrows():
            sqlstr = 'insert ignore into {tbname} values("{ts_code}","{ann_date}","{f_ann_date}","{end_date}",' \
                     '"{report_type}","{comp_type}","{basic_eps}","{diluted_eps}","{total_revenue}","{revenue}",' \
                     '"{int_income}","{prem_earned}","{comm_income}","{n_commis_income}","{n_oth_income}",' \
                     '"{n_oth_b_income}","{prem_income}","{out_prem}","{une_prem_reser}","{reins_income}",' \
                     '"{n_sec_tb_income}","{n_sec_uw_income}","{n_asset_mg_income}","{oth_b_income}",' \
                     '"{fv_value_chg_gain}","{invest_income}","{ass_invest_income}","{forex_gain}",' \
                     '"{total_cogs}","{oper_cost}","{int_exp}","{comm_exp}","{biz_tax_surchg}","{sell_exp}",' \
                     '"{admin_exp}","{fin_exp}","{assets_impair_loss}","{prem_refund}","{compens_payout}",' \
                     '"{reser_insur_liab}","{div_payt}","{reins_exp}","{oper_exp}","{compens_payout_refu}",' \
                     '"{insur_reser_refu}","{reins_cost_refund}","{other_bus_cost}","{operate_profit}",' \
                     '"{non_oper_income}","{non_oper_exp}","{nca_disploss}","{total_profit}","{income_tax}",' \
                     '"{n_income}","{n_income_attr_p}","{minority_gain}","{oth_compr_income}","{t_compr_income}",' \
                     '"{compr_inc_attr_p}","{compr_inc_attr_m_s}","{ebit}","{ebitda}","{insurance_exp}",' \
                     '"{undist_profit}","{distable_profit}")' \
                     ''.format(tbname=thistbname,ts_code=row['ts_code'],ann_date=row['ann_date'],
                               f_ann_date=row['f_ann_date'],end_date=row['f_ann_date'],report_type=row['report_type'],
                               comp_type=row['comp_type'],basic_eps=row['basic_eps'],diluted_eps=row['diluted_eps'],
                               total_revenue=row['total_revenue'],revenue=row['revenue'],int_income=row['int_income'],
                               prem_earned=row['prem_earned'],comm_income=row['comm_income'],
                               n_commis_income=row['n_commis_income'],n_oth_income=row['n_oth_income'],
                               n_oth_b_income=row['n_oth_b_income'],prem_income=row['prem_income'],
                               out_prem=row['out_prem'],une_prem_reser=row['une_prem_reser'],
                               reins_income=row['reins_income'],n_sec_tb_income=row['n_sec_tb_income'],
                               n_sec_uw_income=row['n_sec_uw_income'],n_asset_mg_income=row['n_asset_mg_income'],
                               oth_b_income=row['oth_b_income'],fv_value_chg_gain=row['fv_value_chg_gain'],
                               invest_income=row['invest_income'],ass_invest_income=row['ass_invest_income'],
                               forex_gain=row['forex_gain'],total_cogs=row['total_cogs'],oper_cost=row['oper_cost'],
                               int_exp=row['int_exp'],comm_exp=row['comm_exp'],biz_tax_surchg=row['biz_tax_surchg'],
                               sell_exp=row['sell_exp'],admin_exp=row['admin_exp'],fin_exp=row['fin_exp'],
                               assets_impair_loss=row['assets_impair_loss'],prem_refund=row['prem_refund'],
                               compens_payout=row['compens_payout'],reser_insur_liab=row['reser_insur_liab'],
                               div_payt=row['div_payt'],reins_exp=row['reins_exp'],oper_exp=row['oper_exp'],
                               compens_payout_refu=row['compens_payout_refu'],insur_reser_refu=row['insur_reser_refu'],
                               reins_cost_refund=row['reins_cost_refund'],other_bus_cost=row['other_bus_cost'],
                               operate_profit=row['operate_profit'], non_oper_income=row['non_oper_income'],
                               non_oper_exp=row['non_oper_exp'],nca_disploss=row['nca_disploss'],
                               total_profit=row['total_profit'],income_tax=row['income_tax'],
                               n_income=row['n_income'],n_income_attr_p=row['n_income_attr_p'],
                               minority_gain=row['minority_gain'],oth_compr_income=row['oth_compr_income'],
                               t_compr_income=row['t_compr_income'],compr_inc_attr_p=row['compr_inc_attr_p'],
                               compr_inc_attr_m_s=row['compr_inc_attr_m_s'],ebit=row['ebit'],
                               ebitda=row['ebitda'],insurance_exp=row['insurance_exp'],
                               undist_profit=row['undist_profit'],distable_profit=row['distable_profit'])

            # print(sqlstr)
            cu.execute(sqlstr)
            con.commit()
        lastdate = (datetime.datetime.strptime(lastdate, '%Y%m%d').date() +
                    datetime.timedelta(days=1)).strftime("%Y%m%d")
        time.sleep(20)
    cu.execute('update tb_index set lastdate="{nowdate}" where tbname="{tbname}"'
               .format(nowdate=nowdate, tbname=thistbname))
    con.commit()
    con.close()
    print(nowtime + ' - 更新{tbinfo}({tbname})成功！'.format(tbinfo=thistbinfo, tbname=thistbname))
except Exception as e:
    print(str(e))
    print(nowtime + ' - 更新{tbinfo}({tbname})失败！'.format(tbinfo=thistbinfo, tbname=thistbname))

