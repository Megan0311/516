# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
from pandasql import sqldf
import os
import datetime
pysqldf = lambda q: sqldf(q, globals())


data_list = os.listdir(r"./contest_data")
# =============================================================================
# start_index = data_list.index("20181025_Stock.csv")
# data_list = data_list[start_index+1:]
# =============================================================================

interval = 30 * 100000000 #頻率為5分鐘
interval = str(interval)

def process_time(t,interval):
	interval = int(interval)
	
	#若有秒數，則調整至下個間隔
	if t % interval != 0:
		result = t // interval * interval + interval
	else:
		result = t
	
	#換回60進位
	result+= 90000000000 + result // 6000000000 * 4000000000
	
	#進位
	if result % 10000000000 >= 6000000000:
		result += 4000000000
	
	#超過收盤處理
	if result > 133000000000:
		result = 133000000000
	return result

#走訪日期
for Data in data_list:
	print(Data)
	#清除雜質資料
	all_daily_data = pd.read_csv(r"./contest_data/" + Data ,names = ["MATCH_TIME","STOCK_SYMBOL","ITEM","PRICE","QTY","AMOUNT"],sep = ",", low_memory=False)
	
	#將資料的小時轉承10進位的分鐘
	all_daily_data = all_daily_data[(all_daily_data["MATCH_TIME"] >= 90000000000) &(all_daily_data["MATCH_TIME"] <= 133000000000)]
	all_daily_data["MATCH_TIME"] = all_daily_data["MATCH_TIME"] - 90000000000
	all_daily_data["MATCH_TIME"] = all_daily_data["MATCH_TIME"] - all_daily_data["MATCH_TIME"] // 10000000000 * 4000000000
	
	query = "select STOCK_SYMBOL,MATCH_TIME,PRICE as open,round(MATCH_TIME / "+interval+" -0.5) as merge from all_daily_data group by STOCK_SYMBOL,round(MATCH_TIME / "+interval+" -0.5) having min(MATCH_TIME)"
	first_price = pysqldf(query)

	query = "select STOCK_SYMBOL,MATCH_TIME,PRICE as close,round(MATCH_TIME / "+interval+" -0.5) as merge from all_daily_data group by STOCK_SYMBOL,round(MATCH_TIME / "+interval+" -0.5) having max(MATCH_TIME)"
	last_price = pysqldf(query)

	query = "select STOCK_SYMBOL, MATCH_TIME, max(PRICE) as highest, min(PRICE)  as lowest, round(MATCH_TIME / "+interval+" -0.5) as merge, sum(QTY) as Quantity from all_daily_data group by STOCK_SYMBOL,round(MATCH_TIME / "+interval+" -0.5)"
	other_data = pysqldf(query)
	
	query = "select A.*,B.highest,B.lowest,B.Quantity from last_price as A inner join other_data as B on A.STOCK_SYMBOL = B.STOCK_SYMBOL and A.merge = B.merge"
	merge1_df = pysqldf(query)
	
	query = "select A.*,B.open from merge1_df as A inner join first_price as B on A.STOCK_SYMBOL = B.STOCK_SYMBOL and A.merge = B.merge"
	merge2_df = pysqldf(query)
	
	#將資料轉換回來小時與分鐘60進位
	merge2_df["MATCH_TIME"] = merge2_df["MATCH_TIME"].apply(process_time,args=(interval,))
	#merge2_df["MATCH_TIME"] = merge2_df["MATCH_TIME"] + 90000000000 + merge2_df["MATCH_TIME"] // 6000000000 * 4000000000

	merge2_df = merge2_df.drop(['merge'], axis=1)
	
	merge2_df.to_csv(r"./merged_data/merged_data_"+ str(int(int(interval) / 100000000)) + r"min/" + Data.split("_")[0] + "_merged_data.csv",sep = ",",index=False)
