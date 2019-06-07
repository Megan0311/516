# -*- coding: utf-8 -*-
import pandas as pd
from Django516 import KPI
import os, datetime
#from pandasql import sqldf
import talib
from talib import MA_Type
from KPI import work as KPI_work
#pysqldf = lambda q: sqldf(q, globals())

pd.set_option('display.expand_frame_repr', False) #讓pandas印出所有欄位


#DataFrame新增資料，傳入該DataFrame與要新增的資料
def add_row(df,add_list):
	df.loc[-1] = add_list #新增資料到最後一個
	df.index = df.index + 1  # 將index移位
	df = df.sort_index()  # 依index排序
	return df

#建倉，傳入股票代號、建倉日期、價格、數量、做多或放空、現金帳戶餘額、持倉資料
def hold(stock_id, date, match_time, MK_price, MK_q, bought_or_sold,cash_account,hold_stock):
	Total_value = MK_price * MK_q 	#此次建倉的總價值
	Funding_needs = Total_value * 1.001425 #資金需求
	if bought_or_sold == 1 and cash_account > Funding_needs: #若做多以及現金帳戶餘額大於資金需求
		add_value =  [stock_id, date, match_time, MK_price, MK_q, bought_or_sold] #新增這筆資料
		hold_stock = add_row(hold_stock,add_value)
		cash_account-= Funding_needs #扣除現金帳戶餘額
		
	elif bought_or_sold == 2 and cash_account > Total_value * 1.004425: #若放空
		add_value =  [stock_id, date, match_time, MK_price, MK_q, bought_or_sold]  #新增這筆資料
		hold_stock = add_row(hold_stock,add_value)
		cash_account-= Total_value + Total_value * (0.001425 + 0.003) #現金資產增加
	return cash_account,hold_stock #回傳現金帳戶餘額、持倉資料

#平倉，產生交易紀錄，傳入股票代號、平倉日期、配對時間、價格、現金帳戶餘額、持倉資料
def Offset(stock_id, date, match_time, O_price, cash_account, hold_stock, trade_record):
	process_data = hold_stock[hold_stock.stockid == stock_id].iloc[0] #抽出改股票的第一筆(假定先進先出)
	Total_value = O_price * process_data["QTY"] #此次平倉的總價值

	if process_data["LS_type"] == 1 or process_data["LS_type"] == 3: #若本來為做多
		process_data["LS_type"] = 1
		cash_account+= Total_value - Total_value*(0.003 + 0.001425) #現金資產增加
		#新增這筆資料
		add_value = [stock_id, process_data["MK_date"], process_data["MK_time"], process_data["MK_price"], date, match_time, O_price, process_data["QTY"],cash_account,process_data["LS_type"]]
		trade_record = add_row(trade_record,add_value)
				
	elif process_data["LS_type"] == 2 or process_data["LS_type"] == 4:
		process_data["LS_type"] = 2
		cash_account+= O_price *  process_data["QTY"] #取回保證金
		cash_account+= O_price *  process_data["QTY"] - Total_value * (1 + 0.001425) #取得獲利、回補股票
		#新增這筆資料
		add_value = [stock_id, date, match_time, O_price, process_data["MK_date"], process_data["MK_time"], process_data["MK_price"], process_data["QTY"],cash_account,process_data["LS_type"]]
		trade_record = add_row(trade_record,add_value)

	hold_stock = hold_stock[hold_stock.stockid != stock_id] #刪除該筆持倉資料
	return cash_account,hold_stock,trade_record #回傳現金帳戶餘額、持倉資料、交易紀錄

#策略判斷，傳入股票代號、判斷日期、下一根K線價格、配對時間、持倉紀錄、現金餘額、交易紀錄、當前趨勢、RSI欄位名稱、RSI買進點、RSI賣出點
def Trade_Condition_process(input_row, Date, match_time, find_price, hold_stock, cash_account, trade_record, current_trend):
	now_hold_stock = list(hold_stock['stockid'])
	stock_id = input_row['STOCK_SYMBOL']
	if stock_id not in now_hold_stock: #若沒有在持有清單中，則建倉
		if input_row["LS_type"] == 1 and current_trend[stock_id] != 1: #若RSI小於20且趨勢不是上漲(防止一買再買)，則建倉
			cash_account,hold_stock = hold(stock_id, Date, match_time, find_price, 1000, 1, cash_account, hold_stock) #建倉
			current_trend[stock_id] = 1 #定義目前是上漲趨勢

		elif input_row["LS_type"] == 2 and current_trend[stock_id] != 2: #若RSU大於80且趨勢不是下跌(防止一買再買)，則建倉
			cash_account,hold_stock = hold(stock_id, Date, match_time, find_price, 1000, 2, cash_account, hold_stock) #建倉
			current_trend[stock_id] = 2 #定義目前是下跌趨勢

	else: 	#否則判斷是否平倉
		if input_row["LS_type"] == 2:
			cash_account,hold_stock,trade_record = Offset(stock_id, Date, match_time, find_price, cash_account,hold_stock,trade_record)
			current_trend[stock_id] = 2 #定義目前是下跌趨勢
			
		elif input_row["LS_type"] == 1:
			cash_account,hold_stock,trade_record = Offset(stock_id, Date, match_time, find_price, cash_account,hold_stock,trade_record)
			current_trend[stock_id] = 1 #定義目前是上漲趨勢
		
		elif input_row["LS_type"] == 3: #觸發買方濾網平倉
			cash_account,hold_stock,trade_record = Offset(stock_id, Date, match_time, find_price, cash_account,hold_stock,trade_record)
			current_trend[stock_id] = 0 #定義目前是無趨勢
			
		elif input_row["LS_type"] == 4: #觸發賣方濾網平倉
			cash_account,hold_stock,trade_record = Offset(stock_id, Date, match_time, find_price, cash_account,hold_stock,trade_record)
			current_trend[stock_id] = 0 #定義目前是無趨勢
		
	return hold_stock, cash_account, trade_record, current_trend


def RSI_data_process(df,para):
	df.loc[:,"RSI"] = talib.RSI(df["close"], para["RSI_period"])
	return df

def KD_data_process(df,para):
	df["K"],df["D"] = talib.STOCH(	#輸出成K與D欄位
		df["highest"].values,
		df["lowest"].values,
		df["close"].values,
		#9,3,3
		fastk_period = para["fastk_period"],
		slowk_period = para["slowk_period"],
		slowk_matype = 0,
		slowd_period = para["slowd_period"],
		slowd_matype = 0
	)
	return df

def SMA_data_process(df,para,SMA_count):
	column_name = "SMA_" + str(SMA_count)
	df[column_name] = talib.SMA( df["close"], timeperiod = para ).shift(1)
	return df

def create_last_data(df,s_id,keep_data_mark_list,last_data_dict,keep_data_days,mark):
	#保留資料前處理，確定要留那些標記
	if len(keep_data_mark_list[s_id]) < keep_data_days:
		keep_data_mark_list[s_id].append(mark)
	else:
		keep_data_mark_list[s_id].append(mark)
		keep_data_mark_list[s_id] = keep_data_mark_list[s_id][1:]

	last_data_dict[s_id] = df[df["sheet_name"].isin(keep_data_mark_list[s_id])]	#刪除多餘的的資料，以資料標記篩選

	return keep_data_mark_list,last_data_dict

def work(stock_list, Strategy, para_dict, lag_interval):
	pd.options.mode.chained_assignment = None #防止跳不必要警告訊息

	#定義初始值
	cash_account = 5000000	#初始資金
	hold_stock = pd.DataFrame(columns = ["stockid","MK_date","MK_time","MK_price", "QTY","LS_type"]) #持倉DataFrame
	trade_record = pd.DataFrame(columns = ["stockid","bought_date","bought_time","bought_price","sold_date","sold_time","sold_price","QTY","cash_account","LS_type"]) #交易紀錄DataFrame
	lag_interval = lag_interval * 1000000	#LAG值轉換
	
	#data_list = os.listdir(r"./merged_data")
	data_list = os.listdir(r"./merged_data/merged_data_" + str(para_dict["major_data_interval"]) + "min")
	data_list.sort() #依日期做排序
	
	last_data_dict = {}	#記錄前一日交易資料
	keep_tomorrow = {}	#記錄前一天沒能交易，隔日交易之紀錄
	current_trend = {}	#記錄當下趨勢，初始值為0，1代表多頭(上漲趨勢)，2代表空頭(下跌趨勢)
	keep_data_days = para_dict["keep_day"]	#設定保留先前資料天數
	keep_data_mark_list = {}
	
	for s_id in stock_list:
		last_data_dict[s_id] = pd.DataFrame()#columns = ['STOCK_SYMBOL', 'MATCH_TIME', 'close', 'highest', 'lowest', 'Quantity', 'open', "RSI","sheet_name","K","D",'MACD','MACDsignal','MACDhist'])
		current_trend[s_id] = 0
		keep_data_mark_list[s_id] = []
		
	for i in data_list:
		Date = i.split("_")[0] #當日日期
		print(Date)
		#all_daily_data = pd.read_csv(r"./merged_data/" + i ,sep = "," , low_memory=False)	#讀取檔案
		all_daily_data = pd.read_csv(r"./merged_data/merged_data_" + str(para_dict["major_data_interval"]) + "min/" + i ,sep = "," , low_memory=False)	#讀取K線檔案
		raw_data =  pd.read_csv(r"./contest_data/" + Date + "_Stock.csv" , names = ["MATCH_TIME","STOCK_SYMBOL","ITEM","PRICE","QTY","AMOUNT"], sep = "," , low_memory=False)	#讀取原始TICK資料
        Day_buy_sell_point = pd.DataFrame(columns = ['STOCK_SYMBOL', 'MATCH_TIME', 'close', "LS_type"])	#記錄當日的買賣點
        if not bool(keep_tomorrow):
           	for key, value in keep_tomorrow.items():
           		stock_id = key #股票代號
           		rows_matchtime = all_daily_data[["STOCK_SYMBOL"] == stock_id].iloc[0]['MATCH_TIME'] #假設開盤前掛市價單
           		
           		try:	#找不到價格會出錯
           			#取得第一筆交易資料
           			find_price_time = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id) & (all_daily_data["MATCH_TIME"] > rows_matchtime)].iloc[0][["close","MATCH_TIME"]]
           			del keep_tomorrow[s_id]
           		except:	#若找不到價格，則為之後無交易或是當日沒交易，故將其再記錄隔日開盤買賣
           			keep_tomorrow[s_id] = value
           			continue 	#紀錄後則跳過此巡
           		hold_stock, cash_account, trade_record, current_trend = Trade_Condition_process(value, Date, find_price_time["MATCH_TIME"], find_price_time["close"],hold_stock,cash_account,trade_record,current_trend)
        if(stock_list == ['2408','2421','2313','3661','2330','1409','2481','2515']):

		  for s_id in stock_list:
              current_process = all_daily_data[all_daily_data["STOCK_SYMBOL"] == str(s_id)]	#找出個股的資料
			#current_process.loc[:, "RSI"] = "" 	#給合併前的資料一個空白的欄位，以便合併對齊
			  try:
				current_process.loc[:,"sheet_name"] = i	#給定資料標記，以便刪除資料時有依據
			 except:
				continue
			 merged_data = pd.concat([last_data_dict[s_id] ,current_process],axis = 0, sort=True) 	#將前一天的資料合併，否前當日前六筆都是空值

			 if Strategy == "RSI":
				merged_data = RSI_data_process(merged_data,para_dict)
		
				keep_data_mark_list, last_data_dict = create_last_data(merged_data, s_id, keep_data_mark_list, last_data_dict, keep_data_days, i)
				
				merged_data = merged_data[merged_data["sheet_name"] == i]	#僅保留今日資料
				
				#找出當日買賣點
				Buy_sell_point_1 = merged_data[merged_data["RSI"] < para_dict["RSI_BUY"]][['STOCK_SYMBOL', 'MATCH_TIME', 'close']]
				
				if not bool(Buy_sell_point_1.empty):
					Buy_sell_point_1.loc[:,"LS_type"] = 1
				
				Buy_sell_point_2 = merged_data[merged_data["RSI"] > para_dict["RSI_SELL"]][['STOCK_SYMBOL', 'MATCH_TIME', 'close']]
				
				if not bool(Buy_sell_point_2.empty):
					Buy_sell_point_2.loc[:,"LS_type"] = 2
				Day_buy_sell_point = pd.concat([Day_buy_sell_point,Buy_sell_point_1,Buy_sell_point_2], axis = 0,sort=False)
			
			 if Strategy == "RSI&KD":
				merged_data = RSI_data_process(merged_data,para_dict)	#計算RSI資料
				merged_data = KD_data_process(merged_data,para_dict)	#計算KD資料
				
				Buy_sell_point_1 = merged_data[
						(merged_data["RSI"] < para_dict["RSI_BUY"]) &
						(merged_data["K"] > para_dict["BUY_K"])
						][['STOCK_SYMBOL', 'MATCH_TIME', 'close']]
				
				if not bool(Buy_sell_point_1.empty):
					Buy_sell_point_1.loc[:,"LS_type"] = 1
				
				Buy_sell_point_2 = merged_data[
						(merged_data["RSI"] > para_dict["RSI_SELL"]) &
						(merged_data["K"] < para_dict["SELL_K"])
						
						][['STOCK_SYMBOL', 'MATCH_TIME', 'close']]
				
				if not bool(Buy_sell_point_2.empty):
					Buy_sell_point_2.loc[:,"LS_type"] = 2
				Day_buy_sell_point = pd.concat([Day_buy_sell_point,Buy_sell_point_1,Buy_sell_point_2], axis = 0,sort=False)
				
				
				
		Day_buy_sell_point = Day_buy_sell_point.sort_values("MATCH_TIME") #依據配對時間做排序
		#Day_buy_sell_point.to_excel(Date + "_Day_buy_sell_point.xlsx")
		
		#走訪資料，決定買賣點
		for index, rows in Day_buy_sell_point.iterrows():
			stock_id = rows["STOCK_SYMBOL"]
			rows_matchtime = rows["MATCH_TIME"]
			
			#以下一根K棒收盤價作為建倉價格
			try:	#找不到價格會出錯
				#find_price_time = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id) & (all_daily_data["MATCH_TIME"] > rows_matchtime)].iloc[0][["close","MATCH_TIME"]]
				find_price_time = raw_data[(raw_data["STOCK_SYMBOL"] == stock_id) & (raw_data["MATCH_TIME"] > rows_matchtime + lag_interval)].iloc[0][["PRICE","MATCH_TIME"]]
			except:	#若找不到價格，則為之後無交易或是當日沒交易，故將其再記錄隔日開盤買賣
				keep_tomorrow[s_id] = rows
				continue 	#紀錄後則跳過此巡

			hold_stock, cash_account, trade_record,current_trend = Trade_Condition_process(rows, Date, find_price_time["MATCH_TIME"] , find_price_time["PRICE"],hold_stock,cash_account,trade_record,current_trend)

		#若為最後一日交易日，則將持倉全部以當天最後一筆價格進行平倉
		if i == data_list[-1]:
			for index, rows in hold_stock.iterrows():
				stock_id = rows["stockid"]
				rows_matchtime = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id)].iloc[-1]['MATCH_TIME']
				try:
					#取得最後一筆價格
					find_price = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id) & (all_daily_data["MATCH_TIME"] == rows_matchtime)].iloc[-1]['close']
				except:
					find_price = rows["close"] #若失敗則以當初購買價格平倉
				cash_account,hold_stock,trade_record =  Offset(stock_id, Date, rows_matchtime, find_price, cash_account,hold_stock,trade_record)

	trade_record["Algorithm"] = 1 #演算法、策略編號
	trade_record["offset_date"] = trade_record[["bought_date","sold_date"]].max(axis=1) #抓出平倉日
	
	trade_record.index.names = ['trade_no'] #將index命名為trade_no
	trade_record =  trade_record.sort_values(by=["offset_date",'trade_no']) 	#依據平倉日、交易順序進行排序
	trade_record = trade_record.reset_index(drop=True) #重設index
	trade_record.index.names = ['trade_no'] #將index命名為trade_no
	trade_record = trade_record.drop(["offset_date","cash_account"], axis=1) #將多餘的紀錄刪除

	print(hold_stock) #最後應該為空DataFrame
	#print(trade_record)
	trade_record.to_csv(r"./trade_record.csv", sep = ",") #輸出成CSV檔
	print(KPI_work())
	return trade_record

if __name__ == '__main__':
	stock_list = ["1101","1102","1216","1301","1303","1326","1402","2002","2105","2207","2227","2301","2303","2308","2317","2327","2330","2352","2357","2382","2395","2408","2409","2412","2454","2474","2609","2610","2633","2801","2823","2880","2881","2882","2883","2884","2885","2886","2887","2890","2891","2892","2912","3008","3045","3481","3711","4904","4938","5871","5880","6505","9904"]
	if stock_list == []:
		stock_data = pd.read_csv(r"./merged_data/merged_data_30min/20180702_merged_data.csv" ,sep = "," , low_memory=False)	#讀取K線檔案
		stock_list = stock_data["STOCK_SYMBOL"].unique()
		
	Strategy = "RSI&KD"
	
	if Strategy == "RSI":
		#RSI間隔、買進條件、賣出條件參數
		para_dict = {
			"major_data_interval" : 5,
			"keep_day" : 10,
			"RSI_period" : 6,
			"RSI_BUY" : 20,
			"RSI_SELL" : 80
			}

	if Strategy == "RSI&KD":
		#RSI間隔、買進條件、賣出條件參數
		para_dict = {
			"major_data_interval" : 5,
			"keep_day" : 10,	#保留前N天資料天數，使用比較長天期或低頻率的就要調越高
			
			#計算參數值 - RSI
			"RSI_period" : 6,
			"RSI_BUY" : 20,
			"RSI_SELL" : 80,
			#計算參數值 - KD
			"fastk_period" : 9,
			"slowk_period" : 3,
			"slowd_period" : 3,
			
			#比較值
			"BUY_K" : 40,
			"SELL_K" : 60
			}

	print(str(datetime.datetime.now()) + "開始抓取")
	work(stock_list,Strategy,para_dict,5)
	print(str(datetime.datetime.now()) + "結束抓取")
	print(Strategy,"interval",para_dict["major_data_interval"])