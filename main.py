# -*- coding: utf-8 -*-
import io
import requests
import datetime
import pandas as pd
import os
import sys
#from pandasql import sqldf
import talib
#pysqldf = lambda q: sqldf(q, globals())

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

	if process_data["LS_type"] == 1: #若本來為做多
		
		cash_account+= Total_value - Total_value*(0.003 + 0.001425) #現金資產增加
		#新增這筆資料
		add_value = [stock_id, process_data["MK_date"], process_data["MK_time"], process_data["MK_price"], date, match_time, O_price, process_data["QTY"],cash_account,process_data["LS_type"]]
		trade_record = add_row(trade_record,add_value)
				
	elif process_data["LS_type"] == 2:
		cash_account+= O_price *  process_data["QTY"] #取回保證金
		cash_account+= O_price *  process_data["QTY"] - Total_value * (1 + 0.001425) #取得獲利、回補股票
		#新增這筆資料
		add_value = [stock_id, date, match_time, O_price, process_data["MK_date"], process_data["MK_time"], process_data["MK_price"], process_data["QTY"],cash_account,process_data["LS_type"]]
		trade_record = add_row(trade_record,add_value)

	hold_stock = hold_stock[hold_stock.stockid != stock_id] #刪除該筆持倉資料
	return cash_account,hold_stock,trade_record #回傳現金帳戶餘額、持倉資料、交易紀錄

#策略判斷，傳入股票代號、判斷日期、下一根K線價格、配對時間、持倉紀錄、現金餘額、交易紀錄、當前趨勢、RSI欄位名稱、RSI買進點、RSI賣出點
def RSI_method(input_row, Date, match_time, find_price, hold_stock, cash_account, trade_record, current_trend, RSI_column_name, RSI_BUY, RSI_SELL):
	now_hold_stock = list(hold_stock['stockid'])
	stock_id = input_row['STOCK_SYMBOL']
	if stock_id not in now_hold_stock: #若沒有在持有清單中，則建倉
		if input_row[RSI_column_name] < RSI_BUY and current_trend[stock_id] != 1: #若RSI小於20且趨勢不是上漲(防止一買再買)，則建倉
			cash_account,hold_stock = hold(stock_id, Date, match_time, find_price, 1000, 1, cash_account, hold_stock) #建倉
			current_trend[stock_id] = 1 #定義目前是上漲趨勢

		elif input_row[RSI_column_name] > RSI_SELL and current_trend[stock_id] != 2: #若RSU大於80且趨勢不是下跌(防止一買再買)，則建倉
			cash_account,hold_stock = hold(stock_id, Date, match_time, find_price, 1000, 2, cash_account, hold_stock) #建倉
			current_trend[stock_id] = 2 #定義目前是下跌趨勢
		
	else: 	#否則判斷是否平倉
		if input_row[RSI_column_name] > RSI_SELL:
			cash_account,hold_stock,trade_record = Offset(stock_id, Date, match_time, find_price, cash_account,hold_stock,trade_record)
			current_trend[stock_id] = 2 #定義目前是下跌趨勢
			
		elif input_row[RSI_column_name] < RSI_BUY:
			cash_account,hold_stock,trade_record = Offset(stock_id, Date, match_time, find_price, cash_account,hold_stock,trade_record)
			current_trend[stock_id] = 1 #定義目前是上漲趨勢
	return hold_stock, cash_account, trade_record, current_trend

def work(stock_list,RSI_period,RSI_BUY,RSI_SELL):
    pd.options.mode.chained_assignment = None #防止跳不必要警告訊息
    RSI_period = int(RSI_period)
    RSI_BUY = int(RSI_BUY)
    RSI_SELL = int(RSI_SELL)
    #定義初始值
    cash_account = 5000000	#初始資金
    hold_stock = pd.DataFrame(columns = ["stockid","MK_date","MK_time","MK_price", "QTY","LS_type"]) #持倉DataFrame
    trade_record = pd.DataFrame(columns = ["stockid","bought_date","bought_time","bought_price","sold_date","sold_time","sold_price","QTY","cash_account","LS_type"]) #交易紀錄DataFrame
    #data_list = os.listdir(r"./merged_data")
    data_list = os.listdir(r"./app/merged_data/merged_data_5min")
    data_list = data_list[:20]
    data_list.sort() #依日期做排序
    last_data_dict = {}	#記錄前一日交易資料
    keep_tomorrow = {}	#記錄前一天沒能交易，隔日交易之紀錄
    current_trend = {}	#記錄當下趨勢，初始值為0，1代表多頭(上漲趨勢)，2代表空頭(下跌趨勢)
    RSI_column_name = 'RSI'+ str(RSI_period)
    for s_id in stock_list:
    	last_data_dict[s_id] = pd.DataFrame(columns = ['STOCK_SYMBOL', 'MATCH_TIME', 'close', 'highest', 'lowest', 'Quantity', 'open', RSI_column_name,"sheet_name"])
    	current_trend[s_id] = 0
    for i in data_list:
        print(i)
        Date = i.split("_")[0] #當日日期
        #all_daily_data = pd.read_csv(r"./merged_data/" + i ,sep = "," , low_memory=False)	#讀取檔案
        all_daily_data = pd.read_csv(r"./app/merged_data/merged_data_5min/" + i ,sep = "," , low_memory=False)	#讀取檔案
        Day_buy_sell_point = pd.DataFrame(columns = ['STOCK_SYMBOL', 'MATCH_TIME', 'close', RSI_column_name])	#記錄當日的買賣點
        if not bool(keep_tomorrow):
            for key, value in keep_tomorrow.items():
                stock_id = key #股票代號
                rows_matchtime = all_daily_data[["STOCK_SYMBOL"] == stock_id].iloc[0]['MATCH_TIME'] #假設開盤前掛市價單
                try:	
                    find_price_time = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id) & (all_daily_data["MATCH_TIME"] > rows_matchtime)].iloc[0][["close","MATCH_TIME"]]
                except:	#若找不到價格，則為之後無交易或是當日沒交易，故將其再記錄隔日開盤買賣
                    keep_tomorrow[s_id] = value
                    continue 	#紀錄後則跳過此巡
                hold_stock, cash_account, trade_record, current_trend = RSI_method(value, Date, find_price_time["MATCH_TIME"], find_price_time["close"],hold_stock,cash_account,trade_record,current_trend,RSI_column_name, RSI_BUY, RSI_SELL)
        if(stock_list == ['2408','2421','2313','3661','2330','1409','2481','2515']):
           #stock_list = ['2408','2421','2313','3661','2330','1409','2481','2515']
           for s_id in stock_list:
               try:
                current_process = all_daily_data[all_daily_data["STOCK_SYMBOL"] == str(s_id)]	#找出個股的資料
                current_process.loc[:, RSI_column_name] = "" 	#給合併前的資料一個空白的欄位，以便合併對齊
                current_process.loc[:,"sheet_name"] = i	#給定資料標記，以便刪除資料時有依據
                merged_data = pd.concat([last_data_dict[s_id] ,current_process],axis = 0) 	#將前一天的資料合併，否前當日前六筆都是空值
                merged_data.loc[:,RSI_column_name] = talib.RSI(merged_data["close"], RSI_period) 	#計算RSI
                merged_data = merged_data[merged_data["sheet_name"] == i]	#刪除前一天的資料，以資撩標記等於今日的為準篩選
                last_data_dict[s_id] = merged_data 	#將今日資料儲存，當作明日的昨日資料
                #找出當日買賣點
                Buy_sell_point = merged_data[(merged_data[RSI_column_name] < RSI_BUY) | (merged_data[RSI_column_name] > RSI_SELL)][['STOCK_SYMBOL', 'MATCH_TIME', 'close', RSI_column_name]]
                Day_buy_sell_point = pd.concat([Day_buy_sell_point,Buy_sell_point], axis = 0)
               except ValueError:
                 print("Sorry~The Datas seem to have no condition to fit this model")
                 break
        else:
           try:
             s_id=stock_list
             current_process = all_daily_data[all_daily_data["STOCK_SYMBOL"] == str(s_id)]	#找出個股的資料
             current_process.loc[:, RSI_column_name] = "" 	#給合併前的資料一個空白的欄位，以便合併對齊
             current_process.loc[:,"sheet_name"] = i	#給定資料標記，以便刪除資料時有依據
             merged_data = pd.concat([last_data_dict[s_id] ,current_process],axis = 0) 	#將前一天的資料合併，否前當日前六筆都是空值
             merged_data.loc[:,RSI_column_name] = talib.RSI(merged_data["close"], RSI_period) 	#計算RSI
             merged_data = merged_data[merged_data["sheet_name"] == i]	#刪除前一天的資料，以資撩標記等於今日的為準篩選
             last_data_dict[s_id] = merged_data 	#將今日資料儲存，當作明日的昨日資料
             #找出當日買賣點
             Buy_sell_point = merged_data[(merged_data[RSI_column_name] < RSI_BUY) | (merged_data[RSI_column_name] > RSI_SELL)][['STOCK_SYMBOL', 'MATCH_TIME', 'close', RSI_column_name]]
             Day_buy_sell_point = pd.concat([Day_buy_sell_point,Buy_sell_point], axis = 0)
           except ValueError:
              print("Sorry~The Datas seem to have no condition to fit this model")
              break
        Day_buy_sell_point = Day_buy_sell_point.sort_values("MATCH_TIME") #依據配對時間做排序
    		#Day_buy_sell_point.to_excel(Date + "_Day_buy_sell_point.xlsx")
       		#走訪資料，決定買賣點
        for index, rows in Day_buy_sell_point.iterrows():
        	stock_id = rows["STOCK_SYMBOL"]
        	rows_matchtime = rows["MATCH_TIME"]
        	#以下一根K棒收盤價作為建倉價格
        	try:	#找不到價格會出錯
        		find_price_time = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id) & (all_daily_data["MATCH_TIME"] > rows_matchtime)].iloc[0][["close","MATCH_TIME"]]
        	except:	#若找不到價格，則為之後無交易或是當日沒交易，故將其再記錄隔日開盤買賣
        		keep_tomorrow[s_id] = rows
        		continue 	#紀錄後則跳過此巡
        	hold_stock, cash_account, trade_record,current_trend = RSI_method(rows, Date, find_price_time["MATCH_TIME"] , find_price_time["close"],hold_stock,cash_account,trade_record,current_trend,RSI_column_name, RSI_BUY, RSI_SELL)
              #最後一日交易日，則將持倉全部以當天最後一筆價格進行平倉
        if i == data_list[-1]:
            for index, rows in hold_stock.iterrows():
              stock_id = rows["stockid"]
              rows_matchtime = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id)].iloc[-1]['MATCH_TIME']
              try:
              	#取得最後一筆價格
              	find_price = all_daily_data[(all_daily_data["STOCK_SYMBOL"] == stock_id) & (all_daily_data["MATCH_TIME"] == rows_matchtime)].iloc[-1]['close']
              except:
              	find_price = rows["close"] #若失敗則以當初購買價格平倉
              cash_account,hold_stock,trade_record = Offset(stock_id, Date, find_price, cash_account,hold_stock,trade_record)
    trade_record["bought_time"]=trade_record["bought_time"]/100000000
    trade_record["sold_time"]=trade_record["sold_time"]/100000000
    trade_record["Algorithm"] = 1 #演算法、策略編號
    trade_record["offset_date"] = trade_record[["bought_date","sold_date"]].max(axis=1) #抓出平倉日
    trade_record.index.names = ['trade_no'] #將index命名為trade_no
    trade_record =  trade_record.sort_values(by=["offset_date",'trade_no']) 	#依據平倉日、交易順序進行排序
    trade_record = trade_record.reset_index(drop=True) #重設index
    trade_record.index.names = ['trade_no'] #將index命名為trade_no
    trade_record = trade_record.drop(["offset_date","cash_account"], axis=1) #將多餘的紀錄刪除 
    print(hold_stock) #最後應該為空DataFrame
    print(trade_record)
    trade_record.to_csv(r"./trade_record_RSI.csv", sep = ",") #輸出成CSV檔
    return trade_record
if __name__ == '__main__':
	stock_list = ['2408','2421','2313','3661','2330','1409','2481','2515']
    #RSI間隔、買進條件、賣出條件參數
	RSI_period = 15
	RSI_BUY = 14
	RSI_SELL = 70
	work(stock_list,RSI_period,RSI_BUY,RSI_SELL)
	crawl_price()

#def crawl_price():
#    stock_list = ['2408','2421','2313','3661','2330','1409','2481','2515']
#    for s_id in stock_list:
#     df_data_dict[s_id] = pd.DataFrame(columns = ['STOCK_SYMBOL', 'MATCH_TIME', 'close', 'highest', 'lowest', 'Quantity', 'open','date'])
#     for i in data_list:
#       date = i.split("_")[0] #當日日期	 
#       all_daily_data = pd.read_csv(r"./app/merged_data/merged_data_5min/" + i ,sep = "," , low_memory=False)	#讀取檔案
#       for s_id in stock_list:
#         df1 =pd.DataFrame( all_daily_data[all_daily_data["STOCK_SYMBOL"] == str(s_id)] )
#         df1.loc[:,date]=date
#         df1.rename(columns={date:'date'},inplace = True)
#         df=pd.concat([df_data_dict[s_id],df1])
#         df_data_dict[s_id] = df
#         #df=df1
#         df=df_data_dict[s_id]['close'].plot()
 