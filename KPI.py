# -*- coding: utf-8 -*-
import pandas as pd
def work():
	df = pd.read_csv(r"./trade_record.csv",sep = ",")
	#df = pd.read_csv(r"./myapp/RSI_method/trade_test.csv",sep = ",")
	
	KPI = pd.DataFrame(columns = ["over_bought","trade_times", "avg_return", "Success_rate", "Profit_Factor", "earning_loss_rate","total_return","MDD","sharpe_ratio"])
	
	buy_cost = df["bought_price"] * 1.001425 	#買入成本
	sold_revenue= df["sold_price"] - df["sold_price"] * (0.001 + 0.001425) 	#賣出收入
	earning_data = df[buy_cost < sold_revenue]  #獲益交易資料
	loss_data = df[buy_cost >= sold_revenue] 	#虧損交易資料

	#檢查交易期間現金資產是否小於0
	bought_data = df[["trade_no","bought_date","bought_price","QTY","LS_type"]] #分離買入資料
	bought_data = bought_data.assign(bs_type=1) #給定標記1(先假設全做多)
	bought_data.columns = ["trade_no","trade_date","price","QTY","LS_type","bs_type"]
	bought_data.loc[bought_data["LS_type"] == 2,"bs_type"] = 4 #標記放空回補，順序最後

	sold_data = df[["trade_no","sold_date","sold_price","QTY","LS_type"]]
	sold_data = sold_data.assign(bs_type=2)
	sold_data.columns = ["trade_no","trade_date","price","QTY","LS_type","bs_type"]
	sold_data.loc[sold_data["LS_type"] == 2,"bs_type"] = 3 #標記放空賣出，順序優先

	all_trade_data  = pd.concat([bought_data, sold_data])
	all_trade_data = all_trade_data.sort_values(by=["trade_date","trade_no","bs_type"], ascending=[1, 1, 1])
	all_trade_data = all_trade_data.reset_index(drop=True)

	cash_remain = 5000000
	over_bought = 0
	for index, row in all_trade_data.iterrows():
		if row["bs_type"] == 1:
			cash_remain-= (row["price"] * row["QTY"]) * 1.001425
		elif row["bs_type"] == 2:
			cash_remain+= (row["price"] * row["QTY"]) * (1 - 0.003 - 0.001425)
		elif row["bs_type"] == 3:
			cash_remain+= row["price"] * row["QTY"]
			cash_remain-= (row["price"] * row["QTY"]) * (1 - 0.003 - 0.001425)
		elif row["bs_type"] == 4:
			#找出持倉時的資料
			MK_data = all_trade_data[(all_trade_data["trade_no"] == row["trade_no"]) & (all_trade_data["bs_type"] == 3)].iloc[0]
			cash_remain+= MK_data["price"] * MK_data["QTY"]
			cash_remain-= (row["price"] * row["QTY"]) * 1.001425

		if cash_remain < 0:
			over_bought = 1
			break
	if over_bought == 0:
		print("無超買之虞#")
	else:
		print("有超買之虞!!!")
	
	
	#交易次數
	trade_times = len(df)
	
	#平均報酬率
	avg_return_data = sold_revenue / buy_cost
	avg_return = avg_return_data.mean() - 1
	
	#勝率
	Success_rate = len(df[buy_cost < buy_cost]) / trade_times
	
	#獲利因子
	sum_earning = sum((earning_data["sold_price"] * (1- 0.001425 - 0.003) - earning_data["bought_price"] * 1.001425) * earning_data["QTY"])
	sum_loss =  sum((loss_data["sold_price"] * (1- 0.001425 - 0.003) - loss_data["bought_price"]  * 1.001425) * loss_data["QTY"])
	Profit_Factor = abs(sum_earning / sum_loss)
	
	#賺賠比
	earning_avg = (earning_data["sold_price"] * (1- 0.001425 - 0.003) - earning_data["bought_price"] * 1.001425).mean()
	loss_avg = (loss_data["sold_price"] * (1- 0.001425 - 0.003) - loss_data["bought_price"] * 1.001425).mean()
	earning_loss_rate = abs(earning_avg / loss_avg)
	
	def return_process(temp_df):
		result = (temp_df["sold_price"] * (1 - 0.003 - 0.001425) - temp_df["bought_price"] * 1.003) * temp_df["QTY"]
		return result
		
	#MDD(最大回檔)
	df["offset_date"] = df[["bought_date","sold_date"]].max(axis=1)
	MDD_data = df.copy()
	MDD_data = MDD_data.sort_values(["offset_date","trade_no"])
	MDD_data["single_return"] = MDD_data.apply(return_process, axis = 1)
	MDD_data["acuumulate_return"] = MDD_data["single_return"].cumsum()

	peak = 0
	MDD = 0
	for i in MDD_data["acuumulate_return"]:
		if i > peak:
			peak = i
		diff = peak - i
		if diff > MDD:
			MDD = diff
			
	#總資產報酬率
	total_return = MDD_data[MDD_data["trade_no"] == MDD_data["trade_no"].max()]["acuumulate_return"].iloc[0] / 5000000
	
	
	#穩定度(累計報酬率的標準差)
	return_standard= avg_return_data.std()
	sharpe_ratio = (avg_return - 0.01095) / return_standard	#夏普指標。無風險利率已2019/05各大銀行最高固定利率(1.095%)計算
	KPI.loc[0] =  [over_bought,trade_times, avg_return, Success_rate, Profit_Factor, earning_loss_rate, total_return, MDD, sharpe_ratio]
	KPI.to_csv("KPI.csv",sep = ",", index = False)
	print(KPI.T)
	return KPI

if __name__ == "__main__":
	work()