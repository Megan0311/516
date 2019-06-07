import io
import os
import requests
import pymysql.cursors
import pandas as pd
import numpy as np
import datetime
import time
#引用talib相關套件
import talib
from talib import MA_Type

def SMAWMA(df,count, acmroi, winnum, winfact,revenue_sum,avg_return,total_return_ratio):
    #pd.options.mode.chained_assignment = None #
    
    trade_record = pd.DataFrame(data=None, index=None,columns = ["stockid","bought_date","bought_time","bought_price","sold_date","sold_time","sold_price","QTY","cash_account","LS_type"])
    highest= np.array(df['highest'], dtype=float)
    close = np.array(df['close'], dtype=float)
    vol =df['Quantity'].as_matrix().astype("float64") 
    df['volMA'] = talib.SMA(vol, timeperiod=20)
    SMA = talib.SMA(highest,30) #close 代進SMA方法做計算
    WMA = talib.WMA(close,5) #close 代進WMA方法做計算
    df['SMA'] = SMA
    df['WMA'] = WMA
#    print(df)
    #設定初始值
    
    df["stockid"]= np.nan
    df["bought_date"]= np.nan
    df["bought_time"]= np.nan
    df["bought_price"]= np.nan
    df["sold_date"]= np.nan
    df["sold_time"]= np.nan
    df["sold_price"]= np.nan
    df["QTY"]=1000
    df["cash_account"]= np.nan
    df["LS_type"]=0
    df['XBuy'] = np.nan
    df['YBuy'] = np.nan
    df['XSell'] = np.nan
    df['YSell'] = np.nan
    row = len(df)
    flag1 = False
    flag = False
    change = 0
    buyprice=[]
    sellprice=[] 
    win = 0
    loss = 0
    roi =0
    revenue=0
    total_return=0
    revenue_sum=0
    for i in range(row):
        change =df['WMA'].iloc[i]/ df['SMA'].iloc[i]
       
        if (flag1 == False) & (df['WMA'].iloc[i] <df['SMA'].iloc[i]) & (change <= 0.948)& (float(df['Quantity'].iloc[i]) >=(float(df['volMA'].iloc[i]))):
            df['XSell'].iloc[i] = df['MATCH_TIME'].iloc[i]
            df['YSell'].iloc[i] = df['close'].iloc[i]
            sellprice= df['close'].iloc[i]
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["sold_date"].iloc[i]=df['date'].iloc[i]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["sold_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]= round(float(5000000+float(buyprice*1.001425-sellprice*(1-0.001425*0.6-0.001))*1000),0)
            df["LS_type"].iloc[i]=2
            print("S2賣出價"+str(sellprice))
            flag1 = True
        elif (flag == False) & (df['WMA'].iloc[i] <= df['SMA'].iloc[i]) &(change <= 0.995) & (float(df['Quantity'].iloc[i]) >=(float(df['volMA'].iloc[i]))):
            df['XBuy'].iloc[i] = df['MATCH_TIME'].iloc[i]
            df['YBuy'].iloc[i] = df['close'].iloc[i]
            buyprice =df['close'].iloc[i]
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["bought_date"].iloc[i]=df['date'].iloc[i]
            df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["bought_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]= round(float(5000000-float(buyprice)*1000*(1-0.001425*0.6-0.001)),0)
            df["LS_type"].iloc[i]=1
            flag = True
            print("S1買進價"+str(buyprice))
        
        elif(flag1 == True) & (df['WMA'].iloc[i] >= df['SMA'].iloc[i])& (change >= 1.009) & (float(df['Quantity'].iloc[i]) >=(float(df['volMA'].iloc[i]))):
          try:  
            df['XBuy'].iloc[i] = df['MATCH_TIME'].iloc[i+1]
            df['YBuy'].iloc[i] = df['close'].iloc[i+1]
            buyprice =df['close'].iloc[i+1]
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i+1]
            df["bought_date"].iloc[i]=df['date'].iloc[i+1]
            df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i+1]
            df["bought_price"].iloc[i]=df['close'].iloc[i+1]
            #df["cash_account"].iloc[i]= round(float(5000000-float(buyprice)*1000*(1-0.001425*0.6-0.001)),0)
            df["LS_type"].iloc[i]=2
            flag1 = False
            print("S2回補價"+str(buyprice))
            count += 1
            revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000 
            revenue_sum +=revenue
            premium=revenue/((buyprice*1.001425)*1000)
            total_return +=premium
            avg_return=(total_return/count)
            total_return_ratio=round((revenue_sum/5000000),3)*100
            [roi, winnum] =roical(buyprice, sellprice, winnum)
            acmroi += roi
            [loss, win] = winfactor(buyprice, sellprice, loss, win)
            
          except (ValueError): 
            continue 
        elif (flag == True) & (df['WMA'].iloc[i] >df['SMA'].iloc[i]) & (change >= 1.021) & (float(df['Quantity'].iloc[i]) >=(float(df['volMA'].iloc[i]))):  
          try: 
            df['XSell'].iloc[i] = df['MATCH_TIME'].iloc[i+2]
            df['YSell'].iloc[i] = df['close'].iloc[i+2]
            sellprice= df['close'].iloc[i+2]
            count += 1
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i+2]
            df["sold_date"].iloc[i]=df['date'].iloc[i+2]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i+2]
            df["sold_price"].iloc[i]=df['close'].iloc[i+2]
            #df["cash_account"].iloc[i]= round(float(5000000+float(buyprice*1.001425-sellprice*(1-0.001425*0.6-0.001))*1000),0)
            df["LS_type"].iloc[i]=1
            flag = False
            print("S1賣出價"+str(sellprice))
            [roi, winnum] =roical(buyprice, sellprice, winnum)
            acmroi += roi
            [loss, win] = winfactor(buyprice, sellprice, loss, win)
            
            revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000 
            revenue_sum +=revenue
            premium=revenue/((buyprice*1.001425)*1000)
            total_return +=premium
            avg_return=(total_return/count)
            total_return_ratio=round((revenue_sum/5000000),3)*100
          except (ValueError): 
            continue
          if (flag == True & i==(row-1)):
              df['XSell'].iloc[i] = df['MATCH_TIME'].iloc[i]
              df['YSell'].iloc[i] = df['close'].iloc[i]
              sellprice=df['close'].iloc[i]
              df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
              df["sold_date"].iloc[i]=df['date'].iloc[i]
              df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
              df["sold_price"].iloc[i]=df['close'].iloc[i]
              df["QTY"].iloc[i]=1000
              #df["cash_account"].iloc[i]= (5000000+(buyprice*1.001425*0.6-sellprice* (1-0.001425*0.6-0.001))*1000)
              df["LS_type"].iloc[i]=1
              count += 1
              [roi, winnum] = roical(buyprice, sellprice, winnum)
              acmroi += roi
              [loss, win] = winfactor(buyprice, sellprice, loss, win)
              print("S最後被迫賣價"+str(sellprice))
              #revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000 
              #revenue_sum +=revenue
              #premium=revenue/((buyprice*1.001425)*1000)
              #total_return +=premium
              #avg_return=(total_return/count)
              #total_return_ratio=round((revenue_sum/5000000),3)*100
          elif (flag1 == True & i==(row-1)): 
              df['Xbuy'][i] = df['MATCH_TIME'].iloc[i]
              df['Ybuy'][i] = df['close'].iloc[i]
              buyprice=df['close'].iloc[i]
              df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
              df["bought_date"].iloc[i]=df['date'].iloc[i]
              df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
              df["bought_price"].iloc[i]=df['close'].iloc[i]
              df["QTY"].iloc[i]=1000
              #df["cash_account"].iloc[i]= (5000000+(buyprice*1.001425*0.6-sellprice* (1-0.001425*0.6-0.001))*1000)
              df["LS_type"].iloc[i]=2
              count += 1
              [roi, winnum] = roical(buyprice, sellprice, winnum)
              acmroi += roi
              print("S2最後被迫回補價"+str(buyprice))
              #revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000 
              #revenue_sum +=round(revenue,2)
              #premium=revenue/((buyprice*1.001425)*1000)
              #total_return +=premium
              #avg_return=(total_return/count)
              #total_return_ratio=round((revenue_sum/5000000),3)*100
          elif (flag1 == False or flag == False&  i==(row-10)):
              break
    if (loss == 0):
        loss = 1
    winvar = win / loss
    print(' win = ', win)
    print('loss = ', loss)
    print('winvar = ', winvar)
    print('revenue_sum=',revenue_sum)
    if (count == 0):
        count = 0.01  
    #str1 = 'SMAWMA策略: ' +'交易次數 = '+ str(count) + ' 次; ' + '累計報酬率 = ' + str(round(acmroi*100, 2)) + '%;' + '勝率 = ' + str(round((winnum/count)*100,2)) + '%' + '; 獲利因子 = ' + str(round(winvar, 2) )+ '%' + '; 總收益 = ' + str(round(revenue_sum, 2)+ '; 平均報酬率 = ' + str(round(avg_return*100, 2))+ '%'+ '; 夏普指數 = ' + str(round(sharpe_ratio*100, 2))+ '%'+ '; MDD = ' + str(round(MDD, 2))+ '; 總資產報酬率 = ' + str(round(total_return_ratio*100, 2))+ '%')
    #print(str1)
    trade_record.index.names = ['trade_no']
    trade_record["stockid"]= df["stockid"]
    trade_record["bought_date"]=df["bought_date"]
    trade_record["bought_time"]=df["bought_time"]
    trade_record["bought_price"]=df["bought_price"]
    trade_record["sold_date"]=df["sold_date"]
    trade_record["sold_time"]= df["sold_time"]
    trade_record["sold_price"]=df["sold_price"]
    #trade_record["cash_account"]=df["cash_account"]
    trade_record["LS_type"]=df["LS_type"]
    trade_record["offset_date"] = trade_record[["bought_date","sold_date"]].max(axis=1) #抓出平倉日
    #trade_record =trade_record.sort_values(by=["offset_date",'trade_no'])
    #trade_record = trade_record.drop(["bought_date"],axis=1) #將多餘的紀錄刪除
    #trade_record =trade_record.reset_index(drop=True) #重設index
    #trade_record["Algorithm"] =2
    #trade_record["QTY"]=1000
    trade_record.to_csv(r"./SAM_trade_record.csv", sep = ",")
    #print(str1)
    #print(trade_record)
    #print(df)
    #print(trade_record)
    return (count, acmroi, winnum, winvar,revenue_sum,avg_return,total_return_ratio ) 
#BbandMA自訂函數
def BBandMA(df, count, acmroi, winnum, winfact,revenue_sum,avg_return,total_return_ratio, MDD,sharpe_ratio):
    #設定初始值
  #for stk_no in range(df):
    #total_trade_record=pd.DataFrame(data=None, index=None,columns = ["stockid","bought_date","bought_time","bought_price","sold_date","sold_time","sold_price","QTY","cash_account","LS_type"])
    trade_record = pd.DataFrame(data=None, index=None,columns = ["stockid","bought_date","bought_time","bought_price","sold_date","sold_time","sold_price","QTY","cash_account","LS_type"])
    df["stockid"]= np.nan
    df["bought_date"]= np.nan
    df["bought_time"]= np.nan
    df["bought_price"]= np.nan
    df["sold_date"]= np.nan
    df["sold_time"]= np.nan
    df["sold_price"]= np.nan
    df["QTY"]=1000
    df["cash_account"]= np.nan
    df["LS_type"]=0
    df['BBXBuy'] = np.nan
    df['BBYBuy'] = np.nan
    df['BBXSell'] = np.nan
    df['BBYSell'] = np.nan
    #計算BBand上, 中, 下線
    close = np.array(df['close'], dtype=float)
    upper, middle, lower = talib.BBANDS(close, timeperiod=10, nbdevup=0.03, nbdevdn=2, matype=MA_Type.T3)
    df['BBupper'] = upper
    df['BBlower'] = lower
    vol =df['Quantity'].as_matrix().astype("float64") 
    df['volMA'] = talib.SMA(vol, timeperiod=30)   
    flag = False
    buyprice=[]
    sellprice=[]
    win = 0
    loss = 0
    roi = 0
    revenue=0
    total_return=0
    revenue_sum=0
    for i in range(len(df)):
        if (flag == False) & (float(df['highest'].iloc[i])> float(df['BBupper'].iloc[i])*1.075)   :
        #if (flag == False) & (float(df['highest'].iloc[i]) > float(df['BBupper'].iloc[i])) & (float(df['Quantity'].iloc[i]) > (float(df['volMA'].iloc[i]))) & (float(df['close'].iloc[i])*1.01 <(float(df['BBupper'].iloc[i]))) :
            sellprice= df['close'].iloc[i]
            df['BBXSell'].iloc[i] = df['MATCH_TIME'].iloc[i]
            df['BBYSell'].iloc[i] = sellprice
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["sold_date"].iloc[i]=df['date'].iloc[i]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["sold_price"].iloc[i]=df['close'].iloc[i]  
            #df["cash_account"].iloc[i]= round((5000000-float(buyprice)*1000*1.001425*0.6),0)
            flag = True
            print("BB賣價"+str(sellprice))
        if (flag == True) & (float(df['lowest'].iloc[i])*0.953>=float(df['BBlower'].iloc[i])<float(df['BBupper'].iloc[i])) :
            buyprice=df['close'].iloc[i+6]
            df['BBXBuy'].iloc[i] = df['MATCH_TIME'].iloc[i+6]
            df['BBYBuy'].iloc[i] = buyprice
            print("BB買進價"+str(buyprice))
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i+6]
            df["bought_date"].iloc[i]=df['date'].iloc[i+6]
            df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i+6]
            df["bought_price"].iloc[i]=df['close'].iloc[i+6]
            
            #df["cash_account"].iloc[i]= round( (5000000+float(buyprice* 1.001425*0.6-sellprice* (1-0.001425*0.6-0.001))*1000),0)
            df["LS_type"].iloc[i]=2
            count += 1
            flag = False
            [roi, winnum] =roical(buyprice,sellprice, winnum)
            acmroi += roi
            [loss, win] = winfactor(buyprice, sellprice, loss, win)
            revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000 
            revenue_sum +=round((revenue),2)
            premium=revenue/((buyprice*1.001425)*1000)
            total_return +=premium
            avg_return=(total_return/count)
            total_return_ratio=round((revenue_sum/5000000),3)*100
            #return_standard+=premium.std()#這是以半年投資報酬標準差,所以標準差將會半年化算Sharp指數
            #sharpe_ratio=((avg_return - 0.01095/2)/ return_standard)*100
                       
            if (flag == True & i==(len(df)-1)):
                df['BBXBuy'][i] = sellprice
                df['BBYBuy'][i] = df['MATCH_TIME'].iloc[i]
                df['BBXBuy'][i] = buyprice
                df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
                df["bought_date"].iloc[i]=df['date'].iloc[i]
                df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
                df["bought_price"].iloc[i]=df['close'].iloc[i]
                #df["cash_account"].iloc[i]=round( (5000000+float(buyprice* (1+0.001425*0.6)-sellprice*(1-0.001425*0.6-0.001))*1000),0)
                df["LS_type"].iloc[i]=2
                count += 1
                [roi, winnum] = roical(buyprice, sellprice, winnum)
                acmroi += roi
                [loss, win] = winfactor(buyprice, sellprice, loss, win)
                print("BB最後1天買入價"+str(buyprice))
                revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000 
                revenue_sum +=round((revenue),2)
                premium=revenue/(buyprice*1.001425)
                total_return +=premium
                avg_return=(total_return/count)
                total_return_ratio=round((revenue_sum/5000000),3)*100
                #return_standard+=premium.std()#這是以半年投資報酬標準差,所以將會半年化算Sharp指數
                #sharpe_ratio=((avg_return - 0.01095/2)/ return_standard)*100
            elif ( flag == False& i==(len(df)-10)):
                break   
    
    if (loss == 0):
        loss = 1
    winvar = win / loss
    if (count == 0):
        count = 0.01      
    #str1 = 'BBand策略: ' + '交易次數 = '+ str(count) + ' 次; ' + '累計報酬率 = ' + str(round(acmroi*100, 2)) + '%; ' + '勝率 = ' + str(round((winnum/count)*100,2)) + '%' + '; 獲利因子 = ' + str(round(winvar, 2))+ '%' + '; 總收益 = ' + str(round(revenue_sum, 2)+ '; 平均報酬率 = ' + str(round(avg_return*100, 2))+ '%'+ '; 夏普指數 = ' + str(round(sharpe_ratio*100, 2))+ '%'+ '; MDD = ' + str(round(MDD, 2))+ '; 總資產報酬率 = ' + str(round(total_return_ratio*100, 2))+ '%')
    
   
    trade_record.index.names = ['trade_no']
    trade_record["stockid"]= df["stockid"]
    trade_record["bought_date"]=df["bought_date"]
    trade_record["bought_time"]=df["bought_time"]
    trade_record["bought_price"]=df["bought_price"]
    trade_record["sold_date"]=df["sold_date"]
    trade_record["sold_time"]= df["sold_time"]
    trade_record["sold_price"]=df["sold_price"]
    #trade_record["cash_account"]=df["cash_account"]
    trade_record["offset_date"] = trade_record[["bought_date","sold_date"]].max(axis=1) #抓出平倉日
    #trade_record =trade_record.sort_values(by=["offset_date",'trade_no'])
    #trade_record = trade_record.drop(["bought_date"],axis=1) #將多餘的紀錄刪除
    #trade_record.loc[:,["offset_date"]].dropna(axis=1)
    #trade_record.loc[:,["bought_date"],["bought_time"],["bought_price"]].sort_values(by=["bought_date"])
    trade_record.loc[:,["bought_date"]].dropna(axis=1)
    #trade_record.loc[:,["sell_date"],["sell_time"],["sell_price"]].sort_values(by=["sold_date"])
    #trade_record =trade_record.reset_index(drop=True) #重設index
    #trade_record["LS_type"]=df["LS_type"]=2
    #trade_record["Algorithm"] =3
    #trade_record["QTY"]=1000
    #total_trade_record.append(trade_record)
    trade_record.to_csv(r"./BBands_record.csv", sep = ",")
    #print(str1)

    #print(trade_record)
       
    return (count, acmroi, winnum, winvar,revenue_sum,avg_return,total_return_ratio, MDD,sharpe_ratio) 
def revenue_sum(buyprice, sellprice,count):
    revenue=((sellprice*0.995575)- (buyprice*1.001425))*1000
    revenue_sum +=revenue
    premium=revenue/((buyprice*1.001425)*1000)
    total_return +=premium
    avg_return=(total_return/count)
    total_return_ratio=round((revenue_sum/5000000),3)*100
    #return_standard+=premium.std()#這是以半年投資報酬標準差,所以利息將會半年化算Sharp指數
    #sharpe_ratio = (avg_return - 0.01095/2) / (return_standard)	#夏普指標。無風險利率已2019/05各大銀行最高固定利率(1.095%)計算
    peak = 0
    MDD = 0
    for i in revenue:
     if i > peak:
       peak = i
     diff = peak - i
     if diff > MDD:
      MDD = diff
        
    return(revenue_sum,avg_return,total_return_ratio)

def roical(buyprice, sellprice, winnum):
    roi = (sellprice*0.995575 - buyprice*1.001425) /buyprice*1.001425
    if roi > 0:
        winnum+=1
    return (roi, winnum)
#計算獲利因子要用的累計獲利金額與損失金額
def winfactor(buyprice, sellprice, loss, win):
    payoff = buyprice*1.001425 - sellprice*0.995575
    if payoff > 0:
        loss += payoff
    else:
        win += (-payoff)
    return (loss, win)
#查詢資料庫的股票資料
def select_db(stock_id):
   
   df_data_dict={}
   data_list = os.listdir(r"./app/merged_data/merged_data_5min")
   #print(data_list)   
   stock_list=[]
   if(stock_id == "all"):
     stock_list = ['2408','2421','2313','3661','2330','1409','2481','2515']
   else:
     stock_list=[str(stock_id)]
   for s_id in stock_list:
       df_data_dict[s_id] = pd.DataFrame(columns = ['STOCK_SYMBOL', 'MATCH_TIME', 'close', 'highest', 'lowest', 'Quantity', 'open','date'])
       try:
        for i in data_list:
         date = i.split("_")[0] #當日日期	

         all_daily_data = pd.read_csv(r"./app/merged_data/merged_data_5min/" + i ,sep = "," , low_memory=False)	#讀取檔案
         for s_id in stock_list:
          df1 =pd.DataFrame( all_daily_data[all_daily_data["STOCK_SYMBOL"] == str(s_id)] )
          df1.loc[:,date]=date
          df1.rename(columns={date:'date'},inplace = True)
          
          df=pd.concat([df_data_dict[s_id],df1])
          df_data_dict[s_id] = df
       except ValueError:
             continue
         #df=df1
         #print(df)
       return(df)
 

   
#主程式
def main(stock_id, dfSMA, dfBBand,i): 
    df = select_db(stock_id)     
    #呼叫SMAWMA策略
    [count, acmroi, winnum, winfact,revenue_sum,avg_return,total_return_ratio] = SMAWMA(df, count = 0, acmroi = 0, winnum = 0, winfact = 0,revenue_sum=0,avg_return=0,total_return_ratio=0)
    if (count == 0):
        count = 0.01  
    dfSMA['times'].iloc[i] =count
    dfSMA['earn_loss'].iloc[i] = round(acmroi * 100,2)
    dfSMA['Success_rate'].iloc[i] = round((winnum /count) * 100,3)
    dfSMA['Profit_Factor'].iloc[i] = round(winfact,2)
    dfSMA['revenue'].iloc[i]=round(revenue_sum,0)
    dfSMA['avg_return'].iloc[i]=round((avg_return)*100,3)
    dfSMA['total_return'].iloc[i]=(total_return_ratio)*100
    #dfSMA[' MDD'].iloc[i]=round(MDD,2)
    #dfSMA['sharpe_ratio'].iloc[i]=(sharpe_ratio*100)
    print('dfSMA = ', dfSMA)
   
    #呼叫BBand策略
    [count, acmroi, winnum, winfact,revenue_sum,avg_return,total_return_ratio, MDD,sharpe_ratio] = BBandMA(df, count = 0, acmroi = 0, winnum = 0, winfact = 0,revenue_sum=0,avg_return=0,total_return_ratio=0, MDD=0,sharpe_ratio=0)  
    if (count == 0):
        count = 0.01        
    dfBBand['times'].iloc[i] = count
    dfBBand['earn_loss'].iloc[i] = round(acmroi * 100,2)
    dfBBand['Success_rate'].iloc[i] = round((winnum / count) * 100,2)
    dfBBand['Profit_Factor'].iloc[i] =round(winfact,2)
    dfBBand['revenue'].iloc[i]=round(revenue_sum,0)
    dfBBand['avg_return'].iloc[i]=round((avg_return)*100,3)
    dfBBand['total_return'].iloc[i]=(total_return_ratio)*100
    #dfBBand[' MDD'].iloc[i]=round(MDD,2)
    #dfBBand['sharpe_ratio'].iloc[i]=(sharpe_ratio*100)
    print('dfBBand = ', dfBBand)
    return (dfSMA.to_html(classes='')+dfBBand.to_html(classes=''))
#呼叫主程式並代入股票代號與起訖日期
if __name__ == "__main__":   
    starttime = time.clock()
    dfSMA = pd.read_excel(r'result_SMA.xlsx')
    dfBBand = pd.read_excel(r'result_BBand.xlsx')    
    writer= pd.ExcelWriter(r'FinalResult.xlsx')
    writer2= pd.ExcelWriter(r'result_SMA.xlsx')
    writer3= pd.ExcelWriter(r'result_BBand.xlsx')
    for i in range(len(dfSMA)):
        print(dfSMA['STOCK_SYMBOL'].iloc[i])
        stkno=dfSMA['STOCK_SYMBOL'].iloc[i]
        try:
            main(stkno, dfSMA, dfBBand, i)
        except:
            continue
    print('dfSMA = ', dfSMA)
    print('dfBBand = ', dfBBand)
    dfSMA.to_excel(writer, 'SMA')
    dfSMA.to_excel(writer2, 'SMA')
    dfBBand.to_excel(writer, 'BBand')
    dfBBand.to_excel(writer3, 'BBand')
   
    writer.save()
    writer2.save()
    writer3.save()
    
    endtime = time.clock()
    print('程式執行時間 = %d%s' %(round(endtime-starttime), '秒'))
