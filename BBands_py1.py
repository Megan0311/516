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

def SMAWMA(df,count, acmroi, winnum, winfact):
    #pd.options.mode.chained_assignment = None #
    
    trade_record = pd.DataFrame(data=None, index=None,columns = ["stockid","bought_date","bought_time","bought_price","sold_date","sold_time","sold_price","QTY","cash_account","LS_type"])
    highest=  np.array(df['highest'], dtype=float)
    close = np.array(df['close'], dtype=float)
    SMA = talib.SMA(highest,20) #close 代進SMA方法做計算
    WMA = talib.WMA(highest,3) #close 代進WMA方法做計算
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
    flag = False
    change = 0
    buyprice=[]
    sellprice=[] 
    win = 0
    loss = 0
    roi =0
    for i in range(row):
        change =df['WMA'].iloc[i]/ df['SMA'].iloc[i]
        if (flag == True) & (df['WMA'].iloc[i] <= df['SMA'].iloc[i]) & (change <= 1.005)&(df['close'].iloc[i]*1.002 >df['WMA'].iloc[i]):
        
            df['XBuy'].iloc[i] = df['MATCH_TIME'].iloc[i]
            df['YBuy'].iloc[i] = df['close'].iloc[i]
            buyprice =df['close'].iloc[i]
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["bought_date"].iloc[i]=df['date'].iloc[i]
            df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["bought_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]= round(float(5000000-float(buyprice)*1000*(1-0.001425*0.6-0.001)),0)
            
            flag = True
            print("S買進價"+str(buyprice))
        if (flag == False) & (df['WMA'].iloc[i] >df['SMA'].iloc[i]) & (change >= 1.01)&(df['close'].iloc[i]*1.002 <df['WMA'].iloc[i] ):
        
            df['XSell'][i] = df['MATCH_TIME'].iloc[i]
            df['YSell'][i] = df['close'].iloc[i]
            sellprice= df['close'].iloc[i]
            #if (sellprice==buyprice):

            count += 1
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["sold_date"].iloc[i]=df['date'].iloc[i]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["sold_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]= round(float(5000000+float(buyprice*1.001425-sellprice*(1-0.001425*0.6-0.001))*1000),0)
            df["LS_type"].iloc[i]=1
            flag = False
            print("S賣價"+str(sellprice))
            [roi, winnum] =roical(buyprice, sellprice, winnum)
            acmroi += roi
            [loss, win] = winfactor(buyprice, sellprice, loss, win)
        if (flag == True & i==(row-5)):
            df['XSell'][i] = df['MATCH_TIME'].iloc[i]
            df['YSell'][i] = df['close'].iloc[i]
            sellprice=df['close'].iloc[i]
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["sold_date"].iloc[i]=df['date'].iloc[i]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["sold_price"].iloc[i]=df['close'].iloc[i]
            df["QTY"].iloc[i]=1000
            #df["cash_account"].iloc[i]= (5000000+(buyprice*1.001425*0.6-sellprice* (1-0.001425*0.6-0.001))*1000)
            df["LS_type"].iloc[i]=1
            count += 1
            [roi, winnum] = roical(buypric, sellprice, winnum)
            acmroi += roi
            print("S最後被迫賣價"+str(sellprice))
    if (loss == 0):
        loss = 1
    winvar = win / loss
    print(' win = ', win)
    print('loss = ', loss)
    print('winvar = ', winvar)
    if (count == 0):
        count = 0.01  
    str1 = 'SMAWMA策略: ' +'交易次數 = '+ str(count) + ' 次; ' + '累計報酬率 = ' + str(round(acmroi*100, 2)) + '%;' + '勝率 = ' + str(round((winnum/count)*100,2)) + '%' + '; 獲利因子 = ' + str(round(winvar, 2)) 
    
    trade_record.index.names = ['trade_no']
    trade_record["stockid"]= df["stockid"]
    trade_record["bought_date"]=df["bought_date"]
    trade_record["bought_time"]=df["bought_time"]
    trade_record["bought_price"]=df["bought_price"]
    trade_record["sold_date"]=df["sold_date"]
    trade_record["sold_time"]= df["sold_time"]
    trade_record["sold_price"]=df["sold_price"]
    #trade_record["cash_account"]=df["cash_account"]
    trade_record["LS_type"]=df["LS_type"]=1
    #trade_record =trade_record.reset_index(drop=True) #重設index
    trade_record.index.names = ['trade_no'] #將index命名為trade_no
    #trade_record["offset_date"] = trade_record[["bought_date","sold_date"]].max(axis=1) #抓出平倉日
    #trade_record =trade_record.sort_values(by=["offset_date",'trade_no'])
    #trade_record = trade_record.drop(["offset_date","cash_account"], axis=1) #將多餘的紀錄刪除
    trade_record["Algorithm"] =2
    trade_record["QTY"]=1000
    #trade_record.to_csv(r"./SAM_trade_record.csv", sep = ",")
    print(str1)
    print(trade_record)
    #print(df)
    #print(trade_record)
    return (count, acmroi, winnum, winvar) 
#BbandMA自訂函數
def BBandMA(df, count, acmroi, winnum, winfact):
    #設定初始值
  #for stk_no in range(df):
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
    upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=0.35, nbdevdn=0.20, matype=MA_Type.T3)
    df['BBupper'] = upper
    df['BBlower'] = lower
    vol =df['Quantity'].as_matrix().astype("float64") 
    df['volMA'] = talib.SMA(vol, timeperiod=20)   
    flag = False
    buyprice=[]
    sellprice=[]
    win = 0
    loss = 0
    roi = 0
    for i in range(len(df)):
        if (flag == False) & (float(df['lowest'].iloc[i]) <float(df['BBlower'].iloc[i]))&  (float(df['Quantity'].iloc[i]) > (float(df['volMA'].iloc[i]))):
        #if (flag == False) & (float(df['highest'].iloc[i]) > float(df['BBupper'].iloc[i])) & (float(df['Quantity'].iloc[i]) > (float(df['volMA'].iloc[i]))) & (float(df['close'].iloc[i])*1.01 <(float(df['BBupper'].iloc[i]))) :
            buyprice=df['close'].iloc[i]
            df['BBXBuy'].iloc[i] = df['MATCH_TIME'].iloc[i]
            df['BBYBuy'].iloc[i] = buyprice
            print("BB買進價"+str(buyprice))
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["bought_date"].iloc[i]=df['date'].iloc[i]
            df["bought_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["bought_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]= round((5000000-float(buyprice)*1000*1.001425*0.6),0)
            flag = True
        if (flag == True) & (float(df['highest'].iloc[i]) > float(df['BBupper'].iloc[i])) & (float(df['Quantity'].iloc[i]) > (float(df['volMA'].iloc[i]))) & (float(df['close'].iloc[i])*1.01 <(float(df['BBupper'].iloc[i]))) :
        #if (flag == True) & (float(df['lowest'].iloc[i]) <float(df['BBlower'].iloc[i]))&  (float(df['Quantity'].iloc[i]) > (float(df['volMA'].iloc[i]))):
            sellprice= df['close'].iloc[i]
            #if sellprice==buyprice:
            #    flag=True
            #    continue
            df.BBXSell[i] = df['MATCH_TIME'].iloc[i]
            df.BBYSell[i] = sellprice
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["sold_date"].iloc[i]=df['date'].iloc[i]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["sold_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]= round( (5000000+float(buyprice* 1.001425*0.6-sellprice* (1-0.001425*0.6-0.001))*1000),0)
            df["LS_type"].iloc[i]=1
            count += 1
            flag = False 
            [roi, winnum] =roical(buyprice,sellprice, winnum)
            acmroi += roi
            [loss, win] = winfactor(buyprice, sellprice, loss, win)
            print("BB賣價"+str(sellprice))
        if (flag == True & i==(len(df)-5)):
            df.BBXSell[i] = df['MATCH_TIME'].iloc[i]
            sellprice= df['close'].iloc[i]
            df.BBYSell[i] = sellprice
            df.BBXSell[i] = df['MATCH_TIME'].iloc[i]
            df.BBYSell[i] = sellprice
            df["stockid"].iloc[i]=df['STOCK_SYMBOL'].iloc[i]
            df["sold_date"].iloc[i]=df['date'].iloc[i]
            df["sold_time"].iloc[i]=df['MATCH_TIME'].iloc[i]
            df["sold_price"].iloc[i]=df['close'].iloc[i]
            #df["cash_account"].iloc[i]=round( (5000000+float(buyprice* (1+0.001425*0.6)-sellprice*(1-0.001425*0.6-0.001))*1000),0)
            df["LS_type"].iloc[i]=1
            count += 1
            [roi, winnum] = roical(buyprice, sellprice, winnum)
            acmroi += roi
            print("BB最後1天賣出價"+str(sellprice))
    if (loss == 0):
        loss = 1
    winvar = win / loss
    if (count == 0):
        count = 0.01      
    str1 = 'BBand策略: ' + '交易次數 = '+ str(count) + ' 次; ' + '累計報酬率 = ' + str(round(acmroi*100, 2)) + '%; ' + '勝率 = ' + str(round((winnum/count)*100,2)) + '%' + '; 獲利因子 = ' + str(round(winvar, 2)) 
    trade_record.index.names = ['trade_no']
    trade_record["stockid"]= df["stockid"]
    trade_record["bought_date"]=df["bought_date"]
    trade_record["bought_time"]=df["bought_time"]
    trade_record["bought_price"]=df["bought_price"]
    trade_record["sold_date"]=df["sold_date"]
    trade_record["sold_time"]= df["sold_time"]
    trade_record["sold_price"]=df["sold_price"]
    #trade_record["cash_account"]=df["cash_account"]
    trade_record["LS_type"]=df["LS_type"]=1
    trade_record.index.names = ['trade_no'] #將index命名為trade_no
    #trade_record =trade_record.reset_index(drop=True) #重設index
    #trade_record["offset_date"] = trade_record[["bought_date","sold_date"]].max(axis=1) #抓出平倉日
    #trade_record =trade_record.sort_values(by=["offset_date",'trade_no'])
    #trade_record = trade_record.drop(["offset_date","cash_account"], axis=1) #將多餘的紀錄刪除
    trade_record["Algorithm"] =3
    trade_record["QTY"]=1000
    trade_record.to_csv(r"./BBands_record.csv", sep = ",")
    print(str1)
    print(trade_record)
       
    return (count, acmroi, winnum, winvar) 
#計算報酬率與勝率
def roical(buyprice, sellprice, winnum):
    roi = ((float(sellprice) - float(buyprice)) / float(buyprice))
    if roi > 0:
        winnum+=1
    return (roi, winnum)
#計算獲利因子要用的累計獲利金額與損失金額
def winfactor(buyprice, sellprice, loss, win):
    payoff = float(buyprice) - float(sellprice)
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
    [count, acmroi, winnum, winfact] = SMAWMA(df, count = 0, acmroi = 0, winnum = 0, winfact = 0)
    if (count == 0):
        count = 0.01  
    dfSMA['count'].iloc[i] =count
    dfSMA['roi'].iloc[i] = round(acmroi * 100,2)
    dfSMA['winrate'].iloc[i] = round((winnum /count) * 100,2)
    dfSMA['winfactor'].iloc[i] = winfact
    print('dfSMA = ', dfSMA)
   
    #呼叫BBand策略
    [count, acmroi, winnum, winfact] = BBandMA(df, count = 0, acmroi = 0, winnum = 0, winfact = 0)  
    if (count == 0):
        count = 0.02        
    dfBBand['count'].iloc[i] = count
    dfBBand['roi'].iloc[i] = round(acmroi * 100,2)
    dfBBand['winrate'].iloc[i] = round((winnum / count) * 100,2)
    dfBBand['winfactor'].iloc[i] = winfact  
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
        print(dfSMA['id'].iloc[i])
        stkno=dfSMA['id'].iloc[i]
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
#if(stock_id == "all"):
#         #print(dfSMA['STOCK_SYMBOL'].iloc[i])
#          stock_list= ['2408','2330','2421','2344','2454','5483','2317']
#          try:
#            main(stkno, dfSMA, dfBBand, i)
#          except:
#            continue
#        else:
#          try:main(stock_id, dfSMA, dfBBand, 0)
#          except:
#            continue