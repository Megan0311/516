"""
Definition of views.
"""

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.pyplot as plt
import os
from django.shortcuts import render
from django.http import HttpRequest
from django.http import HttpResponse
from django.template import RequestContext
from datetime import datetime
from Django516.main import work as trade_test_work
#from Django516.main import crawl_price as crawl_price
from Django516.KPI import work as KPI_work
#from django.http import JsonResponse
from Django516.BBands_py import main as BBands_main
import pandas as pd
import numpy as np
import csv



def RSI_page(request):
	return render(request,'app/RSI_page.html')
#def Stockpool(request):
#	return render(request,'app/Stockpool.html')

def home(request):
    """Renders the home page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'index.html',
        {
            'title':'Home Page',
            'year':datetime.now().year,
        }
    )

def contact(request):
    """Renders the contact page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/contact.html',
        {
            'title':'Contact',
            'message':'Your contact page.',
            'year':datetime.now().year,
        }
    )

def about(request):
    """Renders the about page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/about.html',
        {
            'title':'About',
            'message':'Your application description page.',
            'year':datetime.now().year,
        }
    )
def RSI_process(request):
	#stock_list = ['2408','2421','2313','3661','2330','1409','2481','2515']
    stock_list=[]
    stock_list1=request.GET["stock_list"]
    RSI_BUY = request.GET["RSI_BUY"]
    RSI_SELL = request.GET["RSI_SELL"]
    RSI_period=request.GET["RSI_period"]
    if(stock_list1 == "all"):
        stock_list = ['2421','2313','3661','2330','1409','2481','2515']
   
    response = trade_test_work(stock_list,RSI_period,RSI_BUY,RSI_SELL)
    response.index = response.index + 1 
     
    return HttpResponse(response.reset_index(level=0).to_html(classes='list', col_space=120,index = False))
def KPI_process(request):
	response = KPI_work()
	print(response)
	return HttpResponse(response.T.to_html(classes='', col_space=100,header = False))
def BBands(request):
	return render(request,'app/BBands.html')
def bo(request):
	return render(request,'app/bo.html')
def sm(request):
	return render(request,'app/sm.html')
def rsi(request):
	return render(request,'app/rsi.html')
def BBands_py(request):
    stock_id = request.GET['stock_id']
    if(stock_id == "all"):
      tables  = ['2421','2313','3661','2330','1409','2481','2515']
      dfSMA = pd.DataFrame()
      dfBBand = pd.DataFrame()
      #set DataFrame
      for index, table in enumerate(tables):
      	
         stock_id = table
         df2= pd.DataFrame([[stock_id,'','','','','','','']],columns=['STOCK_SYMBOL','times','earn_loss','Success_rate','Profit_Factor','revenue','avg_return','total_return'])
         dfSMA = dfSMA.append(df2, ignore_index=True)
         dfBBand = dfBBand.append(df2, ignore_index=True) 
        
      #將每一股股票進行回測
      for index in range(len(dfBBand.index)):
      	print(dfBBand.get_value(index, 'STOCK_SYMBOL'))
      	result =BBands_main(dfBBand.get_value(index, 'STOCK_SYMBOL'), dfSMA, dfBBand, index)
      response = result
      
    else:
       dfSMA = pd.DataFrame(columns=['STOCK_SYMBOL','times','earn_loss','Success_rate','Profit_Factor','revenue','avg_return','total_return'],index=[0])
       dfBBand = pd.DataFrame(columns=['STOCK_SYMBOL','times','earn_loss','Success_rate','Profit_Factor','revenue','avg_return','total_return'],index=[0])
       dfSMA['STOCK_SYMBOL'] = stock_id
       dfBBand['STOCK_SYMBOL'] = stock_id
       response = BBands_main(stock_id, dfSMA, dfBBand, 0)
       
    return HttpResponse(response)



#def boolin_py(request):
#	stock_id = request.GET['stock_id']
#    dfSMA ={}
#	dfBBand = {}
#    #stock_list= ['2408','2330','3008','2421','2344','2454','5483','2317']
#    #stock_list= ['2408','6165','2421','2313','2337','6153','3661','2330','2454','2340','1455','1409','6456','2317','2481','2515']
#	if(stock_id == "all"):
#		stock_list= ['2408','6165','2421','2313','2337','6153','3661','2330','2454','2340','1455','1409','6456','2317','2481','2515']
#         #set DataFrame
#		for index, stock_id in enumerate(stock_list):
#			#if(len(table) == 10 and table[:6] == 'stock_' and table[-4:].isdigit()):
#		 	 df2= pd.DataFrame([[stock_id,'','','','']],columns=['STOCK_SYMBOL','count','roi','winrate','winfactor'])
#			 dfSMA = dfSMA.append(df2, ignore_index=True)
#			 dfBBand = dfBBand.append(df2, ignore_index=True)
#		#將每一股股票進行回測
#		for index in range(len(dfBBand.index)):
#			print(dfBBand.get_value(index, 'STOCK_SYMBOL'))
#			result = boolin_main(dfBBand.get_value(index, 'STOCK_SYMBOL'), dfSMA, dfBBand, index)
#		response = result
#	 else:
#		dfSMA = pd.DataFrame(columns=['STOCK_SYMBOL','count','roi','winrate','winfactor'],index=[0])
#		dfBBand = pd.DataFrame(columns=['STOCK_SYMBOL','count','roi','winrate','winfactor'],index=[0])
#		dfSMA['STOCK_SYMBOL'] = stock_id 
#		dfBBand['STOCK_SYMBOL'] = stock_id
#		response = boolin_main(stock_id, dfSMA, dfBBand, 0)
#	return HttpResponse(response.to_html(classes='', col_space=100,header = False))



#def crawl_price(request):
#    data_list = os.listdir(r"./app/merged_data/merged_data_5min")
#    df_data_dict={}
#    stock_list =  ['2408']
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
#    y=pd.Series(df_data_dict[s_id]['close'])
#    print(y)
#    x=np.array(df.index)
#    fig=plt.figure(figsize=(15,5))
#    ax=fig.add_subplot(111)
#    ax.set_xlim(0,len(y))
#    ax.plot(y,color='#000000',label="股價",lw=3)
#    ax.set_xticks(range(0,len(y),60))
#    png = s_id+".png"
#    #fig.save(png)
#    print (fig)
#    response=FigureCanvasAgg(fig)
#    #canvas.print_png(response)
#    return HttpResponse( response.print_png(response),content_type='image/png')
    #canvas=FigureCanvasAgg(fig)
    #df_data_dict[s_id]['close'].plot(figsize=(10,6))
    #plt.xlabel('股價')
    #plt.legend(loc='upper left')
    #print(fig)
    #response =fig
    #plt.close(fig)
    #f.writelines(response.text)
    #response.to_html = requests.get(url)
#def stockpool(request):
#    response = stockpool(request, *callback_args, **callback_kwargs) 
#    responses = xhr.responseText;
#    return JsonResponse(response)

