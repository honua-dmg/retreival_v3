# Final code:
import multiprocessing.process
import authToken
import wbsoc
import time
import redis
import Save
import json
import os
import dotenv
import avgParser
import multiprocessing
import threading
import logging
import pandas as pd
import datetime as dt
"""
    Considerations for speed:
    1. in avgParser, to get the signals, we read from the csv files we make to get past data- if this proves to be too slow for you
        put the data in redis and extract it from there. 
"""

def producer(testing,access_token=None,client=0):
    if access_token==None:
        token = authToken.AutoLogin(client).get_access_token()
    else:
        token = access_token
    r = redis.Redis(host="localhost",port="6379",db=0)
    symb = wbsoc.Symbol(r,token,testing)
    depth = wbsoc.Depth(r,token,testing)
    symb.connect()
    depth.connect()
    symb.subscribe()
    depth.subscribe()
    time.sleep(60*60*6)
    print("its time to end!")
    depth.unsubscribe()
    symb.unsubscribe()
    symb.close()
    depth.close()



# saves all incoming data to csv files :)
def csvWorker(directory,testing): # should be on a separate process
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    worker = Save.csv(directory,testing)
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}
    worker.initialise()
    while r.get('end')!='true':
        messages = r.xread(stonks,block=100)
        if messages == []:
            continue
        print(messages)
        for stream in messages:
            for uncoded_msg in stream[1]:
                try:
                    worker.save_msg({key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()})
                except Exception as e:
                    print(e)
                    logging.log(msg=f"{e} this went wrong :)")
                    return



def avgParserWorker(directory,testing):
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)
    t = redis.Redis(host="localhost",port="6379",db=1)

    
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}
    while r.get('end')!='true': # continuosly reading the incoming stream of data.
        data = r.xread(stonks,block=100)
        for stream in data:
            for uncoded_msg in stream[1]:
                msg = {key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()} # decoding message
                parsed_msg = avgParser.parseMsg(t,msg)

                # saving the file to csv
                avgParser.to_csv(parsed_msg,directory)

    t.flushdb()

def SignalWorker():
    polarisers={}
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)

    avg_r = redis.Redis(host="localhost",port="6379",db=2)
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}
    while r.get('end')!='true': # continuosly reading the incoming stream of data.
        data = r.xread(stonks,block=100)
        for stream in data:
            for uncoded_msg in stream[1]:
                msg = {key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()} # decoding message
                parsed_msg = avgParser.parseMsg(avg_r,msg)
                #looking for signals
                try:
                    SignalWorker(parsed_msg,avg_r,polarisers[parsed_msg['stonk']])
                except KeyError: # incase we haven't made it yet
                    logging.log(f'new stonk polariser dict for {parsed_msg['stonk']} made')
                    polarisers[parsed_msg['stonk']] = {}
                    SignalWorker(parsed_msg,avg_r,polarisers[parsed_msg['stonk']])

def SignalFinder(msg,avg_r,polariser):
    # independant variables
    slice_len =400
    decision_range = 15
    error_range =.3

    total_buy_qty = 0


    slice =sorted([int(float(x[1][b'ltp'])) for x in avg_r.revrange(msg['stonk'].split('-')[0],slice_len)]) # pretty irritating code, deal with it :)
    maxes = slice[-3:]
    mins = slice[:3]
    
    current_ltp  = msg['ltp']


    if current_ltp in polariser.keys():

        polariser[current_ltp] +=msg['amt-buy']
    else:
        polariser[current_ltp] = msg['amt-buy']

    total_buy_qty+=msg['amt-buy']
    count = len([x for x in polariser.keys() if polariser[x]>0])
    avg_qty = total_buy_qty/count if count>0 else 0# average updates each time we get a new update. 
    keys = sorted(polariser.keys())
    traded_time = msg['last_traded_time']
    below, above  = keys[:keys.index(current_ltp)],keys[keys.index(current_ltp):]

    india_date=dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    with open(f'./messages/{msg['stonk'].split('-')[0]}-{india_date}','a') as f:
        print(f"\n\n\n{current_ltp} ::{traded_time}:: {len(keys)}, len below, above: {len(below)}, {len(above)}, {total_buy_qty}, {count}, {avg_qty} \below:{[polariser[x] for x in below]}, above:{[polariser[x] for x in above]}",file=f)  
        # potential short signal : len(above)<20 and most of them are 0s and below>20 and most of them are reds
        if len(below)>decision_range:
            print('\tchecking for a short',file=f)
            greens,reds = 0,0
            for key in below[:-decision_range:-1]:
                if polariser[key]<=avg_qty:
                    reds+=1
                else:
                    greens+=1
            print(f'\treds:{reds},greens:{greens}',file=f)

            if reds>=decision_range*(1-error_range) and greens/(reds+greens)<=error_range and current_ltp in maxes:
                print(f'SHORT SIGNAL:\n\ttraded_time:{traded_time}: 20+gap found at ltp: {current_ltp},reds:{reds},greens:{greens}\n',file=f)

                avg_r.xadd(msg['stonk'].split('-')[0]+'-short',{traded_time:current_ltp})
        # potential buy signal : short but inverted
        if len(above)>decision_range:
            print('\tchecking for a buy',file=f)
            greens,reds = 0,0
            for key in above[:decision_range]:
                if polariser[key]<=avg_qty:
                    reds+=1
                else:
                    greens+=1
            print(f'\treds:{reds},greens:{greens}',file=f)
            if reds>=decision_range*(1-error_range) and greens/(reds+greens)<=error_range and current_ltp in mins:
                print(f'BUY SIGNAL:\n\ttraded_time:{traded_time} 20+gap found at ltp: {current_ltp},reds:{reds},greens:{greens}\n',file=f)
                avg_r.xadd(msg['stonk'].split('-')[0]+'-long',{traded_time:current_ltp})

    
    
    avg_r.xadd(msg['stonk'].split('-')[0],msg)
    

    
def endDay(testing): # clears cache. 
    dotenv.load_dotenv()
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    r = redis.Redis(host="localhost",port="6379",db=0)
    r.flushall()



if __name__ =="__main__":
    testing = False
    producerProcess =multiprocessing.Process(producer,args=(testing))

    csvWorkerProcess = multiprocessing.Process(csvWorker,args=('./data',testing))
    avgParserWorkerProcess = multiprocessing.Process(avgParserWorker,args=('./averages',testing))
    
    producerProcess.start()
    csvWorkerProcess.start()
    avgParserWorkerProcess.start()


    producerProcess.join()
    csvWorkerProcess.join()
    avgParserWorkerProcess.join()

    endDay(testing)



def threadripper(token=None,testing=True):
    if token==None:
        producerProcess =threading.Thread(target=producer,args=(testing))
    else:
        producerProcess =threading.Thread(target=producer,args=(testing,token))
    csvWorkerProcess = threading.Thread(target=csvWorker,args=('./data',testing))
    avgParserWorkerProcess = threading.Thread(target=avgParserWorker,args=('./averages',testing))
    
    producerProcess.start()
    csvWorkerProcess.start()
    avgParserWorkerProcess.start()


    producerProcess.join()
    csvWorkerProcess.join()
    avgParserWorkerProcess.join()

    endDay(testing)

def processripper(token=None,testing=False):
    if token==None:
        producerProcess =multiprocessing.Process(target=producer,args=(testing))
    else:
        producerProcess =multiprocessing.Process(target=producer,args=(testing,token))
    csvWorkerProcess = multiprocessing.Process(target=csvWorker,args=('./data',testing))
    avgParserWorkerProcess = multiprocessing.Process(target=avgParserWorker,args=('./averages',testing))
    
    producerProcess.start()
    csvWorkerProcess.start()
    avgParserWorkerProcess.start()


    producerProcess.join()
    csvWorkerProcess.join()
    avgParserWorkerProcess.join()

    endDay(testing)