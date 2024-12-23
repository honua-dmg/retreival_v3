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


def producer(testing,access_token=None):
    if access_token==None:
        token = authToken.AutoLogin('test').get_access_token()
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
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.reads(os.readenv("STOCKS"))["REAL"]
    worker = Save.csv(directory,testing)
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}
    worker.initialise()
    while r.get('end')!='true':
        messages = r.xread(stonks,block=0)
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
    while r.get('end')!='true':
        data = r.xread(stonks,block=0)
        for stream in data:
            for uncoded_msg in stream[1]:
                msg = {key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()} # decoding message
                parsed_msg = avgParser.parseMsg(t,msg)

                avgParser.to_csv(parsed_msg,directory)

    t.flushdb()

def endDay(testing): # clears cache. 
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    r = redis.Redis(host="localhost",port="6379",db=0)
    for stonk in stonksList:
        r.xtrim(stonk)



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