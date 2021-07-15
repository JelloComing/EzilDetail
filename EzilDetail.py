import os
import queue
import threading

import requests
from requests.adapters import HTTPAdapter
import time
import json
from datetime import datetime,timezone,timedelta,date

import pandas as pd


exitFlag = 0

class myThread (threading.Thread):
    def __init__(self, threadID, name, Queue , Deal):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.Queue = Queue
        self.Deal = Deal

    def run(self):
        #print ("开启线程：" + self.name)
        process_data(self.name, self.Queue ,self.Deal)
        #print ("退出线程：" + self.name)

def process_data(threadName, Queue ,Deal):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            data = Queue.get()
            queueLock.release()



            '''
            balances = '/balances/' + Walet
            payouts = '/payouts/' + Walet + '?page={page}&per_page={per_page}&from={from}&to={to}'
            zil_rewards = '/zil_rewards/' + Walet + '/{ds_epoch}'
            '''
            rewards = '/rewards/' + Walet + '?page='+str(data)+'&per_page=20&coin='

            #eth
            coinType ='eth'
            datas = webDeal(rewards,coinType)
            eth_datas.append(datas)

            #eth
            coinType ='etc'
            datas = webDeal(rewards,coinType)
            etc_datas.append(datas)

            #zil
            coinType ='zil'
            datas = webDeal(rewards,coinType)
            zil_datas.append(datas)
        else:
            queueLock.release()

def webDeal(rewards,coinType):
    load_url = base_url + rewards +coinType
    # print ("Thread-%s processing %s  ： "% (threadName, data) +load_url  )
    s = requests.session()
    # max_retries=3 重试3次
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))
    req = s.request("GET", url=load_url, timeout=15)

    while "Too Many Requests" in req.text:
        time.sleep(1)
        s = requests.session()
        # max_retries=3 重试3次
        s.mount('http://', HTTPAdapter(max_retries=3))
        s.mount('https://', HTTPAdapter(max_retries=3))
        req = s.request("GET", url=load_url, timeout=15)

    #datas = req.text  # json.loads(req.text)
    return req.text

def deal_data(coinType,deal_datas):
    for deal_data in deal_datas:
        datas=json.loads(deal_data)
        for data in datas:
            utc_time = datetime.strptime(data['created_at'], "%Y-%m-%dT%H:%M:%SZ")
            china_time = utc_time.astimezone(timezone(timedelta(hours=16)))
            #china_time_stru = time.strptime(china_time, '%Y-%m-%d %H:%M:%S')

            amout = '{:.18f}'.format(data['amount'])

            date = utc_time.astimezone(timezone(timedelta(hours=16))).strftime('%Y-%m-%d')
            time_hour = china_time.hour
            china_server_time = china_time.strftime('%Y-%m-%d %H:%M:%S')

            data_dict = dict()
            data_dict['币种'] = str(coinType)
            data_dict['日期'] = str(date)
            data_dict['时段'] = int(time_hour)
            #data_dict['china_server_time'] = str(china_server_time)
            data_dict['数量'] = float(amout)
            data_lists.append(data_dict)


if __name__ == '__main__':

    print('============================================================')
    print('V1.1 program by Jello 推荐码9020')
    print('============================================================')

    #钱包地址
    if not os.path.exists(os.getcwd() + '/EzilWalet.txt'):
        Walet = input("【输入【ETH.ZIL】/【ETC.ZIL】格式地址】：\n")
    else:
        with open(os.getcwd() + '/EzilWalet.txt', "r", encoding='utf-8') as f:
            num = 1
            Walets = {}
            for line in f.readlines():
                line = line.strip('\n')  # 去掉列表中每一个元素的换行符
                if line != '':
                    Walets[num] = line
                    num = num + 1
            if len(Walets) == 0:
                Walet = input("【输入【ETH.ZIL】/【ETC.ZIL】格式地址】：\n")
            else:
                print('【选择序号，或者【0】手工输入地址】')
                for api_num in Walets:
                    print('【' + str(api_num) + '】' + Walets[api_num].strip('\ufeff'))
                xh = int(input('===========请输入====================\n'))
                if xh == 0:
                    Walet = input("【输入【ETH.ZIL】格式地址】：\n")
                else:
                    Walet = Walets[xh].strip('\ufeff')
    print('\n')

    start = time.time()
    print("【输入要生成日期的类型（1或2）】：\n"+"（1 代表3天以内  2 代表一周以内 / 其他参数无效）")
    dateType = str(input('===========请输入====================\n'))
    print('\n')
    # 定义线程数(每天基数)
    threadNum = 10
    # 定义页面请求数(每天基数)
    pageNum = 15
    if dateType == '1' or dateType =='2':
        if dateType == '1' :
            day = 3
        if dateType == '2' :
            day =7
    else:
        print('日期格式参数无效！程序退出...')
        time.sleep(10)
        os._exit()

    today = date.today()
    threadNum = threadNum * 2 #* day
    pageNum = pageNum * day
    day_ago = today - timedelta(days=day)
    day_ago_str = day_ago.strftime('%Y-%m-%d')



    # API URL
    base_url = 'https://billing.ezil.me'

    #数据合集
    eth_datas = []
    etc_datas = []
    zil_datas = []


    print("请求时间【"+day_ago_str+"】之后服务器数据.....请等待....")

    #线程队列
    threadList = range(1,threadNum+1)
    #页面队列
    nameList = range(1,pageNum+1)
    #定义队列大小
    workQueue = queue.Queue(pageNum)

    queueLock = threading.Lock()
    threads = []
    threadID = 1

    # 创建新线程
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue,'')
        thread.start()
        threads.append(thread)
        threadID += 1

    # 填充队列
    queueLock.acquire()
    for word in nameList:
        workQueue.put(word)
    queueLock.release()

    # 等待队列清空
    while not workQueue.empty():
        pass

    # 通知线程是时候退出
    exitFlag = 1

    # 等待所有线程完成
    for t in threads:
        t.join()

    data_lists = list()
    deal_data('ETH', eth_datas)
    deal_data('ETC', etc_datas)
    deal_data('ZIL', zil_datas)



    print ("结束请求,生成excel中....")

    pd.set_option('display.max_rows',1000)
    df = pd.DataFrame(data_lists)#.drop(columns=['china_server_time'])
    df_res = df[(df.日期>=day_ago_str)].groupby(['币种','日期','时段']).sum().unstack(['时段'])
    #df['合计'] = df.apply(lambda 时段: 时段.sum(), axis=1)
    #print(df_res)


    if dateType == '1' or dateType =='2':
        if dateType == '1' :
            end_str = '_'+today.strftime('%Y%m%d')+'_3D'
        if dateType == '2' :
            end_str = '_'+today.strftime('%Y%m%d')+'_7D'
    df_res.to_excel(Walet+end_str+".xls")

    end = time.time()
    print("完成时间: %f s" % (end - start))

    time.sleep(10)

