import xml.etree.ElementTree as ET

from heapq import *
from time import *
from datetime import datetime
import threading
from tabulate import tabulate

tree = ET.parse('orders.xml')
root = tree.getroot()
order_len = len(root)

buy = []
sell = []
dele = []

def printTable():
    for i in range(3):
        print("book :  Book-{}".format(i+1))
        head = ["BUY", "SELL"]
        mydata = []
        buyCount = 0
        m = len(buy[i])
        for j in range(m):
            [a, c, v] = heappop(buy[i])
            if (str(c) in dele[i]):
                continue
            else:
                p = ("{} @ {}".format(v, (-1) * a))
                mydata.append([p, ""])
                buyCount += 1

        sellCount = 0
        n = len(sell[i])
        for j in range(n):
            [a, c, v] = heappop(sell[i])
            if (str(c) in dele[i]):
                continue
            else:
                p = ("{} @ {}".format(v, a))
                if(sellCount < buyCount):
                    mydata[sellCount][1] = p
                    sellCount += 1
                else:
                    mydata.append(["", p])

        print(tabulate(mydata, headers=head, tablefmt="grid"))
    print("Done")
    
def AddOrder(boo, op, price, vol, id):
    global buy, sell, buy2, sell2, dele
    n = int(boo[5:]) - 1
    while(len(buy) < n+1):
        p = []
        q = []
        d = set()
        heapify(p)
        heapify(q)
        buy.append(p)
        sell.append(q)
        dele.append(d)

    if(op == "SELL"):
        m = len(buy[n])
        if(m == 0):
            heappush(sell[n], [price, id, vol])
            return

        [a, c, v] = heappop(buy[n])
        while(str(c) in dele[n]):
            [a, c, v] = heappop(buy[n])

        if(price > (-1)*a):
            heappush(buy[n], [a, c, v])
            heappush(sell[n], [price, id, vol])

        else:
            if(v == vol):
                return
            elif(v > vol):
                heappush(buy[n], [a, c, v-vol])
            else:
                AddOrder(boo, op, price, vol - v, id)

    else:
        m = len(sell[n])
        if(m == 0):
            heappush(buy[n], [(-1)*price, id, vol])
            return

        [a, c, v] = heappop(sell[n])
        while (str(c) in dele[n]):
            [a, c, v] = heappop(sell[n])

        if(price < a):
            heappush(sell[n], [a, c, v])
            heappush(buy[n], [(-1)*price, id, vol])

        else:
            if(v == vol):
                return
            elif(v > vol):
                heappush(sell[n], [a, c, v-vol])
            else:
                AddOrder(boo, op, price, vol - v, id)

def DeleteOrder(b, id):
    global dele
    n = int(b[5:]) - 1
    dele[n].add(id)


print("Processing started at: {}".format(datetime.today()))
st = time()
def fetch(threadNo):
    for i in range(order_len):
        a = root[i].attrib
        alen = len(a)
        y = int(a['book'][5:])
        if(y != threadNo):
            continue
        elif(alen == 2):
            DeleteOrder(a['book'], a['orderId'])
        else:
            AddOrder(a['book'], a['operation'], float(a['price']), int(a['volume']), int(a['orderId']))


threads = []
startTime = time()
noOfThreads = 3

for i in range(noOfThreads):  # for connection 1
    x = threading.Thread(target=fetch, args=(i+1,))
    threads.append(x)
    x.start()

for i in range(noOfThreads):
    threads[i].join()

printTable()
print("Processing completed at: {}".format(datetime.today()))
et = time()
print("Processing Duration: {} seconds".format(et - st))
