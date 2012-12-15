#1. Bidders and AdServers send spend updates directly to this server as UDP datagrams on Port 5005
#4. Bidders retrieve the pacing at which they should run different campaigns from this server

import MySQLdb
import MySQLdb.cursors
import socket
import threading
import thread
import MySQLdb
import json
from collections import defaultdict

pacingIndex = dict()

#This thread handles all the UDP messages coming on port 5006 and handles them
def udpServer():
    print "starting udp server. Listening to spends being made by bidders and ad-servers"
    UDP_IP = ""
    UDP_PORT = 5005
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM) # UDP
    sock.bind((UDP_IP, UDP_PORT))
    while True:
	data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
	data=json.loads(data)
	if data['messageType']=="Spend":
	  cid = data['message']["campaignId"]
	  spend = data['message']["spend"]

#This thread handles all the TCP requests coming on port 5004 and handles them	  
def handler(clientsock,addr):
   data = clientsock.recv(BUFSIZ)
   clientsock.send("Hey Ya\r\n")
   clientsock.close()

if __name__=='__main__':
    cur.execute("SELECT * FROM campaigns")
    rows = cur.fetchall()
    for row in rows:
      campaignId=row['campaignId']
      todaySpend=row['todaySpend']
      pacingIndex['c'+str(campaignId)]['todaySpend']=float(todaySpend)
      pacingIndex['c'+str(campaignId)]['dailyBudget']=float(dailyBudget)      
      pacingIndex['c'+str(campaignId)]['paceType']=int(paceType)
    thread.start_new_thread(udpServer,())
    HOST = ''
    PORT = 5004
    BUFSIZ = 1024
    ADDR = (HOST, PORT)
    serversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversock.bind(ADDR)
    serversock.listen(2)
    while 1:
        clientsock, addr = serversock.accept()
        thread.start_new_thread(handler, (clientsock, addr))
