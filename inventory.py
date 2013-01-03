#1. Bidder sends inventory processed as UDP datagram messages on port 5006 which are captured here and updated in database

import socket
import threading
import thread
import MySQLdb
import json
from collections import defaultdict
import sys

#This thread handles all the UDP messages coming on port 5006 and handles them
print "starting udp server on port 5006. Listening to inventory forecasts"
UDP_IP = ""
UDP_PORT = 5006
sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT))
while True:
    data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
    data=json.loads(data)
    if data['messageType']=="Forecast":
      e = data['message']["e"]
      d = data['message']["d"]
      c = data['message']["c"]
      geo = data['message']["geo"]
      size = data['message']["size"]
      i = data['message']["i"]
      try:
	  con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'impulsedb');
	  cur = con.cursor()
	  cur.execute("SELECT * FROM inventory WHERE domain='"+d+"' AND channel='"+c+"' AND exchange='"+e+"' AND country='"+geo+"' AND size='"+size+"'")
	  if int(cur.rowcount)==0:
	    cur.execute("INSERT INTO `impulsedb`.`inventory` (`sourceId`, `domain`, `channel`, `exchange`, `category`, `country`, `size`, `averageDailyImpressions`, `todayImpressions`,`averageCpm`, `manuallyVerified`) 		  VALUES (NULL, '"+d+"', '"+c+"', '"+e+"', '1', '"+geo+"', '"+size+"', '', '"+str(i)+"', '', 'N');")
	  else:
	    row = cur.fetchone()
	    recordId=row[0]
	    cur.execute("UPDATE inventory SET todayImpressions=todayImpressions+"+str(i)+" WHERE sourceId="+str(recordId)+"")
      except MySQLdb.Error, e:
	  print "Error %d: %s" % (e.args[0],e.args[1])
	  sys.exit(1)