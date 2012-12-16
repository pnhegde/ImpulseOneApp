import MySQLdb
import MySQLdb.cursors
import socket
import json
import httplib, urllib
import random
import csv
import base64
import hmac,hashlib
import binascii
import time

def pollAdServer():
  try:
    conn = httplib.HTTPConnection("rtbidder.impulse01.com",9000)
    conn.request("GET", "/poll")
    response = conn.getresponse()
    data = json.loads(response.read())
    FileList=data["FileList"]    
  except:
    FileList=[]
  MessageList=[]
  for f in FileList:
    try:
      conn.request("GET", "/getFile?file="+f)
      response = conn.getresponse()
      data = json.loads(response.read())
      Messages=data["Messages"]
      MessageList.extend(Messages)
    except:
      print "exception in filelist loop"

  queryList=[]
  for item in MessageList:
    item=json.loads(item)
    if item["message"]=="IMP":
      t=item["timestamp_GMT"].split(" ")
      date=t[0]
      time=t[1]
      if item["city"]==None:
	item["city"]="Undetected"
      if item["isp"]==None:
	item["isp"]="Undetected"
      item["isp"]=item["isp"].strip("'")
      
      try:
	if item["exchange"]=="google":
	  price = item["price"]
	  price = price.replace("-","+").replace("_","/")
	  price = price + '=' * (4 - len(price) % 4)
	  dprice=base64.b64decode(price)
	  initvec=dprice[0:16]
	  ciphertext=dprice[16:24]
	  integritysig=dprice[24:28]
	  ekey=base64.b64decode("SwkocWk+H59O8rf3uVAUMXLUfGn6rWiPX/Ua1pXMh/8=")
	  ikey=base64.b64decode("sH7xBkxKKqtQ3lVTpPT/Z8sBqUJAymjCkMA3JGa9lfU=")
	  pad = hmac.new(ekey, initvec, hashlib.sha1).digest()
	  l = [bin(ord(a) ^ ord(b)) for a,b in zip(ciphertext,pad)]
	  k = int("".join("%02x" % int(x,0) for x in l), 16)
	  price = float(k)/float(1000);

	if item["exchange"]=="openx":
	  price = item["price"]
	  price = price.replace("-","+").replace("_","/")
	  price = price + '=' * (4 - len(price) % 4)
	  dprice=base64.b64decode(price)
	  initvec=dprice[0:16]
	  ciphertext=dprice[16:24]
	  integritysig=dprice[24:28]
	  ekey=binascii.unhexlify("D71E79EB3E18B519412953A7F300478765F455538495A26D6E5EFD7831FBFC08")
	  ikey=binascii.unhexlify("1976F4578EA971D0D3C577C7C6BE4E2BB2A47E373921A5A87FA221B19E1DBAB7")
	  pad = hmac.new(ekey, initvec, hashlib.sha1).digest()
	  l = [bin(ord(a) ^ ord(b)) for a,b in zip(ciphertext,pad)]
	  k = int("".join("%02x" % int(x,0) for x in l), 16)
	  price = float(k)/float(1000);
      except:
	price=float(0)
	print "exception decoding price"
	
      if item["exchange"]=="direct":
	price = item["price"]
	
      try:
	query = "INSERT INTO `logs`.`logsnew` (`impressionId`, `campaignId`, `bannerId`, `exchange`, `domain`, `carrier`, `device`, `userAgent`, `state`, `city`, `country`, `bid`, `price`, `date`, `time`, `impressionCount`) VALUES ('"+item["impressionId"]+"', '"+str(item["campaignId"])+"', '"+str(item["bannerId"])+"', '"+item["exchange"]+"', '"+item["domain"]+"', '"+item["isp"]+"', NULL, NULL, '"+item["state"]+"', '"+item["city"]+"', '"+item["country"]+"', '"+str(item["bid"])+"', '"+str(price)+"', '"+date+"', '"+time+"', '"+str(item["impressionCount"])+"') ON DUPLICATE KEY UPDATE campaignId='"+str(item["campaignId"])+"', bannerId='"+str(item["bannerId"])+"', exchange='"+item["exchange"]+"', domain='"+item["domain"]+"', carrier='"+item["isp"]+"', device=NULL, userAgent=NULL, state='"+item["state"]+"', city='"+item["city"]+"', country='"+item["country"]+"', bid='"+str(item["bid"])+"', price='"+str(price)+"', date='"+date+"', time='"+time+"', impressionCount='"+str(item["impressionCount"])+"';"
	queryList.append(query)
      except:
	print "Exception here"
	print item

    try:
      if item["message"]=="CLICK":
	query = "INSERT INTO `logs`.`logsnew` (`impressionId`,`clicked`) VALUES ('"+item["impressionId"]+"', '1') ON DUPLICATE KEY UPDATE clicked=1"
	queryList.append(query)

      if item["message"]=="CLICKCONV":
	query = "INSERT INTO `logs`.`logsnew` (`impressionId`,`clickConversion`) VALUES ('"+item["impressionId"]+"', '1') ON DUPLICATE KEY UPDATE clickConversion=1"
	queryList.append(query)

      if item["message"]=="VIEWCONV":
	query = "INSERT INTO `logs`.`logsnew` (`impressionId`,`viewConversion`) VALUES ('"+item["impressionId"]+"', '1') ON DUPLICATE KEY UPDATE viewConversion=1"
	queryList.append(query)

      if item["message"]=="GOOGLEMATCH":
	query = "INSERT INTO `audience`.`matchtable` (`impulseId`,`google_gid`) VALUES ('"+item["imp_uid"]+"', '"+item["google_gid"]+"') ON DUPLICATE KEY UPDATE google_gid='"+item["google_gid"]+"'"
	queryList.append(query)
    except:
      print "EXCEPTION"
      print item
    
  con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'impulsedb',compress=1,cursorclass=MySQLdb.cursors.DictCursor);
  cur = con.cursor()
  for query in queryList:
    try:
      cur.execute(query)
    except:
      print query
  con.commit()
  return len(queryList)  
  
if __name__ == "__main__":
  print "starting poll loop"
  while(1):
    n = pollAdServer()
    print "fetched records"+str(n)
    time.sleep(1)