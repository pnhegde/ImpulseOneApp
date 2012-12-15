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

conn = httplib.HTTPConnection("rtbidder.impulse01.com",9000)
conn.request("GET", "/poll")
response = conn.getresponse()
data = json.loads(response.read())
FileList=data["FileList"]
MessageList=[]
for f in FileList:
  conn.request("GET", "/getFile?file="+f)
  response = conn.getresponse()
  data = json.loads(response.read())
  Messages=data["Messages"]
  MessageList.extend(Messages)

queryList=[]
for item in MessageList:
  if item["Message"]=="IMP":
    t=item["timestamp_GMT"].split(" ")
    date=t[0]
    time=t[1]
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
      price = k/1000;

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
      price = k/1000;
      
    if item["exchange"]=="direct":
      price = item["price"]
      
    query = "INSERT INTO `logs`.`logsnew` (`impressionId`, `campaignId`, `bannerId`, `exchange`, `domain`, `carrier`, `device`, `userAgent`, `state`, `city`, `country`, `bid`, `price`, `date`, `time`, `impressionCount`) VALUES ('"+item["impressionId"]+"', '"+item["campaignId"]+"', '"+item["bannerId"]+"', '"+item["exchange"]+"', '"+item["domain"]+"', '"+item["isp"]+"', '', '', '"+item["state"]+"', '"+item["city"]+"', '"+item["country"]+"', '"+bid+"', '"+price+"', '"+date+"', '"+time+"', '"+item["impressionCount"]+"') ON DUPLICATE KEY UPDATE campaignId='"+item["campaignId"]+"', bannerId='"+item["bannerId"]+"', exchange='"+item["exchange"]+"', domain='"+item["domain"]+"', carrier='"+item["isp"]+"', device='', userAgent='ua', state='"+item["state"]+"', city='"+item["city"]+"', country='"+item["country"]+"', bid='"+item["bid"]+"', price='"+price+"', date='"+date+"', time='"+time+"', impressionCount='"+item["impressionCount"]+"';"
    
  if item["Message"]=="CLK":
    query = "INSERT INTO `logs`.`logsnew` (`impressionId`,`clicked`) VALUES ('"+item["impressionId"]+"', '1') ON DUPLICATE KEY UPDATE clicked=1"
  queryList.append(query)
  
con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'impulsedb',compress=1,cursorclass=MySQLdb.cursors.DictCursor);
cur = con.cursor()
for query in queryList:
  cur.execute(query)
  print query