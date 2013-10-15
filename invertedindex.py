#1. Bidders Retrieve InvertedIndex from this server on Port 5003
#2. InvertedIndex is the campaign database which the bidder uses to qualify campaigns to impressions

import MySQLdb
import MySQLdb.cursors
import json
from collections import defaultdict
import tornado.ioloop
import tornado.web
import time

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    if self.request.path == "/index":
      start = time.time()
      invertedIndex = dict()
      con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'impulsedb',compress=1,cursorclass=MySQLdb.cursors.DictCursor);
      cur = con.cursor()
      channel= self.get_argument('channel', True)
      cur.execute("SELECT * FROM campaigns WHERE status = '2' AND channel IN("+channel+") AND dailyBudget>spendToday AND totalBudget>spendTotal AND startDate<=CURRENT_DATE() AND endDate>=CURRENT_DATE() AND creatives<>'' AND countryTargets<>''")
      rows = cur.fetchall()
      for row in rows:
	#This block is for display and video campaigns
	if row['channel']==1 or row['channel']==2:
	  campaignId=int(row['campaignId'])
	  geo = row['countryTargets'].lower()
	  
	  if 'display:geo:'+geo in invertedIndex.keys():
	    k=set(invertedIndex['display:geo:'+geo])
	    k.add(campaignId)
	    z=list(k)
	    invertedIndex['display:geo:'+str(geo)]=z
	  else:
	    invertedIndex['display:geo:'+geo]= [campaignId]
	  
	  creatives = row['creatives'].split(',')
	  for creative in creatives:
	    if creative != '':
		cur.execute("SELECT * FROM creatives WHERE creativeId='"+creative+"'")
		creative=int(creative)
		row1=cur.fetchone()
		width=str(row1['width'])
		height=str(row1['height'])
		size=width+"x"+height
		if 'display:size:'+size in invertedIndex.keys():
		  k=set(invertedIndex['display:size:'+size])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['display:size:'+size]=z
		else:
		  invertedIndex['display:size:'+size]= [campaignId]

		if int(row['channel'])==2:
		  creativeIndex='video:campaign:'+str(campaignId)+':creatives'
		  
		creativeIndex='display:campaign:'+str(campaignId)+':'+str(width)+"x"+str(height)
		if creativeIndex in invertedIndex.keys():
		  k=set(invertedIndex[creativeIndex])
		  k.add(creative)
		  z=list(k)
		  invertedIndex[creativeIndex] = z
		else:
		  invertedIndex[creativeIndex]= [creative] 
		  
	  audiences = row['audienceTargets'].split(',')
	  for audience in audiences:
	    if audience != '':
		audience=int(audience)
		if 'display:audience:'+str(audience) in invertedIndex.keys():
		  k=set(invertedIndex['display:audience:'+str(audience)])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['display:audience:'+str(audience)] = z
		else:
		  invertedIndex['display:audience:'+str(audience)]= [campaignId]

	  if int(row['strategy'])==5 or int(row['strategy'])==9:
	    if 'display:roe' in invertedIndex.keys():
	      k=set(invertedIndex['display:roe'])
	      k.add(campaignId)
	      z=list(k)
	      invertedIndex['display:roe'] = z
	    else:
	      invertedIndex['display:roe'] = [campaignId]
	    blacklist=row['list']
	    blacklist=blacklist.split("\n")
	    newlist=[]
	    for b in blacklist:
	      a=b.split(",")
	      newlist.extend(a)
	    for n in newlist:
	      n=n.strip()	
	      if n!="":
		if 'display:roe:black:'+n in invertedIndex.keys():
		  k=set(invertedIndex['display:roe:black:'+n])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['display:roe:black:'+n] = z
		else:
		  invertedIndex['display:roe:black:'+n] = [campaignId]	  

	  if int(row['strategy'])==4 or int(row['strategy'])==7:
	    whitelist=row['list']
	    whitelist=whitelist.split("\n")
	    newlist=[]
	    for b in whitelist:
	      a=b.split(",")
	      newlist.extend(a)
	    for n in newlist:
	      n=n.strip()
	      if n!="":
		if 'display:white:'+n in invertedIndex.keys():
		  k=set(invertedIndex['display:white:'+n])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['display:white:'+n] = z
		else:
		  invertedIndex['display:white:'+n] = [campaignId]      

	  invertedIndex['display:campaign:'+str(campaignId)+':bid']=float(row['bidCpm'])
	  invertedIndex['display:campaign:'+str(campaignId)+':pacing']=int(row['pacing'])
	  invertedIndex['display:campaign:'+str(campaignId)+':url']=str(row['destinationUrl'])

	  cur.execute("SELECT * FROM accounts WHERE accountId IN (SELECT accountId FROM advertisers WHERE advertiserId IN (SELECT advertiserId FROM brands WHERE brandId IN (SELECT brandId FROM plans WHERE planId IN (SELECT planId FROM campaigns WHERE campaignId='"+str(campaignId)+"'))))")
	  res = cur.fetchall()
	  res1 = res[0]
	  invertedIndex['display:campaign:'+str(campaignId)+':advertiserName']=res1['accountName']
	  invertedIndex['display:campaign:'+str(campaignId)+':advertiserId']=res1['accountId']	  	  

	  if int(row['channel'])==2:
	    if 'video:campaigns' in invertedIndex.keys():
	      k=set(invertedIndex['video:campaigns'])
	      k.add(campaignId)
	      z=list(k)
	      invertedIndex['video:campaigns'] = z
	    else:
	      invertedIndex['video:campaigns']= [campaignId] 

	    
	if row['channel']==3:      
	  campaignId=int(row['campaignId'])
	  geo = row['countryTargets'].lower()    
	  if 'mobile:geo:'+geo in invertedIndex.keys():
	    k=set(invertedIndex['mobile:geo:'+geo])
	    k.add(campaignId)
	    z=list(k)
	    invertedIndex['mobile:geo:'+str(geo)]=z
	  else:
	    invertedIndex['mobile:geo:'+geo]= [campaignId]
	      
	  creatives = row['creatives'].split(',')
	  for creative in creatives:
	    if creative != '':
		cur.execute("SELECT * FROM creatives WHERE creativeId='"+creative+"'")
		creative=int(creative)
		row1=cur.fetchone()
		width=str(row1['width'])
		height=str(row1['height'])
		size=width+"x"+height
		if 'mobile:size:'+size in invertedIndex.keys():
		  k=set(invertedIndex['mobile:size:'+size])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['mobile:size:'+size]=z
		else:
		  invertedIndex['mobile:size:'+size]= [campaignId]
		  
		creativeIndex='mobile:campaign:'+str(campaignId)+':'+str(width)+"x"+str(height)
		if creativeIndex in invertedIndex.keys():
		  k=set(invertedIndex[creativeIndex])
		  k.add(creative)
		  z=list(k)
		  invertedIndex[creativeIndex] = z
		else:
		  invertedIndex[creativeIndex]= [creative]     

	  if int(row['strategy'])==12:
	    if 'mobile:roe' in invertedIndex.keys():
	      k=set(invertedIndex['mobile:roe'])
	      k.add(campaignId)
	      z=list(k)
	      invertedIndex['mobile:roe'] = z
	    else:
	      invertedIndex['mobile:roe'] = [campaignId]
	    blacklist=row['list']
	    blacklist=blacklist.split("\n")
	    newlist=[]
	    for b in blacklist:
	      a=b.split(",")
	      newlist.extend(a)
	    for n in newlist:
	      n=n.strip()
	      if n!="":
		if 'mobile:roe:blackweb:'+n in invertedIndex.keys():
		  k=set(invertedIndex['mobile:roe:blackweb:'+n])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['mobile:roe:blackweb:'+n] = z
		else:
		  invertedIndex['mobile:roe:blackweb:'+n] = [campaignId]	  
	    blacklist=row['listMobileApp']
	    blacklist=blacklist.split("\n")
	    newlist=[]
	    for b in blacklist:
	      a=b.split(",")
	      newlist.extend(a)
	    for n in newlist:
	      n=n.strip()	
	      if n!="":
		if 'mobile:roe:blackapp:'+n in invertedIndex.keys():
		  k=set(invertedIndex['mobile:roe:blackapp:'+n])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['mobile:roe:blackapp:'+n] = z
		else:
		  invertedIndex['mobile:roe:blackapp:'+n] = [campaignId]	  	

	  if int(row['strategy'])==11:
	    whitelist=row['list']
	    whitelist=whitelist.split("\n")
	    newlist=[]
	    for b in whitelist:
	      a=b.split(",")
	      newlist.extend(a)
	    for n in newlist:
	      n=n.strip()	
	      if n!="":
		if 'mobile:whiteweb:'+n in invertedIndex.keys():
		  k=set(invertedIndex['mobile:whiteweb:'+n])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['mobile:whiteweb:'+n] = z
		else:
		  invertedIndex['mobile:whiteweb:'+n] = [campaignId]
	    whitelist=row['listMobileApp']
	    whitelist=whitelist.split("\n")
	    newlist=[]
	    for b in whitelist:
	      a=b.split(",")
	      newlist.extend(a)
	    for n in newlist:
	      n=n.strip()	
	      if n!="":
		if 'mobile:whiteapp:'+n in invertedIndex.keys():
		  k=set(invertedIndex['mobile:whiteapp:'+n])
		  k.add(campaignId)
		  z=list(k)
		  invertedIndex['mobile:whiteapp:'+n] = z
		else:
		  invertedIndex['mobile:whiteapp:'+n] = [campaignId]

	  invertedIndex['mobile:campaign:'+str(campaignId)+':bid']=float(row['bidCpm'])
	  invertedIndex['mobile:campaign:'+str(campaignId)+':pacing']=int(row['pacing'])
	  cur.execute("SELECT * FROM accounts WHERE accountId IN (SELECT accountId FROM advertisers WHERE advertiserId IN (SELECT advertiserId FROM brands WHERE brandId IN (SELECT brandId FROM plans WHERE planId IN (SELECT planId FROM campaigns WHERE campaignId='"+str(campaignId)+"'))))")
	  res = cur.fetchall()
	  res1 = res[0]
	  advertiserName = res1['accountName']
	  invertedIndex['display:campaign:'+str(campaignId)+':advertiserName']=advertiserName	 
	  
      timeTaken = time.time() - start
      print timeTaken
      invertedIndex['timeTaken']=timeTaken
      self.write(json.dumps(invertedIndex))

    if self.request.path == "/rules":
      ruleIndex=dict()
      con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'impulsedb',compress=1,cursorclass=MySQLdb.cursors.DictCursor);
      cur = con.cursor()
      channel= self.get_argument('channel', True)
      cur.execute("SELECT * FROM campaigns WHERE status = '2' AND channel IN("+channel+") AND dailyBudget>spendToday AND totalBudget>spendTotal AND startDate<=CURRENT_DATE() AND endDate>=CURRENT_DATE() AND creatives<>'' AND countryTargets<>''")
      campaigns=[]
      rows = cur.fetchall()
      for row in rows:
	campaigns.append(int(row['campaignId']))
      camps= ','.join(str(n) for n in campaigns) 
      cur.execute("SELECT * FROM rules WHERE campaignId IN("+camps+")")
      rows = cur.fetchall()
      for row in rows:
	  campaignId=int(row['campaignId'])
	  domain=row['domain'].strip()
	  city=row['city'].strip()
	  state=row['state'].strip()
	  weekday=row['weekday'].strip()	  
	  hour=row['hour'].strip()
	  dayPart=row['dayPart'].strip()
	  sizeId=row['sizeId'].strip()
	  bid=row['bid']
	  if domain=='':
	    domain='*'
	  if city=='':
	    city='*'
	  if state=='':
	    state='*'
	  if weekday=='':
	    weekday='*'	    
	  if hour=='':
	    hour='*'
	  if dayPart=='':
	    dayPart='*'
	  if sizeId=='':
	    sizeId='*'
	  key=domain+"|"+city+"|"+state+"|"+weekday+"|"+hour+"|"+dayPart+"|"+sizeId
	  if key in ruleIndex.keys():
	    ruleIndex[key][campaignId]=float(bid)
	  else:
	    ruleIndex[key]=dict()
	    ruleIndex[key][campaignId]=float(bid)
      self.write(json.dumps(ruleIndex))

    if self.request.path == "/adIndex":
      adIndex=dict()
      con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'impulsedb',compress=1,cursorclass=MySQLdb.cursors.DictCursor);
      cur = con.cursor()
      cur.execute("SELECT * FROM campaigns WHERE status IN(2,3)")
      rows = cur.fetchall()
      for row in rows:
	campaignId=row['campaignId']
	url=row['destinationUrl']
	adIndex["c:"+str(campaignId)+":url"]=url
	creativeDestinationUrl=row['creativeDestinationUrl']
	if len(creativeDestinationUrl)>2:
	  creativeUrls=json.loads(row['creativeDestinationUrl'])
	else:
	  creativeUrls=dict()
	for key in creativeUrls.keys():
	  if len(key)>0:
	    newKey="c:"+str(campaignId)+":b:"+str(key)+":url"
	    adIndex[newKey]=creativeUrls[key]
	cw=int(row['clickWindow'])
	vw=int(row['viewWindow'])
	if cw==0:
	  cw=30
	if vw==0:
	  vw=30
	adIndex["vw:"+str(campaignId)]=vw
	adIndex["cw:"+str(campaignId)]=cw
	banners=row['creatives'].split(",")
	for banner in banners:
	  if len(banner)>0:
	    cur.execute("SELECT * FROM creatives WHERE creativeId='"+str(banner)+"'")
	    row = cur.fetchone()
	    adIndex["b:"+str(banner)+":url"]=row['filePath']
	    adIndex["b:"+str(banner)+":type"]=row['creativeType']
	    adIndex["b:"+str(banner)+":width"]=int(row['width'])
	    adIndex["b:"+str(banner)+":height"]=int(row['height'])
	    adIndex["b:"+str(banner)+":code"]=row['tagCode']
	    key = "banners:"+str(campaignId)+":"+str(row['width'])+":"+str(row['height'])
	    if key in adIndex.keys():
	      adIndex[key].append(int(banner))
	    else:
	      adIndex[key] = [int(banner)]
      self.write(json.dumps(adIndex))

application = tornado.web.Application([(r".*", MainHandler),])

if __name__ == "__main__":
    application.listen(5003)
    tornado.ioloop.IOLoop.instance().start()