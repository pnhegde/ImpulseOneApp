import MySQLdb
import MySQLdb.cursors
import datetime


con = MySQLdb.connect('localhost', 'root', 'appyfizz', 'logs',compress=1,cursorclass=MySQLdb.cursors.DictCursor);
cur = con.cursor()

queryList=[]
query = "SELECT campaignId, date, domain, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date, domain"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  try:
    query = "INSERT INTO `report`.`domain` (`id`, `campaignId`, `date`, `domain`, `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+row["domain"]+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"
  
query = "SELECT campaignId, date, state, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date,state"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  try:
    query = "INSERT INTO `report`.`state` (`id`, `campaignId`, `date`, `state`, `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+str(row["state"])+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"
  
query = "SELECT campaignId, date, city, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date,city"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  try:
    query = "INSERT INTO `report`.`city` (`id`, `campaignId`, `date`, `city`, `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+str(row["city"])+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"

query = "SELECT campaignId, date, HOUR(time) as hour,bannerId, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date, HOUR(time),bannerId"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  if row["hour"]>=2 and row["hour"]<6:
      daypart="1"
  if row["hour"]>=6 and row["hour"]<10:
      daypart="2"
  if row["hour"]>=10 and row["hour"]<14:
      daypart="3"
  if row["hour"]>=14 and row["hour"]<18:
      daypart="4"
  if row["hour"]>=18 and row["hour"]<22:
      daypart="5"
  if row["hour"]>=22 or row["hour"]<2:
      daypart="6"  
  try:
    query = "INSERT INTO `report`.`hour` (`id`, `campaignId`, `date`,`weekday`, `hour`,`daypart`,`creative`, `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+str(row["date"].strftime("%w"))+"', '"+str(row["hour"])+"','"+str(daypart)+"','"+str(row["bannerId"])+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"

query = "SELECT campaignId, date, carrier, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date,carrier"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  try:
    query = "INSERT INTO `report`.`carrier` (`id`, `campaignId`, `date`, `carrier`, `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+str(row["carrier"])+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"
  
query = "SELECT campaignId, date, device, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date,device"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  try:
    query = "INSERT INTO `report`.`device` (`id`, `campaignId`, `date`, `device`, `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+str(row["device"])+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"


query = "SELECT campaignId, date, domain, state, city, COUNT(*) AS impressions, SUM(clicked) AS clicks, SUM(clickConversion) AS clickconversions, SUM(viewConversion) AS viewconversions, (SUM(price)/1000) AS spend FROM logsnew WHERE DATE(date)=DATE(NOW()) OR DATE(date)=UNIX_TIMESTAMP(subdate(current_date, 1)) GROUP BY campaignId, date,domain, state, city"
cur.execute(query)
rows=cur.fetchall()
for row in rows:
  if row["clickconversions"]==None:
    row["clickconversions"]=0
  if row["viewconversions"]==None:
    row["viewconversions"]=0
  if row["clicks"]==None:
    row["clicks"]=0
  if row["spend"]==None:
    row["spend"]=0
  try:
    query = "INSERT INTO `report`.`domaingeo` (`id`, `campaignId`, `date`, `domain`,`state`,`city` `impressions`, `clicks`, `viewconv`, `clickconv`, `spend`) VALUES (NULL, '"+str(row["campaignId"])+"', '"+row["date"].strftime("%Y-%m-%d")+"', '"+str(row["domain"])+"', '"+str(row["state"])+"', '"+str(row["city"])+"', '"+str(row["impressions"])+"','"+str(row["clicks"])+"', '"+str(row["viewconversions"])+"', '"+str(row["clickconversions"])+"', '"+str(row["spend"])+"') ON DUPLICATE KEY UPDATE impressions='"+str(row["impressions"])+"', clicks='"+str(row["clicks"])+"', viewconv='"+str(row["viewconversions"])+"', clickconv='"+str(row["clickconversions"])+"', spend='"+str(row["spend"])+"';"
    queryList.append(query)    
  except:
    print "exception"

for query in queryList:
  cur.execute(query)
con.commit()