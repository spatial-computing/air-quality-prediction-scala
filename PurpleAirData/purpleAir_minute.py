import requests
import json
from pprint import pprint
import psycopg2
from datetime import datetime
from pytz import timezone

conn = psycopg2.connect(host="localhost",port="5432",database="prisms", user="jjow", password="m\\tC7;cc")
cursor = conn.cursor()
#create table
'''
cursor.execute("CREATE TABLE purpleair.sensorData(uid bigint NOT NULL DEFAULT nextval('purpleair_seq'::regclass),id Int,timestamp timestamp with time zone, entry_id Int, PM1_ATM real,PM2_5_ATM real, PM10_ATM real, uptime Int, rssi real, temperature real, humidity real, PM2_5_CF_1 real, CONSTRAINT sensordata_pkey PRIMARY KEY (uid))")
conn.commit()
'''

#remove all the alphabats, so we can fit into python datetime format 
def replaceAlphabat(str):
	list1 = list(str)
	word = []
	for char in list1:
		if(char.isdigit()):
			word.append(char)
		else:
			word.append(',')
	return ''.join(word).split(',')

#get every maxtime for each sensor, the time resolution is slightly different 
cursor.execute('select id , max(timestamp) at time zone \'UTC\''\
				'from purpleair.sensordata '\
				'group by id')
idAndTime = cursor.fetchall()
conn.commit()
timeDict = {}

for dt in idAndTime:
	id = dt[0]
	max_time = datetime(dt[1].year, dt[1].month, dt[1].day, dt[1].hour, dt[1].minute, dt[1].second,tzinfo=timezone('UTC'))
	timeDict[str(id)] = max_time


cursor.execute("SELECT * FROM purpleair.purplair_sensor_la ")
records = cursor.fetchall()
conn.commit()

#get all sensor in los angeles area
for record in records:
	url = "https://thingspeak.com/channels/"+str(record[0])+"/feeds.json?api_key="+str(record[1])
	data = requests.get(url)
	data1 = json.loads(data.text)

	for objects in data1["feeds"]:
		#time we will check
		timeList = replaceAlphabat(objects["created_at"])
		timestamp = datetime(int(timeList[0]),int(timeList[1]),int(timeList[2]),int(timeList[3]),int(timeList[4]),int(timeList[5]),tzinfo=timezone('UTC'))
		#max time

		maxdt = timeDict[str(record[0])]
		if (timestamp>maxdt):
			cursor.execute('INSERT INTO purpleair.sensordata  VALUES({uid},{id},\'{time}\',{entry_id},' \
				'{PM1_ATM},{PM2_5_ATM}, {PM10_ATM}, {uptime}, {rssi} , {temperature},{humidity}, {PM2_5_CF_1});'\
				.format(uid='nextval(\'purpleair_seq\')',
					id = data1["channel"]["id"],
					time = timestamp if objects["created_at"] else 'null',
					entry_id = objects["entry_id"] if objects["entry_id"] else 'null',
					PM1_ATM = objects["field1"] if objects["field1"] else 'null',
					PM2_5_ATM = objects["field2"] if objects["field2"] else 'null',
					PM10_ATM = objects["field3"] if objects["field3"] else 'null',
					uptime = objects["field4"] if objects["field4"] else 'null',
					rssi = objects["field5"] if objects["field5"] else 'null',
					temperature = objects["field6"] if objects["field6"] else 'null',
					humidity = objects["field7"] if objects["field7"] else 'null',
					PM2_5_CF_1 = objects["field8"] if objects["field8"] else 'null'))
conn.commit()

