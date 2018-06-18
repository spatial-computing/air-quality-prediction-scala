from pprint import pprint
import psycopg2
from datetime import datetime
from pytz import timezone

def translate(value, leftMin, leftMax, rightMin, rightMax):
    leftSpan = leftMax - leftMin
    rightSpan = rightMax - rightMin
    valueScaled = float(value - leftMin) / float(leftSpan)
    return rightMin + (valueScaled * rightSpan)

def concentrationtoaqi(con):
    if (con < 12 and con >= 0):
        Good = translate(con,0, 12, 0, 50)
        return Good
    elif (con < 35.5 and con >= 12):
        Moderate = translate(con, 12, 35.5, 50, 100)
        return Moderate
    elif (con < 55.5 and con >= 35.5):
        UnhealthyForSensitive = translate(con,35.5, 55.5, 100, 150)
        return UnhealthyForSensitive
    elif (con < 150.5 and con >= 55.5):
        Unhealthy = translate(con,55.5, 150.5, 150, 200)
        return Unhealthy
    elif (con < 250.5 and con >= 150.5):
        VeryUnhealthy = translate(con,150.5, 250.5, 200, 300)
        return VeryUnhealthy
    elif (con < 350.5 and con >= 250.5):
        Hazardous1 = translate(con,250.5, 350.5, 300, 400)
        return Hazardous1
    elif (con < 500.5 and con >= 350.5):
        Hazardous2 = translate(con,350.5, 500.5, 400, 500)
        return Hazardous2
    else:
        return None


conn = psycopg2.connect(host="localhost",port="5432",database="prisms", user="jjow", password="m\\tC7;cc")
cursor = conn.cursor()
#before executing the code below
#delete table
#drop sequence
#create sequence
#create table
#get the past time data to hourly data
'''
#get time from airnow_reporting_area
cursor.execute('select date_observed at time zone \'UTC\''\
               'from airnow_reporting_area '\
               'where date_observed>\'2018-05-31 19:59:59-07\' '\
               'group by date_observed '\
               'order by date_observed asc ')

timeCenter = cursor.fetchall()
conn.commit()

#for each date insert a aggregated outcome to hourly table
for dt in timeCenter:
    timeUTC = datetime(dt[0].year, dt[0].month, dt[0].day, dt[0].hour,
                       dt[0].minute, dt[0].second, tzinfo=timezone('UTC'))

    cursor.execute('select timestamp,id, median, ST_SetSRID(location,4326) '\
                    'from purpleair.purplair_sensor_la '\
                    'join (select id, median(pm2_5_atm),\'{time}\' '\
                    'at time zone \'America/Los_Angeles\' as timestamp '\
                    'from purpleair.sensordata '\
                    'where timestamp > \'{time}\'at time zone \'America/Los_Angeles\' -(10 * interval \'1 minute\') '\
                    'AND timestamp < \'{time}\'at time zone \'America/Los_Angeles\' +(10 * interval \'1 minute\') '\
                    'group by id ) as medianTable on id = sensor_id'\
                    .format(time=timeUTC))

    hourlyData = cursor.fetchall()
    conn.commit()
    for each in hourlyData:
        aqi = concentrationtoaqi(each[2])
        if aqi:
            cursor.execute('insert into purpleair.purpleair_sensor_la_hourly values({uid}'
                           ',\'{timestamp}\',{id},{median},{con},\'{location}\')'
                           .format(uid='nextval(\'purpleair.purpleair_sensor_la_hourly_seq\')',
                                   timestamp=each[0],
                                   id=each[1],
                                   median=aqi,
                                   con=each[2],
                                   location=each[3]))
    conn.commit()
    #pprint(timeCenter)
    '''

#compare the max time in hourly data with the max time from reporting_area, if not up to date, insert the max time

cursor.execute('select max(date_observed) at time zone \'UTC\' '\
               'from airnow_reporting_area ')
timeCenter = cursor.fetchall()
conn.commit()
cursor.execute('select max(timestamp) at time zone \'UTC\' '\
               'from purpleair.purpleair_sensor_la_hourly ')
purplemaxtime = cursor.fetchall()
conn.commit()
#get the max time
timeUTC = datetime(timeCenter[0][0].year, timeCenter[0][0].month, timeCenter[0][0].day, timeCenter[0][0].hour,
                   timeCenter[0][0].minute, timeCenter[0][0].second, tzinfo=timezone('UTC'))
purplemaxtimeUTC = datetime(purplemaxtime[0][0].year, purplemaxtime[0][0].month, purplemaxtime[0][0].day, purplemaxtime[0][0].hour,
                            purplemaxtime[0][0].minute, purplemaxtime[0][0].second, tzinfo=timezone('UTC'))
#compare time
if(timeUTC>purplemaxtimeUTC):
    cursor.execute('select timestamp,id, median, ST_SetSRID(location,4326) ' \
                   'from purpleair.purplair_sensor_la ' \
                   'join (select id, median(pm2_5_atm),\'{time}\' ' \
                   'at time zone \'America/Los_Angeles\' as timestamp ' \
                   'from purpleair.sensordata ' \
                   'where timestamp > \'{time}\'at time zone \'America/Los_Angeles\' -(10 * interval \'1 minute\') ' \
                   'AND timestamp < \'{time}\'at time zone \'America/Los_Angeles\' +(10 * interval \'1 minute\') ' \
                   'group by id ) as medianTable on id = sensor_id' \
                   .format(time=timeUTC))

    hourlyData = cursor.fetchall()
    conn.commit()
    for each in hourlyData:
        aqi = concentrationtoaqi(each[2])
        if aqi:
            cursor.execute('insert into purpleair.purpleair_sensor_la_hourly values({uid}'
                           ',\'{timestamp}\',{id},{median},{con},\'{location}\')'
                           .format(uid='nextval(\'purpleair.purpleair_sensor_la_hourly_seq\')',
                                   timestamp=each[0],
                                   id=each[1],
                                   median=aqi,
                                   con=each[2],
                                   location=each[3]))
    conn.commit()
    pprint(timeCenter)


