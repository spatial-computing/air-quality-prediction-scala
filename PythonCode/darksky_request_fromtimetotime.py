import requests
import json
import psycopg2
from datetime import datetime, timedelta
import pytz

def get_locations(aq_stations, conn):
    cur = conn.cursor()
    cur.execute('select station_id, st_astext(location) from {aq_stations}'.format(aq_stations=aq_stations))
    res = cur.fetchall()
    cur.close()
    locations = {}
    for each in res:
        idx1 = each[1].index('(')
        idx2 = each[1].index(')')
        if idx1 != -1 and idx2 != -1:
            lon = float(each[1][idx1+1:idx2].split(' ')[0])
            lat = float(each[1][idx1:idx2].split(' ')[1])
            locations[(lon, lat)] = each[0]
    return locations


conn = psycopg2.connect(host='localhost', port='11223', user='yijun', password='m\\tC7;cc', database='prisms')
stations = get_locations('los_angeles_aq_stations', conn)

tz = pytz.timezone('America/Los_Angeles')
dt = datetime.now(tz)
t = dt.strftime('%Y-%m-%dT%H:00:00%z')
print(t)

start_date = datetime.strptime('2018-05-06 02:00:00', '%Y-%m-%d %H:00:00')
end_date = datetime.strptime('2018-06-14 12:00:00', '%Y-%m-%d %H:00:00')

conn = psycopg2.connect(host='localhost', port='11223', user='yijun', password='m\\tC7;cc', database='prisms')
cur = conn.cursor()

while start_date < end_date:
    t = start_date.strftime('%Y-%m-%dT%H:00:00')
    for coord, station in stations.items():
        url = 'https://api.darksky.net/forecast/9d6604af7d3555c4a6a69de7c1ea426e/{lat},{lon},{time}?' \
              'exclude=[minutely,hourly,daily,alerts,flags]' \
              .format(lat=coord[1], lon=coord[0], time=t)
        try:
            raw_data = requests.get(url)
        except Exception as e:
            print('Current time: {time}. Error message: {msg}'.format(time=datetime.now(), msg=e))
            continue

        json_data = json.loads(raw_data.text)
        current_data = json_data['currently']

        location = 'ST_GeomFromText(\'POINT({lon} {lat})\', 4326)'.format(lon=coord[0], lat=coord[1])

        sql = 'insert into public.los_angeles_weather values({uid}, \'{date_observed}\', \'{station_id}\', ' \
              '\'{timezone}\', \'{summary}\', \'{icon}\', {precip_intensity}, {precip_probability}, {temperature}, ' \
              '{apparent_temperature}, {dew_point}, {humidity}, {pressure}, {wind_speed}, {wind_bearing}, {cloud_cover}, ' \
              '{uv_index}, {visibility}, {ozone}, {location})' \
              .format(uid='nextval(\'los_angeles_weather_uid_seq\')',
                      date_observed=start_date,
                      station_id=station,
                      timezone=json_data['timezone'] if json_data.get('timezone') else 'null',
                      summary=current_data['summary'] if current_data.get('summary') else 'null',
                      icon=current_data['icon'] if current_data.get('icon') else 'null',
                      precip_intensity=current_data['precipIntensity'] if current_data.get('precipIntensity') else 'null',
                      precip_probability=current_data['precipProbability'] if current_data.get('precipProbability') else 'null',
                      temperature=current_data['temperature'] if current_data.get('temperature') else 'null',
                      apparent_temperature=current_data['apparentTemperature'] if current_data.get('apparentTemperature') else 'null',
                      dew_point=current_data['dewPoint'] if current_data.get('dewPoint') else 'null',
                      humidity=current_data['humidity'] if current_data.get('humidity') else 'null',
                      pressure=current_data['pressure'] if current_data.get('pressure') else 'null',
                      wind_speed=current_data['windSpeed'] if current_data.get('windSpeed') else 'null',
                      wind_bearing=current_data['windBearing'] if current_data.get('windBearing') else 'null',
                      cloud_cover=current_data['cloudCover'] if current_data.get('cloudCover') else 'null',
                      uv_index=current_data['uvIndex'] if current_data.get('uvIndex') else 'null',
                      visibility=current_data['visibility'] if current_data.get('visibility') else 'null',
                      ozone=current_data['ozone'] if current_data.get('ozone') else 'null',
                      location = location)
        cur.execute(sql)
        conn.commit()
    start_date = start_date + timedelta(hours=1)