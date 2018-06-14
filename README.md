# prisms-air-quality-modeling

In this project, we build an accurate fine-scale air quality prediction model. Here is the paper: [Mining Public Datasets for Modeling Intra-City PM2.5 Concentrations at a Fine Spatial Resolution](https://dl.acm.org/citation.cfm?id=3139958.3140013)

## Data Source
### Air Quality Data
We are collecting the air quality data, including O3, PM25, PM10, CO, NO2, and SO2 concentration/AQI observations from the monitoring stations in [Los Angeles County](https://www.lacounty.gov/) through the [EPA’s Airnow web service](https://docs.airnowapi.org/webservices). The air quality table (los_angeles_air_quality) has been initialized in JonSnow database ([SQL code](/SQL/create_la_aq_table.sql)). We query the web servive every hour automatically ([Python code](/PythonCode/epa_sensor_data_request.py)) using crontab (Appendix I).

### Weather Data
We are collecting meteorological data through the [Dark Sky API](https://darksky.net/dev/docs). The weather table (los_angeles_meteorology) has been initialized in JonSnow database ([SQL code](/SQL/create_la_aq_table.sql)). We query the web servive every hour at a given location (or sensor locations) automatically ([Python code](/PythonCode/epa_sensor_data_request.py)) in the same way. We can also query the data from a given time to a given time ([Python code](/PythonCode/epa_sensor_data_request.py)).

### Geographic Data
We are using Openstreetmap to generate geographic features for our model. For a given location, it creates the buffers (default 100m-3000m with 100m interval) around the location and compute the intersected area/length/count between those buffers and various geographic categories in Openstreetmap data (see figure below). ([Python code](/PythonCode/generate_geo_feature_types.py)) 
![ScreenShot](/images/geoabstraction_example.png?raw=true)

### Other data sources
- Purple Air

- Fishnet Data
Grids over Los Angeles County (around 3000 points), used for fine-scale prediction

## Algorithm

## Appendix
### I. Access JonSnow Database
You need to get the username and password for both JonSnow server and database. 
- Log in server with the server username and password
```
ssh -L [your local port]:localhost:5432 [your username]@jonsnow.usc.edu
```
- Use Postico (only Mac) or PgAdmin to log in with database username and passward show in the figure below. [port] would be [your local port].
![ScreenShot](/images/databaseLogin.jpg?raw=true)

### II. crontab
- Check all the running crontab
```
crontab -l    
```
- Edit user crontab file
```
crontab -e   
```
- Update crontab operations
```
sudo /bin/systemctl restart crond.service
```
