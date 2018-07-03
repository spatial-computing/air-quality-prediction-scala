# prisms-air-quality-modeling

In this project, we build an accurate fine-scale air quality prediction model. Here is the paper: [Mining Public Datasets for Modeling Intra-City PM2.5 Concentrations at a Fine Spatial Resolution](https://dl.acm.org/citation.cfm?id=3139958.3140013)

## Data Source
### Air Quality Data
We are collecting the air quality data, including O3, PM25, PM10, CO, NO2, and SO2 concentration/AQI observations from the monitoring stations in [Los Angeles County](https://www.lacounty.gov/) through the [EPA’s Airnow web service](https://docs.airnowapi.org/webservices). The air quality table (los_angeles_air_quality) has been initialized in JonSnow database ([SQL code](/SQL/create_la_aq_table.sql)). We query the web servive every hour automatically ([Python code](/PythonCode/epa_sensor_data_request.py)) using crontab (Appendix I).

### Weather Data
We are collecting meteorological data through the [Dark Sky API](https://darksky.net/dev/docs). The weather table (los_angeles_meteorology) has been initialized in JonSnow database ([SQL code](/SQL/create_la_aq_table.sql)). We query the web servive every hour at a given location (or sensor locations) automatically ([Python code](/PythonCode/epa_sensor_data_request.py)) in the same way. We can also query the data from a given time to a given time ([Python code](/PythonCode/epa_sensor_data_request.py)).

### Geographic Data
We are using Openstreetmap to generate geographic features for our model. For a given location, it creates the buffers (default 100m-3000m with 100m interval) around the location and compute the intersected area/length/count between those buffers and various geographic categories in Openstreetmap data (see figure below). ([Python code](/PythonCode/generate_geo_feature_types.py)) 
>>>>>>>>![ScreenShot](/images/geoabstraction_example.png)

### Other data sources
#### Purple Air
We are collecting data from [Purple Air](https://www.purpleair.com/). We query the Purple Air web service that each sensor updates air qualty data around every minute, including PM2.5, PM10, PM1, temperature, and humidity. Each machine has two channels (A and B) at the same location. The two-channel mechanism ensures if one channel has noises, the other one can still work properly. 
Each unique "sensor" has three ID numbers:
- id - Each sensor (channel A or B) has its unique id
- sensor_id - Each sensor (channel A or B) has its unique sensor_id
- parent_id - For channel A sensor, it has a unique parent_id. When parent_id = null, it indicates a channel B sensor. If the id from a channel B sensor equals to the parent_id from a channel A sensor, the two sensors share the same machine and location.
                  
#### Fishnet Data
Grids over Los Angeles County (around 3000 points), used for fine-scale prediction

## Algorithm
Edit configuration in [config.json](/PRISMS_AirQualityPrediction/src/data/model/config.json).
### High Level Architecture
>>>>>>>>![ScreenShot](/images/high_level_architecture.png)
### Model Evaluation
- Cross Validation
Run [CrossValidation.scala](/AirQualityPrediction/src/main/scala/Demo/CrossValidation.scala) to evaluate the model with itself.
- Validation
Run [Validation.scala](/PRISMS_AirQualityPrediction/src/main/scala/Demo/Validation.scala) to evaluate the model with other dataset.
### Fishnet Prediction
Run [FishnetPrediction.scala](/AirQualityPrediction/src/main/scala/Demo/FishnetPrediction.scala) to get the prediction result for fishnet.
(Current Time or From Time To Time)

## Appendix
### I. Access JonSnow Database
You need to get the username and password for both JonSnow server and database. 
- Log in server with the server username and password
```
ssh -L [your local port]:localhost:5432 [your username]@jonsnow.usc.edu
```
- Use Postico (only Mac) or PgAdmin to log in with database username and passward show in the figure below. [port] would be [your local port].
>>>>>>>>![ScreenShot](/images/database_login.jpg)

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
