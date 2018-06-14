# prisms-air-quality-modeling

In this project, we build an accurate fine-scale air quality prediction model. Here is the paper: [Mining Public Datasets for Modeling Intra-City PM2.5 Concentrations at a Fine Spatial Resolution](https://dl.acm.org/citation.cfm?id=3139958.3140013)

## Data Source
### Air Quality Data
We are collecting the air quality data, including O3, PM25, PM10, CO, NO2, and SO2 concentration/AQI observations from the monitoring stations in [Los Angeles County](https://www.lacounty.gov/) through the [EPA’s Airnow web service](https://docs.airnowapi.org/webservices). The air quality table should be initialized in JonSnow database ([SQL code](https://github.com/spatial-computing/air-quality-prediction-model/tree/master/SQL/create_la_aq_table.sql)). We query the web servive every hour automatically ([Python code](epa_sensor_data_request.py)) using crontab (Appendix I).

### Weather Data
We are collecting meteorological data through the [Dark Sky API](https://darksky.net/dev/docs). We query the web servive every hour at a given location (sensor locations or other locations) automatically ([Python code](epa_sensor_data_request.py)) in the same way.



## Appendix
### I. crontab
- Check all the running crontab
```
crontab -l    
```
= Edit user crontab file
```
crontab -e   
```
= Update crontab operations
```
sudo /bin/systemctl restart crond.service
```
