# Project Plan

## Summary
This project investigates the correlations between traffic accidents in Germany and the weather conditions at the time of the respective accidents. To do so, it must be questioned whether the weather at accident times, is a specific subset or a random subset of the general weather data. Or in other words - what is the accident probability given a specific wheather condition?
Additionaly, it can be explored if a specific weather condition favors accident probabilities of certain traffic participants, e.g. cyclists or motorists, or whether certain locations are more responsive to a certain wheather condition than others. In the course of the project, it will become clear to what extent these additional questions can be answered.

The German police have been reporting road accidents yearly since 2016, and the study utilizes historical weather data provided by Meteostat.

## Rationale
The analysis aims to identify what wheather conditions may favor accidents. This can inform about the feasability of the establishment of a prediction system that divides Germany into regions of accident probabilities based on real-time weather and traffic data. Such a system could enable ambulances to be sent in advance to regions with high accident probabilities, potentially saving lives. Additionally, the analysis could provide insights that enable the adaptation of speed limits and road signs on specific roads, ensuring safer driving conditions.

## Datasources

### Datasource 1: Mobilithek
- Metadata URL: https://mobilithek.info/offers/-516123251286799708
- Data URL: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
- Data Type: CSV

Each data point consists of location and time of the accident as well as the type of accident and the type of traffic partificants as well as the lighting conditions.

### Datasource 2: Meteostat
- Metadata URL: https://dev.meteostat.net/guide.html
- Data URL: https://dev.meteostat.net/bulk/#quick-start
- Data Type: CSV / JSON

Meteostat provides a range of weather properties, including temperature, precipitation, wind speed and direction, humidity, air pressure, and solar radiation. It also provides derived indices such as heat index, wind chill, and dew point. Users can access the data in a variety of formats, including CSV, JSON, and from within Python, making it easy to integrate the data into a Jupyter Notebook.

## Work Packages

1. [Data Collection](https://github.com/users/lnrd96/projects/4/views/2#)
2. [Data Cleaning and Preprocessing](https://github.com/users/lnrd96/projects/4/views/2#)
3. [Database Design](https://github.com/users/lnrd96/projects/4/views/2#)
4. [Data Integration](https://github.com/users/lnrd96/projects/4/views/2#)
5. [Visualization](https://github.com/users/lnrd96/projects/4/views/2#): Accidents and wheather vectors
6. [Visualisation](https://github.com/users/lnrd96/projects/4/views/2#): Correlation wheather and accident
7. [Visualisation](https://github.com/users/lnrd96/projects/4/views/2#): Influence of different wheather attributes on accidents
8. [Visualisation](https://github.com/users/lnrd96/projects/4/views/2#): Different locations of accidents at good and bad wheather
