# Traffic Accidents in Germany

## Quick Start
You may directly check out the [data analysis notebook](https://github.com/lnrd96/Data-Engineering-Project-on-Traffic-Accidents/blob/main/project/report.ipynb]). There, the data analysis is performed and conclusions are drawn.
The [data pipeline](https://github.com/lnrd96/Data-Engineering-Project-on-Traffic-Accidents/blob/main/project/Pipeline.py) is independent of it as the jupyter notebook assumes the data to be present in the database. That is achieved in the linked class, `Pipeline.py`, which downloads, preprocesses and structures the data before persisting it.

## Summary
This project explores characteristics of traffic accidents in germany.  Moreover investigates correlations between the accidents and the types of roads they took place.
Like that it can be explored if a road type favors accident probabilities of certain traffic participants, e.g. cyclists or motorists, or whether certain locations and road types have a higher accident density than others. Moreover regions with bad lighting conditions can be identified.
Accident probabilities given road type and lighting conditions can be found out.

The German police have been reporting road accidents yearly since 2016. In this project the data was restricted to the years 2018 - 2020 such that the amount of data meets the available computational ressources.
The Open Street Map Organisation allows to query the road type based on the accident coordinates.

## Rationale
The analysis aims to identify what road types and regions may favor accidents. This could be used to divide Germany into different regions of accident risk based on the regions's accident densities. Such a system could enable planning of hospital and ambulance infrastructure to meet the needs of specific regions, potentially saving lives. It can also show safety vulnerabilities of certain highways. This could be used by public administration to take targeted measures against them.

## Datasources

### Datasource 1: Mobilithek
- Metadata URL: https://mobilithek.info/offers/-516123251286799708
- Data URL: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
- Data Type: CSV

Each data point consists of location and time of the accident as well as the type of accident and the type of traffic partificants as well as the lighting conditions. Note that the time resolution does not allow to assign an accident to the day it took place but only to the the weekday in the month - not the actual week.

### Datasource 2: Open Street Map
- Metadata URL: https://www.openstreetmap.de/
- Data URL: https://nominatim.org/release-docs/latest/api/Reverse/
- Data Type: API / JSON
