# Project Plan

## Summary
This project investigates the correlations between traffic accidents in Germany and the road type of the respective accidents. 
Additionaly, it can be explored if a road type favors accident probabilities of certain traffic participants, e.g. cyclists or motorists, or whether certain locations and road types a higher accident density than others. Moreover regions with bad lighting conditions can be identified.
Accident probabilities and severeness, given road type and lighting conditions can be found out.

The German police have been reporting road accidents yearly since 2016.
The Open Street Map Organisation allows to query the road type based on the accident coordinates.

## Rationale
The analysis aims to identify what road types and regions may favor accidents. This can divide Germany into different regions of accident risk based on the regions's accident densities. Such a system could enable planning of hospital and ambolance infrastructure to meet the needs of specific regions, potentially saving lives.

## Datasources

### Datasource 1: Mobilithek
- Metadata URL: https://mobilithek.info/offers/-516123251286799708
- Data URL: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
- Data Type: CSV

Each data point consists of location and time of the accident as well as the type of accident and the type of traffic partificants as well as the lighting conditions.

### Datasource 2: Open Street Map
- Metadata URL: https://www.openstreetmap.de/
- Data URL: https://nominatim.org/release-docs/latest/api/Reverse/
- Data Type: API / JSON

## Work Packages

1. [Data Collection](https://github.com/users/lnrd96/projects/4/views/2#)
2. [Data Cleaning and Preprocessing](https://github.com/users/lnrd96/projects/4/views/2#)
3. [Database Design](https://github.com/users/lnrd96/projects/4/views/2#)
4. [Data Integration](https://github.com/users/lnrd96/projects/4/views/2#)
5. [Visualization](https://github.com/users/lnrd96/projects/4/views/2#)
6. [Visualisation](https://github.com/users/lnrd96/projects/4/views/2#)
7. [Visualisation](https://github.com/users/lnrd96/projects/4/views/2#)
8. [Visualisation](https://github.com/users/lnrd96/projects/4/views/2#)
