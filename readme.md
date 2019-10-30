## Flash Flood Vulnerability Analysis.

##Summary
This repository is devoted to automating analysis of Flash Flood reportings and warning issuance.  Its primary goal is to:
- handle the management of local data and necessary API calls
- enable geographic analysis
- host the creation of geographic data products and risk assessment tools

##Data Management
Current support:
- NWS Warnings
- NWS Storm Reports
- Waze

Upcoming support:
- Social Vulnerability Index (SoVI) factors from the US Census / American Community Survey

Eventual support:
- Environmental vulnerability indicators 

##Geographic Analysis
Geographic analysis is being built out using geopandas (pandas + shapely).  These will be configured to test SoVI scores for a particular reporting region. 

The initial questions being asked are:
- is there socioeconomic bias in storm warning issuance?
- how does Waze (an example of a crowdsourced data platform with storm warning support) compare in reporting to the NWS, by space and time?
- how can risk messaging be improved for at-risk communities?

##Geographic Data Products
Production maps will be created using a QGIS interface, and potentially by hosting a web map.  There is termendous information that says that risk management tools must be built with support from end-users.  What this looks like exactly in our case is yet to be determined.  


#### Technical Notes

**Current State**
* Began time-series analysis, developing several scores per geographic region for Waze reports.  Waze reports per polygon, time and space density
* Began output creation to QGIS

**Immediately Next Up**
* Create attachments for US Census and American Community Survey
* Develop SoVI scores
* Create QGIS Processing Script to load and visualize the data

**And then**
* Add unit tests for all data handlers

**Future**
* Interactive mapping, to interact with the statistical outputs (maybe Tableau)

**Potential Sources**
* NOAA - Storm Events Database
* National Hurricane Center
* Socioeconomic Data Center - SEDAC
* National Housing Index Data
* CDC (https://www.cdc.gov/gis/geo-spatial-data.html)
* ATSDR's GRASP - Social Vulnerability Index
* Particularly Dangerous Situation Warnings (tornados, but may have a parallel for flash floods).
