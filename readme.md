##Flash Flood Analysis

**Summary**
This repository is devoted to automating flash flood data analysis. It is focused on creating modular and flexible attachments to physical data (i.e. topography, land use, and precipitation), forecast and warning data (i.e. NWS forecasts and crowd-sourced storm observations), and demographic data (i.e. census data, at-risk populations, and disaster response preparedness).

**Basic Workflow**
Create bindings to particular data sources for easy, replicable analysis.  

###Current Progress (9/20/2019):

Created a data fetching function, to automate grabbing data from various sources.  Created some structure for local analysis.

####Sources

#####Forecast and Warnings

* Iowa Environmental Mesonet
  * **Status:** Site identified, binding created to pull shapefiles and analyze.
* Waze
  * **Status:** Identified as a source, still waiting to make first contact.
* NOAA - Storm Events Database
* NWS directly (investigate what IEM provides as opposed to direct data.  IEM seems to indicate that they do a lot to clean the NWS data)

#####Demographic Data

* US Census / American Community Survey
* CDC (https://www.cdc.gov/gis/geo-spatial-data.html)

#####Physical Data

* USGS (https://www.usgs.gov/core-science-systems/ngp/3dep/about-3dep-products-services)
* NLCD
* Remote Sensing