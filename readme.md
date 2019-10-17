## Flash Flood Analysis

**Summary**
This repository is devoted to automating flash flood data analysis. It is focused on creating modular and flexible attachments to physical data (i.e. topography, land use, and precipitation), forecast and warning data (i.e. NWS forecasts and crowd-sourced storm observations), and demographic data (i.e. census data, at-risk populations, and disaster response preparedness).

**Basic Workflow**
Create bindings to particular data sources for easy, replicable analysis.  

### Current Progress (10/15/2019):

Completed handlers for NWS Storm Reports and Storm Warnings, and script to pull Waze VEOCs.  Built a file system / url request manager - this enables all of the analysis we do to be agnostic of data source or storm, and the application will handle if we need to fetch new data.  Starting time-analysis for Waze and NWS, starting with how frequently Waze and NWS align/don't align.  Starting to look for Socioeconomic data.

### Current Progress (9/27/2019):

Binding for Waze data created using the Google Sheets API.

### Current Progress (9/20/2019):

Created a data fetching function, to automate grabbing data from various sources.  Created some structure for local analysis.

#### Sources

##### Forecast and Warnings

* Iowa Environmental Mesonet
  * **Status:** Handler created
* Waze
  * **Status:** Handler created
* NOAA - Storm Events Database
* Drought Monitor - NOAA
* Socioeconomic Data Center - SEDAC
* National Housing Index Data
* Look into Particularly Dangerous Situation Warnings.  These exist for tornados, but not yet for floods.

##### Demographic Data

* US Census / American Community Survey
* CDC (https://www.cdc.gov/gis/geo-spatial-data.html)
* ATSDR's GRASP - Social Vulnerability Index

##### Physical Data

* USGS (https://www.usgs.gov/core-science-systems/ngp/3dep/about-3dep-products-services)
* NLCD
* Remote Sensing

### Technical ToDo
* Create an abstract parent class for StormReportHandler and StormWarningHandler
* Begin time-series / analysis objects.  Kernel Density Smoothing to determine the time density of reports in a given area.
* Clean up Waze
* Add unit tests for all data handlers
