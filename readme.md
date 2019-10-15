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
  * **Status:** Site identified, binding created to pull shapefiles and analyze.
* Waze
  * **Status:** Identified as a source, still waiting to make first contact.
* NOAA - Storm Events Database
* NWS directly (investigate what IEM provides as opposed to direct data.  IEM seems to indicate that they do a lot to clean the NWS data)
* Drought Monitor - NOAA
* Socioeconomic Data Center - SEDAC
* National Housing Index Data

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
* Clean up Waze
* Create some sort of Storm Handler that will be used for analysis methods
* File management for Waze and IEM data
* Add unit tests for all data handlers
