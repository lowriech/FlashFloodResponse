## Flash Flood Vulnerability Analysis.

### Summary
This repository is devoted to automating analysis of Flash Flood warning issuance, as part of the NASA Earth Observations project:
- https://appliedsciences.nasa.gov/content/18-geogfrm-0006

It is intended to provide an abstract functionality to compare different spatial-temporal events (such as flash flood warnings and flood reports) to quantify the effectiveness of issued warnings.
The initial use case of this is to compare flood reports from the navigation app *Waze* against the US National Weather Service (NWS).
Waze data was initially supplied for several large storms (see WAZE_REGISTRY in waze.py file), but the end goal is to request a live feed of Waze reports.

### Abstract Space-Time Functionality: Vector
Holds classes for analysis of 2-D containment ("Does a storm report fall within an issued warning?"), which is enabled by several classes:
- AbstractTimePointEvent, an event that happens at a point in time
- AbstractTimeDurationEvent, an event that has a duration in time
- AbstractGeoHandler, a wrapper on GeoPandas' GeoDataFrames
- RemoteDataManager, which allows for basic URL construction to fetch remote data

### In Progress: Social Vulnerability, Vector-Raster comparisons
This will include functionality to ask aggregate questions about interaction between the vector classes described above, and rasters.  
The initial use case of this is for demographic analysis of NWS Warnings ("Are warnings equally distributed, considering the underlying populations that are warned?")

### Research Questions
1. Vulnerability Analysis
    1. Equity of coverage of NWS Warnings by impact and vulnerability/socioeconomic/demographic variables.  
    2. Vulnerability will be informed by research in flooding, particularly by Alex de Sherbinin
    3. Goal of integrating information for Impact Based Forecasting guidelines.
    4. How does the vulnerability contained within NWS Warnings compare to the overall area’s vulnerability?
2. Waze as an alternative reporting dataset
    1. Does Waze represent gaps in NWS coverage?
    2. Does NWS effectively capture high-volume Waze reports?

#### Future, as needed
* An API for development of indices, such as Social Vulnerability Indices (SoVIs)
* QGIS Processing Toolbox interface
* Interactive mapping using React
* Implement a PostGRES, PostGIS server

**Potential Sources**
* NOAA - Storm Events Database
* National Hurricane Center
* Socioeconomic Data Center - SEDAC
* National Housing Index Data
* CDC (https://www.cdc.gov/gis/geo-spatial-data.html)
* ATSDR's GRASP - Social Vulnerability Index
* Particularly Dangerous Situation Warnings (tornados, but may have a parallel for flash floods).
