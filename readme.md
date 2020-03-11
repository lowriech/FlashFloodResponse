## Flash Flood Geostatistics Internship

### Abstract
In recent years, increasing attention has been paid to the potential of supplementing hazard forecasting systems using volunteered geographic information (VGI) (de Albuquerque et al., 2015), typically with a focus on data mining during large events.  This attention is driven, in part, by the difficulty of detecting certain hazard occurrences, and thus the incompleteness of hazard verification databases.  In the United States, the National Weather Service’s (NWS) Flash Flood Local Storm Reports (LSR) service has been shown to have an incomplete representation of historic flash floods in the United States (Gourley, 2010), with important potential feedbacks into the issuance of future Storm Warnings (SWs). This presents an obstacle in the issuance of future warnings, and in the development of an Early Warning Early Action system for flash floods.  

In this project, we examine the use of Waze data to verify flash flood occurrences.  Waze has a significant advantage as a source of VGI, in that it can be assumed to be both spatially and temporally well-referenced.  Given that any individual Waze report is of uncertain magnitude in comparison to established LSR procedures, there is a need to quantify the “flashiness” of Waze reports.  Using data from Hurricane Harvey, our work develops an application for assimilating social media reporting into a spatial analytic framework, and develops spatial-temporal signatures that can be used to translate Waze reports into flash flood reports. 

### Repository Summary
This repository is devoted to validation of Local Storm Reports and Flash Flood Warnings.  This work is funded as part of the NASA Earth Observations project:
- https://appliedsciences.nasa.gov/content/18-geogfrm-0006

This repo provides functionality to compare different spatial-temporal events to quantify the effectiveness of issued warnings.

##### spatial_analytics.py
Holds classes for geostatistical and temporal analysis

##### spacetime_handlers.py
Holds classes for managing spatial and spatial-temporal data.
Functionality for managing remote and local data sources.

##### nws.py
Inherits from spacetime_handlers, and builds out functionality for using NWS Flash Flood data from the Iowa Environmental Mesonet.

##### waze.py
Inherits from spacetime_handlers, and builds out functionality for using Waze VEOC data supplied.
Data supplied includes major storms across the Southeastern United States over the last 6 years.

##### raster_manager.py - In Progress
Funtionality for working with rasters and NetCDFs and asking spatial containment questions.

### Methods
Using Hurricane Harvey as a case study, we assess the LSRs that are most concentrated in space and time with Waze events.  For each LSR, we create an individual study area to use in determining the Waze concentration. 
##### Study area: 
- A spatial buffer of X=1500 m (subject to change)
- A temporal buffer of (-3, +1) hours 
- This four hour buffer is offset to center on an hour before the LSR, due to our theory and visual inspection that the LSR typically follows peak Waze activity
- We’ll use this study area to gauge spatial and temporal randomness.  

##### Spatial-Temporal Density
- For each Waze point, we then measure the spatial buffer distance necessary to include x Waze points, with x=25, 50, or 100 Waze points.  This gives us a distribution of distances that represent the level of clustering between LSRs and Waze, subject to the study area conditions.  Of these, we look at the p5, p10, and p25 percentiles to identify the LSRs with the tightest spatial relationships to their Waze neighbors.  
- Of these highly associated LSRs, we now look for significant areas of temporal clustering within their associated Waze reports.  For each Waze report, we measure how many Waze neighbors it has within 15, 30 and 60 minutes.  This gives us a density distribution of the most temporally clustered events within the already spatially clustered events.  We then take the p5, p10, p25 of these spatial temporal clusterings, which gives us a signal for the Waze reports that most closely match the LSRs.  
- This signal is derived solely from the direction of the LSRs, which means that it can now be applied to group the Waze reports and find events that may have been missed.  We now take the spatial and temporal clustering parameters we’ve derived, use them to cluster the Waze values, and identify if we can validate any Storm Warnings that were previously not validated by LSRs.
