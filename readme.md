## Flash Flood Geostatistics Internship

### Abstract (publication in progress)
This paper examines the appropriateness of using Waze data to verify flash flood occurrences.  Waze is a promising source of VGI data for measuring flooding, however, similar to  all VGI, there are constraints to consider. The primary focus of this paper is to establish robust methods for using Waze reports to supplement existing reporting systems in the United States. Developed using Hurricane Harvey as a case study, social media reports are assimilated into a spatial analytic framework that derives spatial and temporal clustering parameters supported by associations between Waze and existing reports.  These parameters are then applied to find previously unreported flash flood occurrences. The findings of this study have led to a more robust understanding of the  spatial and temporal distribution of flash flood during Hurricane Harvey, with the methodology potentially applicable to other events.

### Repository Summary
This repository is devoted to validation of Local Storm Reports and Flash Flood Warnings.  This work is funded as part of the NASA Earth Observations project:
- https://appliedsciences.nasa.gov/content/18-geogfrm-0006

### Core Functionality

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
Funtionality for working with rasters and NetCDFs and asking vector-to-raster spatial containment questions.

