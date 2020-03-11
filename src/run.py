from datetime import datetime
from waze import WazeHandler
from nws import *
from configuration import *
from utils import *


@dataclass
class Extent:
    """Simple extent holder for the analysis"""
    temporal: tuple
    spatial: AbstractGeoHandler


def prep(extent, waze_storm):
    """Prepare the workspace by loading Waze, LSRs, and Warnings and cutting them to the appropriate extent"""
    waze = WazeHandler(waze_storm)
    waze.prep_data()
    waze.clip_temporal(extent.temporal[0], extent.temporal[1])
    # waze.clip_by_shape(extent.spatial.gdf)

    storm_reports = iterative_fetch(extent, LocalStormReportHandler)
    storm_reports.prep_data()
    storm_reports.gdf = storm_reports.gdf.reset_index()
    # storm_reports.clip_by_shape(extent.spatial.gdf)

    storm_warnings = iterative_fetch(extent, StormWarningHandler)
    storm_warnings.prep_data()
    storm_warnings.gdf = storm_reports.gdf.reset_index()
    # storm_warnings.clip_by_shape(extent.spatial.gdf)
    return waze, storm_reports, storm_warnings


start = datetime(2017, 8, 23)
end = datetime(2017, 9, 15)
counties = "/Users/christopherjlowrie/Repos/FlashFloodResponse/data/harvey_misc/harvey_extent.shp"
study_area = AbstractGeoHandler(gdf=gpd.read_file(counties))

e = Extent(
    temporal=(start, end),
    spatial=study_area
)

w, lsrs, sws = prep(e, "Harvey")
print(w.gdf[w.t_field])
# d = lsrs.bivariate_spatial_distance_matrix(waze)
# t = lsrs.bivariate_temporal_distance_matrix(waze)
