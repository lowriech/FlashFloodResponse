from datetime import datetime
from src.waze import WazeHandler
from src.nws import *
from src.configuration import Extent
from src.utils import *
from src.spacetime.spatial_analytics import SpaceTimePointStatistics


def prep(extent, waze_storm):
    """Prepare the workspace by loading Waze, LSRs, and Warnings and cutting them to the appropriate extent"""
    waze = WazeHandler(waze_storm)
    waze.prep_data()
    waze.clip_temporal(extent.temporal[0], extent.temporal[1])
    waze.clip_by_shape(extent.spatial.gdf)

    storm_reports = iterative_fetch(extent, LocalStormReportHandler)
    storm_reports.prep_data()
    storm_reports.gdf = storm_reports.gdf.reset_index()
    storm_reports.clip_by_shape(extent.spatial.gdf)

    storm_warnings = iterative_fetch(extent, StormWarningHandler)
    storm_warnings.prep_data()
    storm_warnings.gdf = storm_reports.gdf.reset_index()
    storm_warnings.clip_by_shape(extent.spatial.gdf)
    return waze, storm_reports, storm_warnings


harvey_extent = Extent(
    temporal=(datetime(2017, 8, 23), datetime(2017, 9, 15)),
    spatial=AbstractGeoHandler(
        gdf=gpd.read_file("/Users/christopherjlowrie/Repos/FlashFloodResponse/data/harvey_misc/harvey_extent.shp")
    )
)

w, lsrs, sws = prep(harvey_extent, "Harvey")
t = lsrs.bivariate_temporal_distance_matrix(w)
d = lsrs.bivariate_spatial_distance_matrix(w)
d = lsrs.bivariate_temporal_distance_matrix(w)
t = t.applymap(lambda x: x.total_seconds())

# Steps:
# 1. Filter by distance (n points and p percentile)

n = 10
p = .1
dist = SpaceTimePointStatistics.distance_to_n_points_by_observation(d, n)
threshold = dist.quantile(p)
threshold_by_point = dist[dist < threshold]

x = pd.DataFrame(columns=threshold_by_point.index, index=w.gdf.index)
for i in x:
    thresh = threshold_by_point[i]
    print(thresh)
    x[i] = d[i][d[i] < thresh]
