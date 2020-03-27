from datetime import datetime
from src.waze import WazeHandler
from src.nws import *
from src.configuration import Extent
from src.utils import *
from src.spacetime.spacetime_analytics import SpaceTimePointStatistics, get_equidistant_dataframe
import numpy as np
import matplotlib.pyplot as plt
import math


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
d0 = lsrs.bivariate_spatial_distance_matrix(w)
t0 = lsrs.bivariate_temporal_distance_matrix(w)
t0 = t0.applymap(lambda x: x.total_seconds())

# Steps:
# n = number of relationships to find
# p = the percentile of relationships to train on
# time_window = the number of seconds to use in finding temporal density
n = 20
p = .05
time_window = 30*60

# 6 hours previously, to 1 hours after
temporal_filter = (-6*60*60, 1*60*60)

# Filter distance matrices to only the points that fit the temporal filter
t = t0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]
d = d0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]

# Find the threshold radius to contain n points
dist = SpaceTimePointStatistics.distance_to_n_points_by_observation(d, n)
spatial_distance_threshold = dist.quantile(p)

# Now identify any slices that are dense within the time periods
threshold_by_point = dist[dist < spatial_distance_threshold]
filtered_distance_matrix = pd.DataFrame(columns=threshold_by_point.index, index=w.gdf.index)
for i in filtered_distance_matrix:
    thresh = threshold_by_point[i]
    print(thresh)
    filtered_distance_matrix[i] = d[i][d[i] < thresh]

# Plotting SpaceTime Cubes
temporal_distance_buffer = []
for i in filtered_distance_matrix:
    import copy
    index = filtered_distance_matrix[i][filtered_distance_matrix[i].apply(lambda x: not np.isnan(x))].index
    w0 = copy.copy(w)
    w0.gdf = w0.gdf.loc[index]
    w0.gdf = get_equidistant_dataframe(w0.gdf)
    x = w0.spacetime_cube()
    l0 = copy.copy(lsrs)
    l0.gdf = get_equidistant_dataframe(l0.gdf)
    l0.gdf = l0.gdf.loc[i]
    l0.add_self_to_spacetime_cube(x)
    SpaceTimePointStatistics.add_reference_circle(
        x,
        threshold_by_point[i],
        l0.gdf.geometry.x, l0.gdf.geometry.y, min(w0.gdf.time).timestamp()
    )
    plt.title("LSR{}".format(i))
    plt.show()
    waze_time_matrix = w0.bivariate_temporal_distance_matrix(w0).applymap(lambda t: abs(t.total_seconds()))
    for waze_report in waze_time_matrix:
        temporal_distance_buffer.append(len(waze_time_matrix[waze_report][waze_time_matrix[waze_report] < time_window]))

temporal_distance_buffer = pd.Series(temporal_distance_buffer)
temporal_distance_threshold = math.floor(temporal_distance_buffer.quantile(1-p))
print(
    "Points, N={n}\n"
    "P-value, p={p}\n"
    "Time Window, {tw} seconds\n"
    "Spatial threshold = {s}\n"
    "Temporal threshold = {t}".format(n=n,
                                      p=p,
                                      tw=time_window,
                                      s=spatial_distance_threshold,
                                      t=temporal_distance_threshold)
)

waze_time_matrix = w.bivariate_temporal_distance_matrix(w).applymap(lambda t: abs(t.total_seconds()))
waze_space_matrix = w.bivariate_spatial_distance_matrix(w)
waze_time_matrix = waze_time_matrix[waze_space_matrix < spatial_distance_threshold]
waze_time_densities = waze_time_matrix[waze_time_matrix < time_window].count()
validated_waze_reports = waze_time_densities[waze_time_densities > temporal_distance_threshold]

w0 = copy.copy(w)
w0.gdf = w0.gdf.loc[validated_waze_reports.index]
x = w0.spacetime_cube()
lsrs.add_self_to_spacetime_cube(x)
plt.show()
