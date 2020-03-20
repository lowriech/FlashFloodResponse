from datetime import datetime
from src.waze import WazeHandler
from src.nws import *
from src.configuration import Extent
from src.utils import *
from src.spacetime.spacetime_analytics import SpaceTimePointStatistics
import numpy as np
import matplotlib.pyplot as plt


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
n = 20
p = .05

# 6 hours previously, to 1 hours after
temporal_filter = (-6*60*60, 1*60*60)

# Filter distance matrices to only the points that fit the temporal filter
t = t0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]
d = d0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]

# Find the threshold radius to contain n points
dist = SpaceTimePointStatistics.distance_to_n_points_by_observation(d, n)
threshold = dist.quantile(p)
print("Points, N={n}\nP-value, p={p}\nYields a threshold distance of {t}".format(n=n, p=p, t=threshold))

# Now identify any slices that are dense within the time periods
threshold_by_point = dist[dist < threshold]
filtered_distance_matrix = pd.DataFrame(columns=threshold_by_point.index, index=w.gdf.index)
for i in filtered_distance_matrix:
    thresh = threshold_by_point[i]
    print(thresh)
    filtered_distance_matrix[i] = d[i][d[i] < thresh]

# Plotting SpaceTime Cubes
for i in filtered_distance_matrix:
    import copy
    index = filtered_distance_matrix[i][filtered_distance_matrix[i].apply(lambda x: not np.isnan(x))].index
    w0 = copy.copy(w)
    w0.gdf = w0.gdf.loc[index]
    x = w0.spacetime_cube()
    l0 = copy.copy(lsrs)
    l0.gdf = l0.gdf.loc[i]
    l0.add_self_to_spacetime_cube(x)
    plt.show()

# For each LSR in filtered_distance_matrix
# -- For each Waze report, find it's density at the 15, 30, 60 minute slice
# Create a distribution of the time density.
# Select the p value
