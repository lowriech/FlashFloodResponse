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
    #LSRS
    storm_reports = iterative_fetch(extent, LocalStormReportHandler)
    storm_reports.prep_data()
    storm_reports.gdf = storm_reports.gdf.reset_index()
    storm_reports.clip_by_shape(extent.spatial.gdf)
    #SWs
    # storm_warnings = iterative_fetch(extent, StormWarningHandler)
    # storm_warnings.prep_data()
    # storm_warnings.gdf = storm_reports.gdf.reset_index()
    # storm_warnings.clip_by_shape(extent.spatial.gdf)
    return waze, storm_reports


harvey_extent = Extent(
    temporal=(datetime(2017, 8, 23), datetime(2017, 9, 15)),
    spatial=AbstractGeoHandler(
        gdf=gpd.read_file("/Users/christopherjlowrie/Repos/FlashFloodResponse/data/harvey_misc/harvey_extent.shp")
    )
)

w, lsrs = prep(harvey_extent, "Harvey")

print(lsrs)
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
t = t0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]
d = d0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]


def plot_histogram():
    l00 = pd.concat([lsrs.gdf] * 10)
    plt.hist(w.gdf.time, bins=100)
    x2 = plt.hist(l00.valid, bins=70)
    plt.title("Histogram of Waze and LSR reports")
    plt.xticks(rotation=45)
    plt.show()


def calculate_spatial_kde(n, temporal_filter=temporal_filter):
    # Filter distance matrices to only the points that fit the temporal filter
    d = d0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]
    # Find the threshold radius to contain n points
    dist = SpaceTimePointStatistics.distance_to_n_points_by_observation(d, n)
    dist.hist(bins=30)
    plt.title("Histogram of Spatial Radii for inclusion of {} points".format(str(n)))
    plt.show()

    from sklearn.neighbors import KernelDensity
    x_d = np.linspace(0, 250000, 50000)
    # instantiate and fit the KDE model
    kde = KernelDensity(bandwidth=10000, kernel='gaussian')
    kde.fit(dist[:, None])

    # score_samples returns the log of the probability density
    logprob = kde.score_samples(x_d[:, None])

    plt.fill_between(x_d, np.exp(logprob), alpha=0.5)
    plt.plot(dist, np.full_like(dist, -0.000001), '|k', markeredgewidth=1)
    plt.ylim(-0.000002, 0.000015)
    plt.title("Kernel Density Estimate of Spatial Radii for inclusion of {} points".format(str(n)))
    plt.show()


def calculate_temporal_kde(waze_times, temporal_filter=30*60):
    # Filter distance matrices to only the points that fit the temporal filter
    from sklearn.neighbors import KernelDensity
    x_d = np.linspace(0, 6*60*60, 60*60)
    # instantiate and fit the KDE model
    kde = KernelDensity(bandwidth=temporal_filter, kernel='tophat')
    kde.fit(waze_times[:, None])
    # score_samples returns the log of the probability density
    logprob = kde.score_samples(x_d[:, None])
    plt.fill_between(x_d, np.exp(logprob), alpha=0.5)
    plt.plot(waze_times, np.full_like(waze_times, -0.000001), '|k', markeredgewidth=1)
    plt.axvline(1800)
    plt.ylim(-0.000002, 0.00025)
    plt.title("Kernel Density Estimate of Spatial Radii for inclusion of {} points".format(str(n)))
    plt.show()


def plot_waze_timecube(w0, threshold_by_point):
    x = w0.spacetime_cube()
    l0 = copy.copy(lsrs)
    l0.gdf = get_equidistant_dataframe(l0.gdf)
    l0.gdf = l0.gdf.loc[i]
    l0.add_self_to_spacetime_cube(x)
    SpaceTimePointStatistics.add_reference_circle(
        x,
        threshold_by_point[i],
        l0.gdf.geometry.x,
        l0.gdf.geometry.y,
        min(w0.gdf.time).timestamp()
    )
    plt.title("LSR{}".format(i))
    plt.show()

# plot_histogram()

# for n in (10,20,30):
#     calculate_spatial_kde(n)


def main(n, p, time_window=30*60, temporal_filter=(-6*60*60, 1*60*60)):
    # Filter distance matrices to only the points that fit the temporal filter
    t = t0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]
    d = d0[t0 > temporal_filter[0]][t0 < temporal_filter[1]]
    # Find the threshold radius to contain n points
    dist = SpaceTimePointStatistics.distance_to_n_points_by_observation(d, n)
    print(sorted(dist))
    print(len(dist))
    spatial_distance_threshold = dist.quantile(p)
    print("Dist Thresh:", str(spatial_distance_threshold))
    # Now identify any slices that are dense within the time periods
    threshold_by_point = dist[dist < spatial_distance_threshold]
    print(len(threshold_by_point))
    print(threshold_by_point.index)
    filtered_distance_matrix = pd.DataFrame(columns=threshold_by_point.index, index=w.gdf.index)
    for i in filtered_distance_matrix:
        thresh = threshold_by_point[i]
        # print(thresh)
        filtered_distance_matrix[i] = d[i][d[i] < thresh]
    # Plotting SpaceTime Cubes
    temporal_distance_buffer = []
    print(n)
    for i in filtered_distance_matrix:
        print(i)
    for i in filtered_distance_matrix:
        import copy
        index = filtered_distance_matrix[i][filtered_distance_matrix[i].apply(lambda x: not np.isnan(x))].index
        w0 = copy.copy(w)
        w0.gdf = w0.gdf.loc[index]
        w0.gdf = get_equidistant_dataframe(w0.gdf)
        waze_time_matrix = w0.bivariate_temporal_distance_matrix(w0).applymap(lambda t: abs(t.total_seconds()))
        print(waze_time_matrix)
        for waze_report in waze_time_matrix:
            # calculate_temporal_kde(waze_time_matrix[waze_report], time_window)
            temporal_distance_buffer.append(len(waze_time_matrix[waze_report][waze_time_matrix[waze_report] < time_window]))
    temporal_distance_buffer = pd.Series(temporal_distance_buffer)
    plt.hist(temporal_distance_buffer, bins=[i-0.5 for i in range(n+1)])
    plt.title("Histogram of Temporal Clustering; N={}".format(n))
    plt.xticks([i for i in range(n+1)])
    plt.show()
    temporal_distance_threshold = math.floor(temporal_distance_buffer.quantile(1-p))
    return (spatial_distance_threshold,
            temporal_distance_threshold) #temporal_distance_buffer



x = dict()
for n in range(10, 31, 10):
    for p in (0.05,):
        try:
            x[(n, p)] = main(n, p)
        except:
            pass


print("N, p, S, T")
for k,v in x.items():
    print(k[0], k[1], v[0], v[1])



waze_time_matrix = w.bivariate_temporal_distance_matrix(w).applymap(lambda t: abs(t.total_seconds()))
waze_space_matrix = w.bivariate_spatial_distance_matrix(w)

def validate_waze_reports(spatial_distance_threshold, temporal_distance_threshold, n):
    wtm2 = copy.copy(waze_time_matrix)
    wtm2 = wtm2[waze_space_matrix < spatial_distance_threshold]
    waze_time_densities = wtm2[wtm2 < time_window].count()
    validated_waze_reports = waze_time_densities[waze_time_densities > temporal_distance_threshold]
    print(len(validated_waze_reports))
    w0 = copy.copy(w)
    w0.gdf = w0.gdf.loc[validated_waze_reports.index]
    x1 = w0.spacetime_cube()
    x1.set_zticklabels([datetime.fromtimestamp(int(i)).strftime(format="%m/%d - %H:%M") for i in x1.get_zticks()],
                       ha="left")
    plt.title("Virtual Waze Reports; N={}".format(n))
    plt.show()
    return list(validated_waze_reports.index)


import copy
y = []
for k, v in x.items():
    spatial_distance_threshold, temporal_distance_threshold = v
    vwr = validate_waze_reports(spatial_distance_threshold, temporal_distance_threshold, k[0])
    y += vwr

y2 = pd.Series(y)
y3 = y2.groupby(lambda x: y2[x]).count()
y4 = y3.groupby(lambda x: y3[x]).count()
y5 = dict()
for i in y4.index:
    y5[i] = y4[y4.index <= i].sum()

y6 = copy.copy(y4)
y6.index = abs(y6.index-15)

w0 = copy.copy(w)
w0.gdf = w0.gdf.loc[y3[y3==3].index]
x1 = w0.spacetime_cube()
x1.set_zticklabels([datetime.fromtimestamp(int(i)).strftime(format="%m/%d - %H:%M") for i in x1.get_zticks()],
                   ha="left")
plt.title("Virtual Waze Reports supported by 3 N-configurations".format(n))
plt.show()


y7 = dict()
for i in y6.index:
    y7[i] = y6[y6.index <= i].sum()

#
#
#
lt = lsrs.bivariate_temporal_distance_matrix(lsrs)
lt = lt.applymap(lambda x: x.total_seconds())
ld = lsrs.bivariate_spatial_distance_matrix(lsrs)
lt = lt[lt > temporal_filter[0]][lt < temporal_filter[1]]
ld = ld[lt > temporal_filter[0]][lt < temporal_filter[1]]
limit_to_be_new = ld[ld!= 0].min().quantile(0.5)
#

a = []
for i in range(1, 15):
    w0 = copy.copy(w)
    w0.gdf = w0.gdf.loc[y3[y3 >= i].index]
    min_d = d.transpose().min().rename("min_d")
    min_d[min_d.isnull()] = 1000000
    w0.gdf = w0.gdf.join(min_d)
    w0.gdf = w0.gdf[w0.gdf.min_d > limit_to_be_new]
    a.append(w0.gdf.shape[0])


for i in a[::-1]:
    print(i)


for i in ("filtered",):
    for j in ("3D",):
        for k in (2, 3):
            w0 = copy.copy(w)
            w0.gdf = w0.gdf.loc[y3[y3 == k].index]
            min_d = d.transpose().min().rename("min_d")
            min_d[min_d.isnull()] = 1000000
            w0.gdf = w0.gdf.join(min_d)
            if i == "filtered":
                w0.gdf = w0.gdf[w0.gdf.min_d > limit_to_be_new]
            print(w0.gdf.shape[0])
            x = w0.spacetime_cube()
            lsrs.add_self_to_spacetime_cube(x)
            x.set_zticklabels([datetime.fromtimestamp(int(i)).strftime(format="%m/%d - %H:%M") for i in x.get_zticks()],
                              ha="left")
            x.set_zlabel("")
            if j == "Y":
                x.axes.xaxis.set_ticklabels([])
                plt.xlabel("")
            elif j == "X":
                x.axes.yaxis.set_ticklabels([])
                plt.xlabel("")
            plt.title("Virtual Waze Reports after De-Duplication; Supported by {} Configurations".format(k))
            plt.show()
#
#
# # Plot Spatial Distances
# sorted_dist = pd.Series(sorted(dist))
# plt.plot(sorted_dist, sorted_dist.index)
# plt.title("Cumulative Density Function of Spatial Relationship\nBetween Waze and LSRs")
# plt.show()
#
# dist.hist(bins=30)
# plt.title("Histogram of Spatial Relationship\nBetween Waze and LSRs")
# plt.show()

lt = lsrs.bivariate_temporal_distance_matrix(lsrs)
lt = lt.applymap(lambda x: x.total_seconds())
ld = lsrs.bivariate_spatial_distance_matrix(lsrs)
t = lt[lt > temporal_filter[0]][lt < temporal_filter[1]]
d = ld[lt > temporal_filter[0]][lt < temporal_filter[1]]
limit_to_be_new = d[d!= 0].min().quantile(0.5)


x1 = w.spacetime_cube()
# lsrs.add_self_to_spacetime_cube(x1)
x1.set_zticklabels([datetime.fromtimestamp(int(i)).strftime(format="%m/%d - %H:%M") for i in x1.get_zticks()],
                          ha="left")
plt.title("All Waze Reports")
plt.show()

x1 = lsrs.spacetime_cube()
x1.set_zticklabels([datetime.fromtimestamp(int(i)).strftime(format="%m/%d - %H:%M") for i in x1.get_zticks()],
                          ha="left")
plt.show()


