import geopandas as gpd
import pandas as pd
import numpy as np
import os.path
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
from shapely.geometry import Point

import seaborn as sbn
import matplotlib.pyplot as plt

from configuration import *
from utils import *


STORM_REPORT_FF_KEYS = {"PHENOM": "FF"}


class AbstractTimePointEvent:

    def __init__(self, t_field):
        self.t_field = t_field

    def clip_temporal(self, t0, t1):
        self.gdf = self.gdf[self.gdf[self.t_field] < t1][self.gdf[self.t_field] > t0]

    def get_temporal_extent(self, as_datetime = False):
        min_time = min(self.gdf[self.gdf[self.t_field] != 0][self.t_field])
        max_time = max(self.gdf[self.gdf[self.t_field] != 0][self.t_field])
        if as_datetime:
            return convert_numeric_to_datetime(min_time), \
                   convert_numeric_to_datetime(max_time)
        else:
            return min_time, max_time


class AbstractTimeDurationEvent:

    def __init__(self, t_start_field, t_end_field):
        self.t_start_field = t_start_field
        self.t_end_field = t_end_field

    def clip_temporal(self, t0, t1):
        self.gdf = self.gdf[self.gdf[self.t_start_field] < t1][self.gdf[self.t_end_field] > t0]

    def get_temporal_extent(self, as_datetime = False):
        min_time = min(self.gdf[self.gdf[self.t_start_field] != 0][self.t_start_field])
        max_time = max(self.gdf[self.gdf[self.t_end_field] != 0][self.t_end_field])
        if as_datetime:
            return convert_numeric_to_datetime(min_time), \
                   convert_numeric_to_datetime(max_time)
        else:
            return min_time, max_time


class AbstractGeoHandler:
    '''A parent handler for geospatial dataframes.
    This is intended to handle basic geodataframe operations.'''
    def __init__(self, **kwargs):
        if "local_shp_path" in kwargs:
            self.local_shp_path = kwargs.get("local_shp_path")
        else:
            self.local_shp_path = None
        self.get_gdf()

    def get_gdf(self):
        '''Search for the GDF locally, if not found look for a remote file.'''
        if self.local_shp_path is not None:
            self.gdf = gpd.read_file(self.local_shp_path)
        else:
            self.get_local_data()
            if self.gdf is None:
                self.get_remote_shp()

    def cut_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        '''Filter a dataframe by specific values'''
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        self.gdf = x

    def create_spatial_index_fields(self):
        '''Potentially useful for spatial indexing and efficient calculation'''
        bounds = self.gdf["geometry"].bounds
        self.gdf["minx"] = bounds["minx"]
        self.gdf["maxx"] = bounds["maxx"]
        self.gdf["miny"] = bounds["miny"]
        self.gdf["maxy"] = bounds["maxy"]

    def cut_to_extent(self, extent):
        #TODO: try/except is currently implemented to handle empty dataframes.  Not optimal
        '''Takes an extent (lower left, upper right) and clips the GDF to these bounds'''
        lower_left, upper_right = extent
        extent_min_lon, extent_min_lat = lower_left
        extent_max_lon, extent_max_lat = upper_right
        try:
            c1 = self.gdf["geometry"].bounds["minx"] < extent_max_lon
            c2 = self.gdf["geometry"].bounds["maxx"] > extent_min_lon
            c3 = self.gdf["geometry"].bounds["miny"] < extent_max_lat
            c4 = self.gdf["geometry"].bounds["maxy"] > extent_min_lat
            self.gdf = self.gdf[c1][c2][c3][c4]
        except ValueError:
            pass

    def get_spatial_extent(self):
        self.create_spatial_index_fields()
        return ((min(self.gdf["minx"]), min(self.gdf["miny"])),
                (max(self.gdf["maxx"]), max(self.gdf["maxy"])))


class RemoteDataManager(AbstractGeoHandler):

    def __init__(self, **kwargs):
        self.dir = os.path.join(DATA_DIRS[self.data_type], self.construct_identifier())
        super(RemoteDataManager, self).__init__(**kwargs)

    def get_local_data(self):
        '''Look for data in local file system'''
        tentative_shp = get_dotshp_from_shpdir(self.dir)
        try:
            self.local_shp_path = tentative_shp
            print("Found local @ {}".format(tentative_shp))
            self.gdf = gpd.read_file(tentative_shp)
        except:
            print("Local not found")
            return None

    def get_remote_shp(self):
        '''Find a remote shapefile.
        Must have implementations in a child class'''
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)

        zip_name = self.construct_identifier() + ".zip"
        zip_file_path = os.path.join(TMP_DIR, zip_name)

        with open(zip_file_path, "wb") as file:
            r = requests.get(self.construct_remote_shp_path())
            file.write(r.content)

        z = ZipFile(zip_file_path)
        z.extractall(self.dir)
        self.local_shp_path = get_dotshp_from_shpdir(self.dir)
        self.gdf = gpd.read_file(self.local_shp_path)


class NWSHandler(RemoteDataManager, AbstractTimeDurationEvent):
    '''For processing GDFs related to the National Weather Service, via the Iowa Env Mesonet'''
    def __init__(self, **kwargs):
        RemoteDataManager.__init__(self, **kwargs)
        AbstractTimeDurationEvent.__init__(self, t_start_field="ISSUED", t_end_field="EXPIRED")

    def prep_data_variables(self):
        self.gdf["ISSUED"] = self.gdf["ISSUED"].astype('int')
        self.gdf["EXPIRED"] = self.gdf["EXPIRED"].astype('int')
        self.gdf.to_crs({'init': 'epsg:4326'})


class StormWarningHandler(NWSHandler):

    def __init__(self, year, **kwargs):
        self.year = year
        self.data_type = "STORM_WARNING"
        super(StormWarningHandler, self).__init__(**kwargs)

    def construct_identifier(self):
        return str(self.year)

    def construct_remote_shp_path(self):
        return "https://mesonet.agron.iastate.edu/pickup/wwa/{}_tsmf_sbw.zip".format(str(self.year))


class StormReportHandler(NWSHandler):

    def __init__(self, t0, t1, **kwargs):
        self.t0 = t0
        self.t1 = t1
        self.data_type = "STORM_REPORT"
        super(StormReportHandler, self).__init__(**kwargs)

    def construct_remote_shp_path(self):
        t0 = "year1={year1}&" \
             "month1={month1}&" \
             "day1={day1}&" \
             "hour1={hour1}&" \
             "minute1={minute1}".format(year1=self.t0.year,
                                        month1=self.t0.month,
                                        day1=self.t0.day,
                                        hour1=self.t0.hour,
                                        minute1=self.t0.minute)

        t1 = "year2={year2}&" \
             "month2={month2}&" \
             "day2={day2}&" \
             "hour2={hour2}&" \
             "minute2={minute2}".format(year2=self.t1.year,
                                        month2=self.t1.month,
                                        day2=self.t1.day,
                                        hour2=self.t1.hour,
                                        minute2=self.t1.minute)


        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/watchwarn.py?" \
                   "&{t0}&{t1}".format(t0=t0, t1=t1)

        # base_url_point gives a point that may be more useful than the polygon, but need to investigate
        # base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/lsr.py?wfo%5B%5D=ALL" \
        #            "&{t1}&{t2}".format(t0=t0, t1=t1)

        return base_url

    def construct_identifier(self):
        return str(self.t0.year) + str(self.t0.month) + str(self.t0.day) + "_" + \
               str(self.t1.year) + str(self.t1.month) + str(self.t1.day)


# TODO: Implement StormReportPointHandler
# base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/lsr.py?wfo%5B%5D=ALL" \

# TODO: Abstract this to not depend on Waze, just on space-time extent
def get_storm_reports(waze):
    SRH_Buffer = []

    min_datetime, max_datetime = waze.get_min_max_times(as_datetime = True)

    # TODO implement a reasonable way to determine how long the storm lasted
    for i in range(0, 7):
        d0 = min_datetime + timedelta(days=i)
        d1 = min_datetime + timedelta(days=i + 1)
        SRH = StormReportHandler(d0, d1)
        SRH.cut_data_by_values()
        SRH.cut_to_extent(waze.get_extent())
        SRH_Buffer.append(SRH)
    out_path = os.path.join(SHP_TMP, "Storm_Reports.shp")
    merged_SRs = pd.concat([i.gdf for i in SRH_Buffer])
    merged_SRs.to_file(out_path)
    return StormReportHandler(min_datetime, max_datetime,
                              local_shp_path = out_path)


class WazeHandler(AbstractGeoHandler, AbstractTimePointEvent):

    def __init__(self, event_name):
        self.event_name = event_name
        AbstractGeoHandler.__init__(self)
        AbstractTimePointEvent.__init__(self, t_field="time")

    def get_gdf(self):
        csv = os.path.join(WAZE_DIR, "waze_" + self.event_name + ".txt")
        df = pd.read_csv(csv)

        geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
        crs = {'init': 'epsg:4326'}  # http://www.spatialreference.org/ref/epsg/2263/
        g = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        g["time"] = g["time"]//100
        self.gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)


class AbstractSpaceTimeFunctionality:
    '''All functions currently return GDFs, as opposed to handlers.
    This won't fundamentally block anything, but might get confusing.'''
    def __init__(self):
        pass

    @staticmethod
    def space_time_containment(time_point_handler, time_duration_handler):
        '''Returns the spatial intersection, and whether or not each spatial intersection
        is space-time contained or not in the "time_overlap" field'''
        # For convenience and ease of reading.  These come from AbstractTimeDurationEvent
        t0 = time_duration_handler.t_start_field
        t1 = time_duration_handler.t_end_field
        t = time_point_handler.t_field

        # Create a spatial intersection
        spatial_intersection = gpd.sjoin(time_point_handler.gdf, time_duration_handler.gdf, how="left", op="intersects")

        # For points that had no spatial containment, set those fields to -1.  t_start_field
        spatial_intersection[t0] = \
            np.where(spatial_intersection[t0].isnull(),
                     -1,
                     spatial_intersection[t0])

        # For points that had no spatial containment, set those fields to -1.  t_end_field
        spatial_intersection[t1] = \
            np.where(spatial_intersection[t1].isnull(),
                     -1,
                     spatial_intersection[t1])

        # Create boolean field, time_overlap, for whether each point is xyt contained
        spatial_intersection["time_overlap"] = np.where(
            (spatial_intersection[t0] < spatial_intersection[t]) &
            (spatial_intersection[t] < spatial_intersection[t1]), 1,
            0)

        return spatial_intersection

    @staticmethod
    def get_distinct_points_by_space_time_coverage(time_point_handler, time_duration_handler):
        '''Returns whether or not each point has any containing duration event,
        in the "has_overlap" column'''
        z = AbstractSpaceTimeFunctionality.space_time_containment(time_point_handler, time_duration_handler)
        z = z["time_overlap"].groupby(by=z.index).max().reset_index(name='has_overlap')[["has_overlap"]]
        return time_point_handler.gdf.join(z)

    @staticmethod
    def count_points_per_geography(polygon_handler, point_handler, collect_on = None):
        '''Count the number of points contained per polygonal geography.
        Also returns a collected list of a field, if specified.'''
        z = gpd.sjoin(polygon_handler.gdf, point_handler.gdf, how="left", op="intersects")
        z_counts = z["index_right"].groupby(by=z.index).count().reset_index(name="count")[["count"]]
        if collect_on is not None:
            z_collect = z[collect_on].groupby(by=z.index).apply(list).reset_index(name="collection")[["collection"]]
            z_counts = z_counts.join(z_collect)

        return polygon_handler.gdf.join(z_counts)


class AbstractOutputsAndGraphs:

    def __init__(self):
        pass

    @staticmethod
    def plot_kde(values, **kwargs):
        sbn.set_style('darkgrid')
        sbn.distplot(values)
        plt.show()

    def plot_categorical_geo_data(self, data, category_column):
        pass


# TODO: Abstract DataHolder, into more of a generic analysis system
# DataHolder should be completely independent of any particular handlers, etc.
class DataHolder:

    def __init__(self, storm, environment=SHP_TMP, geography_path=ZCTA_PATH):
        self.name = storm
        self.environment = environment

        # TODO: Currently this is hardcoded for one particular census shapefile.
        #  Need to make it abstract so that you initiate based on particular columns
        self.geographies = AbstractGeoHandler(local_shp_path=geography_path)

        self.waze = WazeHandler(self.name)
        self.extent = {
            "temporal": self.waze.get_min_max_times(),
            "spatial": self.waze.get_extent()
        }

        self.storm_reports = get_storm_reports(self.waze)
        self.storm_reports.prep_data_variables()
        t = self.waze.get_min_max_times(as_datetime = True)[0]
        self.storm_warnings = StormWarningHandler(t.year)
        self.storm_warnings.prep_data_variables()
        self.clip_context()

    def clip_context(self):
        self.storm_warnings.cut_to_extent(self.extent["spatial"])
        self.storm_reports.cut_to_extent(self.extent["spatial"])
        self.geographies.cut_to_extent(self.extent["spatial"])
        self.storm_warnings.clip_temporal(self.extent["temporal"][0], self.extent["temporal"][1])
        self.storm_reports.clip_temporal(self.extent["temporal"][0], self.extent["temporal"][1])

    def write_to_tmp(self):
        self.storm_reports.gdf.to_file(os.path.join(self.environment, "Storm_Reports.shp"))
        self.storm_warnings.gdf.to_file(os.path.join(self.environment, "Storm_Warnings.shp"))
        self.waze.gdf.to_file(os.path.join(self.environment, self.name + "_waze.shp"))

    def get_waze_vs_warnings_view1(self):
        waze_warnings = gpd.sjoin(self.waze.gdf, self.storm_warnings.gdf, how="left", op="intersects")
        waze_warnings["ISSUED"] = np.where(waze_warnings["ISSUED"].isnull(), -1, waze_warnings["ISSUED"])
        waze_warnings["EXPIRED"] = np.where(waze_warnings["EXPIRED"].isnull(), -1, waze_warnings["EXPIRED"])
        waze_warnings["time_overlap"] = np.where(
            (waze_warnings["ISSUED"] < waze_warnings["time"]) & (waze_warnings["time"] < waze_warnings["EXPIRED"]), 1,
            0)
        return waze_warnings

    def get_waze_points_and_coverage(self):
        x = self.get_waze_vs_warnings_view1()
        y = x.groupby(by=['lat', 'lon', 'time'])['time_overlap'].max().reset_index(name='has_overlap')
        # Geopandas doesn't support groupby in a great way, so currently need to re-transform to GDF\
        y = gpd.GeoDataFrame(
            y, geometry=gpd.points_from_xy(y.lon, y.lat))
        y.to_file(os.path.join(self.environment, self.name + "_waze_vs_warning.shp"))
        return y

    def get_geographies_and_waze_times(self):
        return gpd.sjoin(self.geographies.gdf, self.get_waze_points_and_coverage(), how="left", op="intersects")

    def construct_zcta_summary(self):
        y = self.get_geographies_and_waze_times()
        pov = self.geographies.gdf[["ZCTA5", "PercPov"]].set_index("ZCTA5")
        x = y.groupby('ZCTA5')['time']
        x_counts = x.count().reset_index(name="count")["count"]
        x = x.apply(list).reset_index(name="times")
        x["counts"] = x_counts
        x = x.set_index("ZCTA5")
        x = x.merge(pov, left_index=True, right_index=True)
        return x

    def plot_time_density_for_zcta(self, zcta):
        zcta = str(zcta)
        x = self.get_geographies_and_waze_times().groupby('ZCTA5')['time'].apply(list).reset_index(name='times')
        times = x[x["ZCTA5"] == zcta]['times'].values[0]
        sbn.set_style('darkgrid')
        sbn.distplot(times)
        plt.show()


# waze = WazeHandler("Harvey")
# warn = StormWarningHandler(2017)
# warn.prep_data_variables()
# poly = AbstractGeoHandler(local_shp_path=ZCTA_PATH)
#
# z1 = AbstractSpaceTimeFunctionality.space_time_containment(waze, warn)
# print(z1)
#
# z2 = AbstractSpaceTimeFunctionality.count_points_per_geography(poly, waze)
# print(z2)
#
# z3 = AbstractSpaceTimeFunctionality.count_points_per_geography(poly, waze, collect_on="time")
# print(z3)

#
# for i in z3.sort_values(by="count", ascending = False).iterrows():
# 	vals = i[1]["collection"]
# 	AbstractOutputsAndGraphs.plot_kde(vals)
#
