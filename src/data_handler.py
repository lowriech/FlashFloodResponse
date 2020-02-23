import geopandas as gpd
import pandas as pd
import numpy as np
import os.path
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
from shapely.geometry import Point
from AbstractXYTFunctionality.AbstractXYTFunc import *
import seaborn as sbn
import matplotlib.pyplot as plt
from configuration import *
from spatial_analytics import *
from utils import *


class NWS:

    def __init__(self):
        pass

    def construct_url(self):
        t0 = str(self.t0.year) + str(self.t0.month).zfill(2) + str(self.t0.day).zfill(2)
        t1 = str(self.t1.year) + str(self.t1.month).zfill(2) + str(self.t1.day).zfill(2)
        base_url = self.base_url.format(t0=t0, t1=t1)

        return base_url

    def construct_local_identifier(self):
        """For constructing a local identifier.  Both for saving remote files,
        and for reducing the amount of remote fetching that has to be done"""

        return str(self.t0.year) + \
               str(self.t0.month).zfill(2) + \
               str(self.t0.day).zfill(2) + \
               str(self.t0.hour).zfill(2) + \
               str(self.t0.minute).zfill(2) + "_" + \
               str(self.t1.year) + \
               str(self.t1.month).zfill(2) + \
               str(self.t1.day).zfill(2) + \
               str(self.t0.hour).zfill(2) + \
               str(self.t0.minute).zfill(2)

    def convert_numeric_to_datetime(self, x):
        """This overwrites the blank function in AbstractTimePointEvent, which
        allows for abstract time functionality"""

        return datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')


class StormWarningHandler(NWS, RemoteDataManager, AbstractTimeDurationEvent):

    def __init__(self, t0, t1, **kwargs):
        self.t0 = t0
        self.t1 = t1
        self.configuration_lookup = "STORM_WARNING"
        self.file_type = ".geojson"
        self.base_url = 'https://mesonet.agron.iastate.edu/geojson/sbw.php?' \
                        'sts={t0}&ets={t1}&wfos='
        NWS.__init__(self)
        RemoteDataManager.__init__(self, **kwargs)
        AbstractTimeDurationEvent.__init__(self, t_start_field="issue", t_end_field="expire")

    def prep_data(self):
        """Called last in the initialization, this handles any adhoc data cleanup that is needed"""

        self.cut_data_by_values({"phenomena": "FF"})
        self.gdf[self.t_start_field] = self.gdf[self.t_start_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )
        self.gdf[self.t_end_field] = self.gdf[self.t_end_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )


class StormReportPointHandler(NWS, RemoteDataManager, AbstractTimePointEvent, SpatialAnalytics):

    def __init__(self, t0, t1, **kwargs):
        self.t0 = t0
        self.t1 = t1
        self.configuration_lookup = "STORM_REPORT_POINT"
        self.file_type = ".geojson"
        self.base_url = "https://mesonet.agron.iastate.edu/geojson/lsr.php?inc_ap=yes&" \
                        "sts={t0}&ets={t1}&wfos="
        NWS.__init__(self)
        RemoteDataManager.__init__(self, **kwargs)
        AbstractTimePointEvent.__init__(self, t_field="valid")

    def prep_data(self):
        """Called last in the initialization, this handles any adhoc data cleanup that is needed"""

        self.cut_data_by_values({"type": "F"})
        self.gdf[self.t_field] = self.gdf[self.t_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )


def iterative_fetch(extent, object, fetch_by = 6):
    storm_reports = []
    min_datetime, max_datetime = extent["temporal"]
    current_time = min_datetime

    while current_time < max_datetime:
        next_time = current_time + timedelta(hours=fetch_by + 1)
        sr = object(current_time, next_time)
        sr.clip_spatial(extent["spatial"])
        storm_reports.append(sr)
        current_time = next_time

    merged_srs = pd.concat([i.gdf for i in storm_reports])
    return object(min_datetime, max_datetime,
                                   gdf=merged_srs)


class WazeHandler(AbstractGeoHandler, AbstractTimePointEvent, SpatialAnalytics):

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

    def prep_data(self):
        self.gdf = self.gdf[self.gdf[self.t_field] != 0]
        self.gdf[self.t_field] = self.gdf[self.t_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )

    def convert_numeric_to_datetime(self, time):
        time = str(time)
        return datetime(int(time[0:4]),
                        int(time[4:6]),
                        int(time[6:8]),
                        int(time[8:10]),
                        int(time[10:12]))


class StormDataHolder:

    def __init__(self, storm, environment=SHP_TMP):
        self.name = storm
        self.environment = environment
        self.waze = WazeHandler(self.name)
        self.extent = {
            "temporal": self.waze.get_temporal_extent(),
            "spatial": self.waze.get_spatial_extent()
        }

        # TODO Probably not the cleanest way to handle the extents, but no harm and it will work for now
        self.spacetime_extent = {
            "temporal": self.waze.get_temporal_extent(as_datetime=True),
            "spatial": self.waze.get_spatial_extent()
        }

        self.storm_reports = StormReportPointHandler(self.spacetime_extent["temporal"][0], self.spacetime_extent["temporal"][1])
        self.storm_warnings = StormWarningHandler(self.spacetime_extent["temporal"][0].year)
        self.storm_warnings.prep_data_variables()
        self.clip_context()

    def clip_context(self):
        self.storm_warnings.clip_spatial(self.extent["spatial"])
        # self.storm_reports.clip_spatial(self.extent["spatial"])
        self.storm_warnings.clip_temporal(self.extent["temporal"][0], self.extent["temporal"][1])
        # self.storm_reports.clip_temporal(self.extent["temporal"][0], self.extent["temporal"][1])

    def write_to_tmp(self):
        # self.storm_reports.gdf.to_file(os.path.join(self.environment, "Storm_Reports.shp"))
        self.storm_warnings.gdf.to_file(os.path.join(self.environment, "Storm_Warnings.shp"))
        self.waze.gdf.to_file(os.path.join(self.environment, self.name + "_waze.shp"))

    def plot_space_time_containment(self):
        #TODO this is temporary, eventually the view creation will exist within one place, and plotting functionality will exist in another
        # Create the Background of the map
        background = AbstractGeoHandler(local_shp_path=ZCTA_PATH)
        background.clip_spatial(self.extent["spatial"])
        background_fields = pd.read_csv(
            "/Users/christopherjlowrie/Repos/FlashFloodResponse/data/us_census/zcta5/resources/poverty/ACS_17_5YR_B17003_with_ann.csv")
        background_fields["PovertyPerc"] = background_fields['Estimate; Income in the past 12 months below poverty level:'] / background_fields['Estimate; Total:']
        background_fields = background_fields[['Id2', 'PovertyPerc']]
        background_fields = background_fields.set_index('Id2')
        background = background.gdf.set_index("ZCTA5CE10")
        background.index = background.index.astype("int")
        background = background.merge(background_fields, left_index=True, right_index=True, how="left")
        waze_coverage = AbstractSpaceTimeFunctionality.get_distinct_points_by_space_time_coverage(self.waze, self.storm_warnings)

        # Plot the distinct points
        fig = plt.figure()
        ax1 = plt.subplot(2, 1, 1)
        ax2 = plt.subplot(2, 1, 2)
        ax1 = background.plot(ax=ax1, color='white', edgecolor='grey', linewidth=0.1)
        waze_coverage[waze_coverage["has_overlap"] == 1].plot(ax=ax1, markersize=0.5, color="black")
        ax1.set_title(
            "Covered by NWS Warnings, n={}".format(str(len(waze_coverage[waze_coverage["has_overlap"] == 1]))))
        ax2 = background.plot(ax=ax2, color='white', edgecolor='grey', linewidth=0.1)
        waze_coverage[waze_coverage["has_overlap"] == 0].plot(ax=ax2, markersize=0.5, color="red")
        fig.suptitle('Hurricane {}, Waze App Reports\nby Poverty Rates'.format(self.name))
        ax2.set_title(
            "Not Covered by NWS Warnings, n={}".format(str(len(waze_coverage[waze_coverage["has_overlap"] == 0]))))
        plt.show()


# class StormReportPolygonHandler(NWSHandler):
#   in case there's ever a reason to implement
#     base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/watchwarn.py?" \
#                    "&{t0}&{t1}".format(t0=t0, t1=t1)





# if __name__ == "__main__":
#     for i in ["Michael", "Irma", "Harvey", "Maria"]:
#         x = StormDataHolder(i)
#         x.plot_space_time_containment()
