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
from utils import *


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

def get_storm_reports(extent):
    SRH_Buffer = []

    min_datetime, max_datetime = extent["temporal"]

    # TODO implement a reasonable way to determine how long the storm lasted
    for i in range(0, 7):
        d0 = min_datetime + timedelta(days=i)
        d1 = min_datetime + timedelta(days=i + 1)
        SRH = StormReportHandler(d0, d1)
        SRH.cut_data_by_values(STORM_REPORT_FF_KEYS)
        SRH.clip_spatial(extent["spatial"])
        SRH_Buffer.append(SRH)
    merged_SRs = pd.concat([i.gdf for i in SRH_Buffer])
    return StormReportHandler(min_datetime, max_datetime,
                              gdf = merged_SRs)


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

        # TODO Probably not the cleanest way to handle the extents,
        # but no harm and it will work for now
        self.datetime_extent = {
            "temporal": self.waze.get_temporal_extent(as_datetime=True),
            "spatial": self.waze.get_spatial_extent()
        }

        self.storm_reports = get_storm_reports(self.datetime_extent)
        self.storm_reports.prep_data_variables()
        self.storm_warnings = StormWarningHandler(self.datetime_extent["temporal"][0].year)
        self.storm_warnings.prep_data_variables()
        self.clip_context()

    def clip_context(self):
        self.storm_warnings.clip_spatial(self.extent["spatial"])
        self.storm_reports.clip_spatial(self.extent["spatial"])
        self.storm_warnings.clip_temporal(self.extent["temporal"][0], self.extent["temporal"][1])
        self.storm_reports.clip_temporal(self.extent["temporal"][0], self.extent["temporal"][1])

    def write_to_tmp(self):
        self.storm_reports.gdf.to_file(os.path.join(self.environment, "Storm_Reports.shp"))
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

    #TODO implement a write_to_PostGRES function


if __name__ == "__main__":
    for i in ["Michael", "Irma", "Harvey", "Maria"]:
        x = StormDataHolder(i)
        x.plot_space_time_containment()
