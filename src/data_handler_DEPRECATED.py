import geopandas as gpd
import pandas as pd
import os.path
from os import walk
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
from configuration import *
from shapely.geometry import Point
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from utils import *

STORM_REPORT_FF_KEYS = {"PHENOM": "FF"}


class AbstractHandler:
    def __init__(self, local_shp_path = None):
        self.local_shp_path = local_shp_path
        self.gdf = self.get_gdf()
        self.dir = os.path.join(DATA_DIRS[self.data_type], self.construct_identifier())

    def get_gdf(self):
        if self.local_shp_path is not None:
            self.gdf = gpd.read_file(self.local_shp_path)
        else:
            if self.get_local_data() is None:
                self.gdf = gpd.read_file(self.get_remote_shp())

    def cut_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        self.gdf = x

    def get_local_data(self):
        tentative_shp = get_dotshp_from_shpdir(self.dir)
        try:
            self.local_shp_path = tentative_shp
            print("Found local @ {}".format(tentative_shp))
            self.gdf = gpd.read_file(tentative_shp)
        except:
            print("Local not found")
            return None

    def create_spatial_index_fields(self):
        bounds = self.gdf["geometry"].bounds
        self.gdf["minx"] = bounds["minx"]
        self.gdf["maxx"] = bounds["maxx"]
        self.gdf["miny"] = bounds["miny"]
        self.gdf["maxy"] = bounds["maxy"]

    def cut_to_extent(self, extent):
        #TODO: try/except is currently implemented to handle empty dataframes.  Not optimal
        # ((lower_left), (upper_right))
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

    def get_remote_shp(self):
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)

        zip_file_path = os.path.join(TMP_DIR, id + ".zip")

        with open(zip_file_path, "wb") as file:
            r = requests.get(self.construct_remote_shp_path())
            file.write(r.content)

        z = ZipFile(zip_file_path)
        z.extractall(self.dir)
        self.local_shp_path = get_dotshp_from_shpdir(self.dir)


class StormWarningHandler(AbstractHandler):

    def __init__(self, year):
        self.year = year
        self.data_type = "STORM_WARNING"

    def construct_identifier(self):
        return str(self.year)

    def construct_remote_shp_path(self):
        return "https://mesonet.agron.iastate.edu/pickup/wwa/{}_tsmf_sbw.zip".format(str(self.year))


class StormReportHandler(AbstractHandler):

    def __init__(self, t0, t1):
        self.t0 = t0
        self.t1 = t1
        self.data_type = "STORM_REPORT"

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

        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/" \
                   "request/gis/watchwarn.py?&{t0}&{t1}".format(t0=t0,
                                                                t1=t1)

        return base_url

    def construct_identifier(self):
        return str(self.t0.year) + str(self.t0.month) + str(self.t0.day) + "_" + \
               str(self.t1.year) + str(self.t1.month) + str(self.t1.day)


class WazeHandler:

    def __init__(self, event_name):
        self.event_name = event_name
        self.gdf = self.getWazeAsDataFrame()
        self.min_time, self.max_time = self.getMinMaxTimes()

    @staticmethod
    def convertWazeTimeTo_DateTime(time):
        # TODO is this really the best way to construct this?  Maybe modular arithmetic?
        time = str(time)
        return datetime(int(time[0:4]),
                        int(time[4:6]),
                        int(time[6:8]),
                        int(time[8:10]),
                        int(time[10:12]))

    def getWazeAsDataFrame(self):
        csv = os.path.join(WAZE_DIR, "waze_" + self.event_name + ".txt")
        df = pd.read_csv(csv)

        geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
        crs = {'init': 'epsg:4326'}  # http://www.spatialreference.org/ref/epsg/2263/
        g = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        g["time"] = g["time"]//100
        return gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    def getMinMaxTimes(self):
        min_time = min(self.gdf[self.gdf['time'] != 0]['time'])
        max_time = max(self.gdf[self.gdf['time'] != 0]['time'])
        return self.convertWazeTimeTo_DateTime(min_time), \
               self.convertWazeTimeTo_DateTime(max_time)

    def getExtent(self):
        return ((min(self.gdf["lon"]), min(self.gdf["lat"])), (max(self.gdf["lon"]), max(self.gdf["lat"])))


class ZCTAHandler(AbstractHandler):

    def __init__(self):
        self.gdf = None
        self.get_gdf()

    def get_gdf(self, path=ZCTA_PATH):
        self.gdf = gpd.read_file(ZCTA_PATH)


class DataHolder:

    def __init__(self, storm, environment=SHP_TMP):
        self.name = storm
        self.environment = environment

        self.waze = None
        self.get_waze_veoc()

        self.min_time = min(self.waze["handler"].gdf[self.waze["handler"].gdf['time'] != 0]['time'])
        self.max_time = max(self.waze["handler"].gdf[self.waze["handler"].gdf['time'] != 0]['time'])

        self.storm_reports = None
        self.get_storm_reports()

        self.storm_warnings = None
        self.get_storm_warnings()

    def get_waze_veoc(self):
        waze = WazeHandler(self.name)
        out_path = self.environment + "{}_waze.shp".format(self.name)
        waze.gdf.to_file(out_path)
        self.waze = {
            "handler": waze,
            "shp": out_path
        }

    def get_storm_reports(self):
        SRH_Buffer = []

        # TODO implement a reasonable way to determine how long the storm lasted
        for i in range(0, 7):
            d0 = self.waze["handler"].min_time + timedelta(days=i)
            d1 = self.waze["handler"].min_time + timedelta(days=i + 1)
            SRH = StormReportHandler(d0, d1)
            SRH.cut_data_by_values()
            SRH.cut_to_extent(self.waze["handler"].get_extent())
            SRH.gdf["ISSUED"] = SRH.gdf["ISSUED"].astype('int')
            SRH.gdf["EXPIRED"] = SRH.gdf["EXPIRED"].astype('int')
            SRH.gdf = SRH.gdf[SRH.gdf["EXPIRED"] > self.min_time][SRH.gdf["ISSUED"] < self.max_time]
            SRH_Buffer.append(SRH)
        out_path = self.environment + "Storm_Reports.shp"
        merged_SRs = pd.concat([i.gdf for i in SRH_Buffer])
        merged_SRs.to_file(out_path)
        self.storm_reports = {
            "gdf": merged_SRs,
            "shp": out_path
        }

    def get_storm_warnings(self):
        SWH = StormWarningHandler(self.waze["handler"].min_time.year)
        SWH.cut_data_by_values()
        SWH.cut_to_extent(self.waze["handler"].get_extent())
        SWH.gdf["ISSUED"] = SWH.gdf["ISSUED"].astype('int')
        SWH.gdf["EXPIRED"] = SWH.gdf["EXPIRED"].astype('int')
        SWH.gdf = SWH.gdf[SWH.gdf["EXPIRED"] > self.min_time][SWH.gdf["ISSUED"] < self.max_time]
        out_path = self.environment + "Storm_Warnings.shp"
        SWH.gdf.to_file(out_path)
        self.storm_warnings = {
            "handler": SWH,
            "shp": out_path
        }

    def create_waze_vs_nws_gdf(self, polygons = ZCTAHandler()):
        polygons.cut_to_extent(data.waze["handler"].get_extent())
        buffer = []
        t_adjust = self.waze["handler"].getMinMaxTimes()[0].timestamp()
        # TODO remove this for loop
        for polygon_index, poly_row in polygons.gdf.iterrows():
            # Look up all the Waze intersections
            waze_intersecting_pts = self.waze["handler"].gdf[self.waze["handler"].gdf["geometry"].intersects(poly_row["geometry"])]
            # ["time"].values
            print(waze_intersecting_pts)
            for waze_index, waze_row in waze_intersecting_pts.iterrows():
                sw_gdf = self.storm_warnings["handler"].gdf
                swh_intersections = sw_gdf[sw_gdf["geometry"].intersects(waze_row["geometry"])]
                time_overlap = False
                print(swh_intersections)
                for i, warning in swh_intersections.iterrows():
                    waze_t = waze_row["time"]
                    issued = warning["ISSUED"]
                    expired = warning["EXPIRED"]
                    if issued < waze_t and waze_t < expired:
                        time_overlap = True



            print("Made it here")


            # Map each Waze point to say if it is space-time contained by a Storm Warning or not
        # waze_intersections = [w for w in waze_intersections if w != 0]
        # t = [WazeHandler.convertWazeTimeTo_DateTime(i).timestamp() for i in waze_intersections]
        # t = mdates.epoch2num(t)
        # if len(t) > 0:
        #     buffer.append({
        #         "ZCTA5": poly_row["GEOID10"],
        #         "times": t
        #     })
        return buffer

    def create_kde(self):
        pass


    @staticmethod
    def plot_all_histograms(buffer, nrow=2, ncol=2):
        ngraphs = len(buffer)
        npages = ngraphs // (nrow*ncol)
        for page in range(0, npages):
            fig, axs = plt.subplots(nrow, ncol, sharey=True, tight_layout=True)
            for r in range(0, nrow):
                for c in range(0, ncol):
                    index = page*nrow*ncol + r*ncol + c
                    axs[r, c].hist(buffer[index][1], bins=10)
                    axs[r, c].set_title(buffer[index][0])
                    axs[r, c].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d - %H'))
            plt.show()


if __name__ == "__main__":
    data = DataHolder("Harvey")
    #data.create_waze_vs_nws_gdf()
    #data.create_waze_time_distributions()

