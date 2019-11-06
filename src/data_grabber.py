import geopandas as gpd
import pandas as pd
import os.path
from os import walk
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
from configuration import *
from shapely.geometry import Point
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

STORM_REPORT_FF_KEYS = {"PHENOM": "FF"}


def get_dotshp_from_shpdir(shpdir):
    for root, dirs, files in walk(shpdir):
        for filename in files:
            if filename.endswith(".shp"):
                print(root)
                return os.path.join(root, filename)


def get_remote_shp(url, data_type, identifier):
    if not os.path.exists(DATA_PATH + data_type):
        os.mkdir(DATA_PATH + data_type)

    local_data_dir = DATA_PATH + data_type + "/" + identifier

    if not os.path.exists(local_data_dir):
        os.mkdir(local_data_dir)

    zip_file_path = TMP_DIR + "/" + identifier + ".zip"

    with open(zip_file_path, "wb") as file:
        r = requests.get(url)
        file.write(r.content)

    z = ZipFile(zip_file_path)
    z.extractall(local_data_dir)
    print(local_data_dir)
    return get_dotshp_from_shpdir(local_data_dir)


class AbstractHandler:
    def __init__(self):
        self.local_shp_path = None
        self.gdf = self.get_gdf()

    def get_gdf(self):
        tentative_dataset = self.get_local_data()
        if tentative_dataset is not None:
            return tentative_dataset
        else:
            self.fetch_remote_data()
            return gpd.read_file(self.local_shp_path)

    def cut_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        self.gdf = x

    def get_local_data(self):
        tentative_shp = get_dotshp_from_shpdir(self.construct_local_shp_path())
        print(tentative_shp)
        print("Checking local")
        try:
            self.local_shp_path = tentative_shp
            print("Local Found")
            return gpd.read_file(tentative_shp)
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


class StormWarningHandler(AbstractHandler):

    def __init__(self, year):
        self.year = year
        self.local_shp_path = None
        self.dir = STORM_WARNING_DIR
        self.gdf = self.get_gdf()

    def construct_local_shp_path(self):
        return os.path.join(self.dir, str(self.year))

    def fetch_remote_data(self):
        url = "https://mesonet.agron.iastate.edu/pickup/wwa/{}_tsmf_sbw.zip".format(str(self.year))
        print("Fetching remotely from {}".format(url))
        self.local_shp_path = get_remote_shp(url, "storm_warnings", str(self.year))
        return self.local_shp_path


class StormReportHandler(AbstractHandler):

    def __init__(self, t0, t1):
        self.t0 = t0
        self.t1 = t1
        self.local_shp_path = None
        self.dir = STORM_REPORT_DIR
        self.gdf = self.get_gdf()

    def construct_local_shp_path(self):
        return os.path.join(self.dir, self.construct_storm_report_identifier())

    def construct_storm_report_url(self):
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

    def construct_storm_report_identifier(self):
        return str(self.t0.year) + str(self.t0.month) + str(self.t0.day) + "_" + \
               str(self.t1.year) + str(self.t1.month) + str(self.t1.day)

    def fetch_remote_data(self):
        storm_report_url = self.construct_storm_report_url()
        identifier = self.construct_storm_report_identifier()
        print("Fetching remotely from {}".format(storm_report_url))
        self.local_shp_path = get_remote_shp(storm_report_url, "storm_reports", identifier)
        return self.local_shp_path


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
            print(SRH.gdf)
            print(d0)
            print(d1)
            print(self.waze["handler"].getExtent())
            SRH.cut_to_extent(self.waze["handler"].getExtent())
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
        SWH.cut_to_extent(self.waze["handler"].getExtent())
        out_path = self.environment + "Storm_Warnings.shp"
        SWH.gdf.to_file(out_path)
        self.storm_warnings = {
            "handler": SWH,
            "shp": out_path
        }

    def create_waze_time_distributions(self, polygons = ZCTAHandler()):
        polygons.cut_to_extent(data.waze["handler"].getExtent())
        buffer = []
        t_adjust = self.waze["handler"].getMinMaxTimes()[0].timestamp()
        # TODO remove this for loop
        for polygon_index, poly_row in polygons.gdf.iterrows():
            waze_intersections = self.waze["handler"].gdf[self.waze["handler"].gdf["geometry"].intersects(poly_row["geometry"])]["time"].values
            t = [WazeHandler.convertWazeTimeTo_DateTime(i).timestamp() for i in waze_intersections]
            t = mdates.epoch2num(t)
            if len(t) > 0:
                buffer.append([polygon_index, t])
        self.plot_all_histograms(buffer)

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


data = DataHolder("Irma")
data.create_waze_time_distributions()

