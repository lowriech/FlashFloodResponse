import geopandas as gpd
import pandas as pd
import os.path
from os import walk
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from waze import fetchAllWaze, analyzeWaze_x_NWS
from configuration import *
from shapely.geometry import Point

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


#TODO Abstract the commonalities of the Handlers and create a parent class
class StormWarningHandler:

    def __init__(self, year):

        self.year = year
        self.local_shp_path = None
        self.gdf = self.get_gdf()

    def get_gdf(self):
        tentative_dataset = self.get_local_data(self.year)
        if tentative_dataset is not None:
            return tentative_dataset
        else:
            self.fetch_storm_warnings(self.year)
            return gpd.read_file(self.local_shp_path)

    def get_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        return x

    def get_local_data(self, year):
        tentative_shp = get_dotshp_from_shpdir(STORM_WARNING_DIR + str(year) + "/")
        print(tentative_shp)
        print("Checking local")
        try:
            self.local_shp_path = tentative_shp
            print("Local Found")
            return gpd.read_file(tentative_shp)
        except:
            print("Local not found")
            return None

    def fetch_storm_warnings(self, year):
        url = "https://mesonet.agron.iastate.edu/pickup/wwa/{}_tsmf_sbw.zip".format(str(year))
        print("Fetching remotely from {}".format(url))
        self.local_shp_path = get_remote_shp(url, "storm_warnings", str(year))
        return self.local_shp_path

class StormReportHandler:

    def __init__(self, t0, t1):
        self.t0 = t0
        self.t1 = t1
        self.local_shp_path = None
        self.gdf = self.get_gdf()

    def get_gdf(self):
        tentative_dataset = self.get_local_data()
        if tentative_dataset is not None:
            return tentative_dataset
        else:
            self.fetch_storm_reports()
            return gpd.read_file(self.local_shp_path)

    def get_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        return x

    def get_local_data(self):
        tentative_shp = get_dotshp_from_shpdir(STORM_REPORT_DIR + self.construct_storm_report_identifier() + "/")
        print(tentative_shp)
        print("Checking Local")
        try:
            self.local_shp_path = tentative_shp
            print("Local Found")
            return gpd.read_file(tentative_shp)
        except:
            print("Local not found")
            return None

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

    def fetch_storm_reports(self):
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
        time = str(time)
        return datetime(int(time[0:4]),
                        int(time[4:6]),
                        int(time[6:8]),
                        int(time[8:10]),
                        int(time[10:12]))

    def getWazeAsDataFrame(self):
        csv = os.path.join(WAZE_DIR, "waze_"+self.event_name+".txt")
        df = pd.read_csv(csv)

        geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
        crs = {'init': 'epsg:4326'}  # http://www.spatialreference.org/ref/epsg/2263/
        return gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    def getMinMaxTimes(self):
        min_time = min(self.gdf[self.gdf['time'] != 0]['time'])
        max_time = max(self.gdf[self.gdf['time'] != 0]['time'])
        return self.convertWazeTimeTo_DateTime(min_time), \
               self.convertWazeTimeTo_DateTime(max_time)



def main():
    Harvey = WazeHandler("Harvey")

    time0 = datetime(2019, 10, 11, 4, 0)
    time1 = datetime(2019, 10, 12, 4, 0)
    SRH = StormReportHandler(time0, time1)
    SR_FF = SRH.get_data_by_values()

    SWH = StormWarningHandler(time0.year)
    SWH_FF = SWH.get_data_by_values()



    return SWH_FF, SR_FF, Harvey

def main_2():

    Harvey = WazeHandler("Harvey")

    SRH_Buffer = []

    # min_time = Harvey.min_time
    # max_time = Harvey.max_time
    # p90_time = Harvey.convertWazeTimeTo_DateTime(Harvey.gdf.quantile(0.9)["time"])
    #
    # for i in range(0, (Harvey.max_time - Harvey.min_time).days):
    for i in range(0, 7):
        d0 = Harvey.min_time + timedelta(days=i)
        d1 = Harvey.min_time + timedelta(days=i+1)
        SRH = StormReportHandler(d0, d1)
        SRH_Buffer.append(SRH.get_data_by_values())

    SWH = StormWarningHandler(Harvey.min_time.year)
    SWH_FF = SWH.get_data_by_values()

    return SWH_FF, SRH_Buffer, Harvey

SWH_FF, SRH_Buffer, Harvey = main_2()



