import geopandas as gpd
import os.path
from os import walk
import requests
from zipfile36 import ZipFile
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from waze import fetchAllWaze, getWazeAsDataFrame, analyzeWaze_x_NWS
from configuration import *


STORM_REPORT_FF_KEYS = {"PHENOM": "FF"}

def get_dotshp_from_shpdir(shpdir):
    for root, dirs, files in walk(shpdir):
        for filename in files:
            if filename.endswith(".shp"):
                return filename

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
    return local_data_dir + "/" + get_dotshp_from_shpdir(local_data_dir)


#TODO Abstract the commonalities of the Handlers and create a parent class
class StormWarningHandler:

    def __init__(self, local_shp_path=None):
        self.local_shp_path = local_shp_path
        self.gdf = None

    def get_gdf(self):
        self.gdf = gpd.read_file(self.local_shp_path)

    def get_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        return x

    def fetch_storm_warnings(self, year):
        url = "https://mesonet.agron.iastate.edu/pickup/wwa/{}_tsmf_sbw.zip".format(str(year))
        self.local_shp_path = get_remote_shp(url, "storm_warnings", str(year))

class StormReportHandler:

    def __init__(self, local_shp_path=None):
        self.local_shp_path = local_shp_path
        self.gdf = None

    def get_gdf(self):
        self.gdf = gpd.read_file(self.local_shp_path)

    def get_data_by_values(self, keys=STORM_REPORT_FF_KEYS):
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        return x

    def construct_storm_report_url(self, t0, t1):
        t0 = "year1={year1}&" \
             "month1={month1}&" \
             "day1={day1}&" \
             "hour1={hour1}&" \
             "minute1={minute1}&".format(year1=t0.year,
                                         month1=t0.month,
                                         day1=t0.day,
                                         hour1=t0.hour,
                                         minute1=t0.minute)

        t1 = "year2={year2}&" \
             "month2={month2}&" \
             "day2={day2}&" \
             "hour2={hour2}&" \
             "minute2={minute2}&".format(year2=t1.year,
                                         month2=t1.month,
                                         day2=t1.day,
                                         hour2=t1.hour,
                                         minute2=t1.minute)

        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/" \
                   "request/gis/watchwarn.py?&{t0}&{t1}".format(t0=t0,
                                                                t1=t1)

        return base_url

    def construct_storm_report_identifier(self, t0, t1):
        return str(t0.year) + str(t0.month) + str(t0.day) + "_" + \
               str(t1.year) + str(t1.month) + str(t1.day)

    def fetch_storm_reports(self, t0, t1):
        storm_report_url = self.construct_storm_report_url(t0, t1)
        identifier = self.construct_storm_report_identifier(t0, t1)
        self.local_shp_path = get_remote_shp(storm_report_url, "storm_reports", identifier)


def main():
    time0 = datetime(2019, 10, 11, 4, 0)
    time1 = datetime(2019, 10, 12, 4, 0)
    SRH = StormReportHandler()
    SRH.fetch_storm_reports(time0, time1)
    SRH.get_gdf()
    SR_FF = SRH.get_data_by_values()

    SWH = StormWarningHandler()
    SWH.fetch_storm_warnings(2019)
    SWH.get_gdf()
    SWH_FF = SWH.get_data_by_values()

    return SR_FF, SWH_FF

SR_FF, SWH_FF = main()



