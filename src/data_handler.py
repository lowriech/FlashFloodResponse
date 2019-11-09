import geopandas as gpd
import pandas as pd
import numpy as np
import os.path
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
from shapely.geometry import Point

from configuration import *
from utils import *

STORM_REPORT_FF_KEYS = {"PHENOM": "FF"}


class AbstractHandler:
    '''A parent handler for geospatial dataframes.
    This is intended to handle basic geodataframe operations.'''
    def __init__(self, local_shp_path = None):
        self.local_shp_path = local_shp_path
        self.gdf = None
        self.dir = os.path.join(DATA_DIRS[self.data_type], self.construct_identifier())
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


class NWSHandler(AbstractHandler):
    '''For processing GDFs related to the National Weather Service, via the Iowa Env Mesonet'''
    def __init__(self, **kwargs):
        super(NWSHandler, self).__init__(**kwargs)
        self.prep_data_variables()

    def prep_data_variables(self):
        self.gdf["ISSUED"] = self.gdf["ISSUED"].astype('int')
        self.gdf["EXPIRED"] = self.gdf["EXPIRED"].astype('int')
        self.gdf.to_crs({'init': 'epsg:4326'})

    def clip_temporal(self, t0, t1):
        self.gdf = self.gdf[self.gdf["ISSUED"] < t1][self.gdf["EXPIRED"] > t0]


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

        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/" \
                   "request/gis/watchwarn.py?&{t0}&{t1}".format(t0=t0,
                                                                t1=t1)

        return base_url

    def construct_identifier(self):
        return str(self.t0.year) + str(self.t0.month) + str(self.t0.day) + "_" + \
               str(self.t1.year) + str(self.t1.month) + str(self.t1.day)


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


class WazeHandler:

    def __init__(self, event_name):
        self.event_name = event_name
        self.gdf = self.getWazeAsDataFrame()
        self.min_time, self.max_time = self.get_min_max_times()

    def getWazeAsDataFrame(self):
        csv = os.path.join(WAZE_DIR, "waze_" + self.event_name + ".txt")
        df = pd.read_csv(csv)

        geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
        crs = {'init': 'epsg:4326'}  # http://www.spatialreference.org/ref/epsg/2263/
        g = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        g["time"] = g["time"]//100
        return gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    def get_min_max_times(self, as_datetime = False):
        min_time = min(self.gdf[self.gdf['time'] != 0]['time'])
        max_time = max(self.gdf[self.gdf['time'] != 0]['time'])
        if as_datetime:
            return convert_numeric_to_datetime(min_time), \
                   convert_numeric_to_datetime(max_time)
        else:
            return min_time, max_time

    def get_extent(self):
        return ((min(self.gdf["lon"]), min(self.gdf["lat"])),
                (max(self.gdf["lon"]), max(self.gdf["lat"])))


class GeographyHandler(AbstractHandler):

    def __init__(self):
        self.data_type = "GEOGRAPHY"
        self.local_shp_path = ZCTA_PATH
        self.get_gdf()

    def get_gdf(self, path=ZCTA_PATH):
        self.gdf = gpd.read_file(ZCTA_PATH)


class DataHolder:

    def __init__(self, storm, environment=SHP_TMP, geographies = GeographyHandler()):
        self.name = storm
        self.environment = environment
        self.geographies = geographies

        self.waze = WazeHandler(self.name)
        self.extent = {
            "temporal": self.waze.get_min_max_times(),
            "spatial": self.waze.get_extent()
        }

        self.storm_reports = get_storm_reports(self.waze)
        t = self.waze.get_min_max_times(as_datetime = True)[0]
        self.storm_warnings = StormWarningHandler(t.year)
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
        waze_warnings = gpd.sjoin(self.waze, self.storm_warnings, how="left", op="intersects")
        waze_warnings["ISSUED"] = np.where(waze_warnings["ISSUED"].isnull(), -1, waze_warnings["ISSUED"])
        waze_warnings["EXPIRED"] = np.where(waze_warnings["EXPIRED"].isnull(), -1, waze_warnings["EXPIRED"])
        waze_warnings["time_overlap"] = np.where(
            (waze_warnings["ISSUED"] < waze_warnings["time"]) & (waze_warnings["time"] < waze_warnings["EXPIRED"]), 1,
            0)
        waze_warnings.to_file(os.path.join(self.environment, self.name + "_view1.shp"))
        return waze_warnings

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


