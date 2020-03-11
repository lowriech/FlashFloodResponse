import os.path
from datetime import datetime, timedelta
from spacetime.spacetime_handlers import *
from configuration import *
from spatial_analytics import *
from utils import *


class IowaEnvironmentalMesonet:
    """A super class for managing connections with the Iowa Environmental Mesonet"""

    base_url: str = "https://mesonet.agron.iastate.edu/geojson/"
    file_type: str = ".geojson"
    t0: datetime = None
    t1: datetime = None

    def construct_url(self):
        """Construct a URL for fetching remote data"""
        times = self.times_as_string_tuple()
        t0 = "".join(times[0][0:3])
        t1 = "".join(times[1][0:3])
        print(self.base_url.format(t0=t0, t1=t1))
        return self.base_url.format(t0=t0, t1=t1)

    def times_as_string_tuple(self):
        """Fetch times as a string tuple"""
        return (
            [str(i).zfill(2) for i in self.t0.timetuple()],
            [str(i).zfill(2) for i in self.t1.timetuple()]
        )

    def construct_local_identifier(self):
        """Construct an identifier to save files locally and reduce network traffic"""
        times = self.times_as_string_tuple()
        t0 = "".join(times[0][0:5])
        t1 = "".join(times[1][0:5])
        return t0 + "_" + t1 + self.file_type

    @staticmethod
    def convert_numeric_to_datetime(x):
        """Convert the initial time storage format to datetime"""
        return datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')


class StormWarningHandler(IowaEnvironmentalMesonet, DataManager, AbstractTimeDurationEvent):
    """Handler for Storm Warning Polygons"""

    t_start_field: str = "issue"
    t_end_field: str = "expire"
    home_dir: str = config.sw

    def __init__(self, t0, t1, **kwargs):
        self.t0 = t0
        self.t1 = t1
        # This is unformatted, and gets formatted in IowaEnvironmentalMesonet.construct_url()
        self.base_url = os.path.join(self.base_url, 'sbw.php?sts={t0}&ets={t1}&wfos=')
        DataManager.__init__(self, **kwargs)

    def prep_data(self):
        """Called last in the initialization, this handles any adhoc data cleanup that is needed"""
        self.cut_data_by_values({"phenomena": "FF"})
        self.gdf[self.t_start_field] = self.gdf[self.t_start_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )
        self.gdf[self.t_end_field] = self.gdf[self.t_end_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )


class LocalStormReportHandler(IowaEnvironmentalMesonet, DataManager, AbstractTimePointEvent, SpatialAnalytics):
    """Handler for Local Storm Report Points"""

    t_field: str = "valid"
    home_dir: str = config.lsr

    def __init__(self, t0, t1, **kwargs):
        self.t0 = t0
        self.t1 = t1
        # This is unformatted, and gets formatted in IowaEnvironmentalMesonet.construct_url()
        self.base_url = os.path.join(self.base_url, "lsr.php?inc_ap=yes&sts={t0}&ets={t1}&wfos=")
        DataManager.__init__(self, **kwargs)

    def prep_data(self):
        """Called last in the initialization, this handles any adhoc data cleanup that is needed"""

        self.cut_data_by_values({"type": "F"})
        self.gdf[self.t_field] = self.gdf[self.t_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )


def iterative_fetch(extent, obj, fetch_by=6):
    """Iteratively fetch when individual API calls would return large results.
    Concatenates the multiple calls into one object to return"""

    storm_reports = []
    min_datetime, max_datetime = extent.temporal
    current_time = min_datetime
    bbox = extent.spatial.get_spatial_extent()

    while current_time < max_datetime:
        next_time = current_time + timedelta(hours=fetch_by + 1)
        sr = obj(current_time, next_time)
        sr.clip_spatial(bbox)
        storm_reports.append(sr)
        current_time = next_time

    merged_srs = pd.concat([i.gdf for i in storm_reports], sort=False)
    return obj(min_datetime, max_datetime,
               gdf=merged_srs)
