import geopandas as gpd
import pandas as pd
import numpy as np
import os.path
import requests
from zipfile36 import ZipFile
from datetime import datetime, timedelta
from shapely.geometry import Point, Polygon
from configuration import *
from utils import *


class AbstractTimePointEvent:

    def __init__(self, t_field):
        self.t_field = t_field

    def clip_temporal(self, t0, t1):
        self.gdf = self.gdf[self.gdf[self.t_field] < t1][self.gdf[self.t_field] > t0]

    def get_temporal_extent(self, as_datetime=False):
        min_time = min(self.gdf[self.gdf[self.t_field] != 0][self.t_field])
        max_time = max(self.gdf[self.gdf[self.t_field] != 0][self.t_field])
        if as_datetime:
            return self.convert_numeric_to_datetime(min_time), \
                   self.convert_numeric_to_datetime(max_time)
        else:
            return min_time, max_time

    def convert_numeric_to_datetime(self, time):
        '''This will be overwritten by subclasses'''
        pass


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
            self.gdf = gpd.read_file(self.local_shp_path)
        elif "gdf" in kwargs:
            self.local_shp_path = None
            self.gdf = kwargs.get("gdf")
        elif "dir" in kwargs:
            if "traverse_subdirs" in kwargs:
                self.local_shp_path = None
                self.get_gdf_by_directory(kwargs.get("dir"), kwargs.get("traverse_subdirs"))
            else:
                self.get_gdf_by_directory(kwargs.get("dir"))
        else:
            self.get_gdf()

    def get_gdf(self):
        '''Search for the GDF locally, if not found look for a remote file.'''
        pass

    def cut_data_by_values(self, keys):
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

    def clip_spatial(self, extent):
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

    def get_spatial_extent(self, buffer=0.01, as_geometry=False):
        self.create_spatial_index_fields()
        pts = ((min(self.gdf["minx"])-buffer, min(self.gdf["miny"])-buffer),
                (max(self.gdf["maxx"])+buffer, max(self.gdf["maxy"])+buffer))
        if as_geometry:
            return Polygon([(pts[0][0], pts[0][1]),
                     (pts[0][0], pts[1][1]),
                     (pts[1][0], pts[1][1]),
                     (pts[1][0], pts[0][1]),
                     (pts[0][0], pts[0][1])])

        return pts

    def get_gdf_by_directory(self, dir, traverse_subdirs=False):
        data = []
        for i in get_by_extension(dir, ".shp"):
            p = os.path.join(dir, i)
            data.append(gpd.read_file(p))
        if traverse_subdirs:
            for root, dirs, files in walk(dir):
                for d in dirs:
                    p = os.path.join(root, d)
                    for i in get_by_extension(p, ".shp"):
                        data.append(gpd.read_file(os.path.join(p, i)))
        self.gdf = pd.concat(data)

    def rasterize_from_template(self, column_to_rasterize, template_path=RASTER_TEMPLATE):
        import rasterio
        from rasterio import features

        out_fn = get_tmp_path(TMP_DIR)
        rst = rasterio.open(template_path)
        meta = rst.meta.copy()
        meta.update(compress='lzw')

        with rasterio.open(out_fn, 'w+', **meta) as out:
            out_arr = out.read(1)
            shapes = ((geom, value) for geom, value in zip(self.gdf.geometry, self.gdf[column_to_rasterize]))
            burned = features.rasterize(shapes=shapes, fill=np.nan, out=out_arr, transform=out.transform)
            out.write_band(1, burned)

        return out_fn

    def get_reprojected_dataframe(self, crs):

        return self.gdf.to_crs(crs)

    #TODO add an append_table function


class RemoteDataManager(AbstractGeoHandler):

    def __init__(self, **kwargs):
        self.dir = os.path.join(DATA_DIRS[self.configuration_lookup], self.construct_local_identifier())
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        print(self.dir)
        super(RemoteDataManager, self).__init__(**kwargs)

    def get_gdf(self):
        '''Search for the GDF locally, if not found look for a remote file.'''
        self.get_local_data()
        if self.gdf is None:
            self.get_remote_data()

    def get_local_data(self):
        """Look for data in local file system"""

        if self.file_type == ".geojson":
            try:
                print("Found local @ {}".format(self.get_local_path()))
                self.gdf = gpd.read_file(self.get_local_path())
            except:
                self.gdf = None

        elif self.file_type == ".zip":
            tentative_shp = get_dotshp_from_shpdir(self.dir)
            try:
                print("Found local @ {}".format(tentative_shp))
                self.gdf = gpd.read_file(tentative_shp)
            except:
                print("Local not found")
                self.gdf = None

    def get_local_path(self):
        out = self.construct_local_identifier() + self.file_type
        return os.path.join(self.dir, out)

    def get_remote_data(self):
        '''Find a remote shapefile.
        Must have implementations in a child class'''
        def extract_zip(file_path):
            z = ZipFile(file_path)
            z.extractall(self.dir)
            self.local_shp_path = get_dotshp_from_shpdir(self.dir)
            self.gdf = gpd.read_file(self.local_shp_path)

        def extract_gjson(file_path):
            self.gdf = gpd.read_file(file_path)

        extraction_dictionary = {
            ".zip": extract_zip,
            ".geojson": extract_gjson
        }

        file_path = self.get_local_path()
        print(file_path)

        with open(file_path, "wb") as file:
            r = requests.get(self.construct_url())
            # print(r.content)
            file.write(r.content)

        extraction_dictionary[self.file_type](file_path)

    def construct_local_identifier(self):
        """Overridden by child classes"""
        pass

    def construct_url(self):
        """Overridden by child classes"""
        pass


class AbstractSpaceTimeFunctionality:
    '''Space Time functionality built on AbstractGeoHandler and AbstractTimeEvents.'''
    def __init__(self):
        pass

    @staticmethod
    def space_time_containment(time_point_handler, time_duration_handler):
        '''Returns the spatial intersection, and whether or not each spatial intersection
        is space-time contained or not in the "time_overlap" field'''
        # This line is for convenience
        t0 = time_duration_handler.t_start_field
        t1 = time_duration_handler.t_end_field
        t = time_point_handler.t_field

        # Create a spatial intersection
        spatial_intersection = gpd.sjoin(time_point_handler.gdf, time_duration_handler.gdf, how="left", op="intersects")
        return spatial_intersection

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


# TODO Implement
class AbstractOutputsAndGraphs:

    def __init__(self):
        pass



