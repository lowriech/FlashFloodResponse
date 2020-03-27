import geopandas as gpd
import os.path


class AbstractTimePointEvent:
    """
    Point in time functionality.
    Attributes:
        - t_field: inherited from child class
    """
    t_field: str = None
    convert_numeric_to_datetime = None
    gdf: gpd.GeoDataFrame = None

    def clip_temporal(self, t0, t1):
        """Clip the data to a temporal extent"""
        self.gdf = self.gdf[self.gdf[self.t_field] < t1][self.gdf[self.t_field] > t0]

    def get_temporal_extent(self, as_datetime=False):
        """Get the temporal extent of the data"""
        min_time = min(self.gdf[self.gdf[self.t_field] != 0][self.t_field])
        max_time = max(self.gdf[self.gdf[self.t_field] != 0][self.t_field])
        if as_datetime:
            return self.convert_numeric_to_datetime(min_time), \
                   self.convert_numeric_to_datetime(max_time)
        else:
            return min_time, max_time


class AbstractTimeDurationEvent:
    """
    Duration in time functionality.
    Attributes:
        Inherited from child class:
        - t_start_field: string indicating the beginning of the event
        - t_end_field: string indicating the end of the event
    """
    t_start_field: str = None
    t_end_field: str = None
    convert_numeric_to_datetime = None
    gdf: gpd.GeoDataFrame = None

    def clip_temporal(self, t0, t1):
        """Clip the data to a temporal extent"""
        self.gdf = self.gdf[self.gdf[self.t_start_field] < t1][self.gdf[self.t_end_field] > t0]

    def get_temporal_extent(self, as_datetime=False):
        """Get the temporal extent of the data"""
        min_time = min(self.gdf[self.gdf[self.t_start_field] != 0][self.t_start_field])
        max_time = max(self.gdf[self.gdf[self.t_end_field] != 0][self.t_end_field])
        if as_datetime:
            return self.convert_numeric_to_datetime(min_time), \
                   self.convert_numeric_to_datetime(max_time)
        else:
            return min_time, max_time


class AbstractGeoHandler:
    """Handler for storing routine operations on GeoDataFrames."""
    get_gdf = None
    gdf: gpd.GeoDataFrame = None

    def __init__(self, gdf):
        """
        Initialize by passing a GDF.  All operations will center around this.
        If more complex initialization is needed, that can be found in DataManager.
        """
        self.gdf = gdf

    def cut_data_by_values(self, keys):
        """Filter a dataframe by specific values"""
        x = self.gdf
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        self.gdf = x

    def create_spatial_index_fields(self):
        """Potentially useful for spatial indexing and efficient calculation"""
        bounds = self.gdf["geometry"].bounds
        self.gdf["minx"] = bounds["minx"]
        self.gdf["maxx"] = bounds["maxx"]
        self.gdf["miny"] = bounds["miny"]
        self.gdf["maxy"] = bounds["maxy"]

    def clip_spatial(self, extent):
        # TODO: try/except is currently implemented to handle empty dataframes.  Not optimal
        """Takes an extent (lower left, upper right) and clips the GDF to these bounds"""
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
        """Get the spatial extent of the GeoDataFrame,
        potentially with a buffer or as a Shapely Polygon"""
        self.create_spatial_index_fields()
        pts = ((min(self.gdf["minx"])-buffer, min(self.gdf["miny"])-buffer),
               (max(self.gdf["maxx"])+buffer, max(self.gdf["maxy"])+buffer))
        if as_geometry:
            from shapely.geometry import Polygon
            return Polygon([(pts[0][0], pts[0][1]),
                            (pts[0][0], pts[1][1]),
                            (pts[1][0], pts[1][1]),
                            (pts[1][0], pts[0][1]),
                            (pts[0][0], pts[0][1])])

        return pts

    def clip_by_shape(self, other_gdf):
        """Clip this GDF by another GDF"""
        self.gdf = gpd.clip(self.gdf, other_gdf)

    #TODO add an append_table function


class DataManager(AbstractGeoHandler):
    """Manages data access between local and remote sources.
    Eventually this will be built into a separate module, along with drivers.
    Currently just supports remote fetching a geojson."""
    construct_local_identifier = None
    construct_url = None
    home_dir: str = None

    def __init__(self, **kwargs):
        """
        Initialize a DataHandler.  If 'path' is passed, use that to read a GDF and initialize
        AbstractGeoHandler.  Otherwise, look for a GDF based on the objects local and remote connections
        """
        if "path" in kwargs:
            gdf = self.read_local_data(kwargs.get("path"))
        else:
            gdf = self.get_gdf()
        AbstractGeoHandler.__init__(self, gdf=gdf)

    def get_gdf(self):
        """Search for the GDF locally, if not found look for a remote file."""
        if not os.path.exists(self.get_local_path()):
            self.get_remote_data()
        return self.read_local_data()

    def read_local_data(self, path=None):
        """Read a local file"""
        if path is None:
            return gpd.read_file(self.get_local_path())
        else:
            pass

    def get_local_path(self):
        """Construct a local file path"""
        out = self.construct_local_identifier()
        return os.path.join(self.home_dir, out)

    def get_remote_data(self):
        """Look for remote data.  Requires URL construction in child class."""
        import requests
        file_path = self.get_local_path()
        with open(file_path, "wb") as file:
            r = requests.get(self.construct_url())
            file.write(r.content)
