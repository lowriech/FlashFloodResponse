from src.spacetime.spacetime_handlers import AbstractGeoHandler, AbstractTimePointEvent
import pandas as pd
import geopandas as gpd
import numpy as np


class SpaceTimePointStatistics:
    """
    Code for implementing spatial statistics.
    Children of AbstractGeoHandler can inherit from this class to add statistical functionality.
    """
    t_field: str = None
    gdf: gpd.GeoDataFrame = None

    def k_function(self):
        """SOURCE:
        Lloyd, C D. (2010).
        Chapter 7: Exploring Spatial Point Patterns.
        Spatial Data Analysis: An Introduction for GIS Users.
        Oxford, UK: Oxford University Press."""

        from pysal.lib.cg import distance_matrix

        g = AbstractGeoHandler(gdf=get_equidistant_dataframe(self.gdf))
        A = g.get_spatial_extent(as_geometry=True).area
        m = min(self.gdf[self.t_field])

        if isinstance(self, AbstractTimePointEvent):
            frame = pd.DataFrame(
                {
                    "x": g.gdf.geometry.x,
                    "y": g.gdf.geometry.y,
                    "t": self.gdf[self.t_field].apply(
                        lambda x: (x - m).total_seconds()
                    )
                }
            ).to_numpy()

        else:
            frame = pd.DataFrame(
                {
                    "x": g.gdf.geometry.x,
                    "y": g.gdf.geometry.y
                }
            ).to_numpy()

        distances = distance_matrix(frame)
        all_distances = distances.flatten()

        distance_array = range(0, int(max(all_distances)), 1000)
        output = dict()
        for distance in distance_array:
            running_sum = 0
            for event in frame:
                running_sum += sum(event < distance)-1
            output[distance] = A/distances.shape[0] * running_sum

        return output

    def bivariate_spatial_distance_matrix(self, other):
        """Create a bivariate, m by n spatial distance matrix
        Columns are from this dataframe, rows/index are from 'other'"""
        # TODO there is undoubtedly a faster way to do this
        g = get_equidistant_dataframe(self.gdf)
        g = g.geometry
        other = get_equidistant_dataframe(other.gdf)
        other = other.geometry
        x = pd.DataFrame(columns=g.index, index=other.index)
        for i in g.index:
            x[i] = other.distance(g[i])
        return x

    def bivariate_temporal_distance_matrix(self, other):
        """Create a bivariate, m by n temporal distance matrix
        Columns are from this dataframe, rows/index are from 'other'"""
        self_t = self.gdf[self.t_field]
        other_t = other.gdf[other.t_field]
        t = pd.DataFrame(columns=self_t.index, index=other_t.index)
        for i in self_t.index:
            t[i] = other_t.apply(lambda a: a - self_t[i])
        return t

    def spacetime_cube(self):
        from mpl_toolkits.mplot3d import Axes3D
        import matplotlib.pyplot as plt
        threedee = plt.figure().gca(projection='3d')
        if isinstance(self.gdf, pd.DataFrame):
            threedee.scatter(
                self.gdf.geometry.x, self.gdf.geometry.y, self.gdf[self.t_field].apply(lambda x: x.timestamp())
            )
        elif isinstance(self.gdf, pd.Series):
            threedee.scatter(
                self.gdf.geometry.x, self.gdf.geometry.y, self.gdf.loc[self.t_field].timestamp()
            )
        threedee.set_xlabel('X')
        threedee.set_ylabel('Y')
        threedee.set_zlabel('T')
        return threedee

    def add_self_to_spacetime_cube(self, figure):
        if isinstance(self.gdf, pd.DataFrame):
            figure.scatter(
                self.gdf.geometry.x, self.gdf.geometry.y, self.gdf[self.t_field].apply(lambda x: x.timestamp())
            )
        elif isinstance(self.gdf, pd.Series):
            figure.scatter(
                self.gdf.geometry.x, self.gdf.geometry.y, self.gdf.loc[self.t_field].timestamp()
            )
        return figure

    @staticmethod
    def distance_to_n_points_by_observation(distance_matrix, n):
        """
        Return a Series representing the distances to include n points for a distance matrix
        :param distance_matrix:
        :param n:
        :return: Series
        """
        return distance_matrix[distance_matrix.rank() <= float(n)].max()

    @staticmethod
    def add_reference_circle(figure, r, x0, y0, z0):
        theta = np.linspace(0, 2*np.pi, 100)
        z = np.ones(100) * z0
        x = x0 + r*np.sin(theta)
        y = y0 + .85*r*np.cos(theta)
        figure.plot(x, y, z)
        figure.text(x0+r, y0-r, z0, s="r = {} m".format(str(int(r))), zdir=(1, 1, 0), fontsize='small',
                    horizontalalignment='center')
        return figure


class SpaceTimeContainment:
    """
    Space Time functionality built on AbstractGeoHandler and AbstractTimeEvents.
    Functionality for checking containment of Time Points and Time Duration.

    Unlike Spatial Statistics, this code is standalone and doesn't need to be inherited.
    """

    @staticmethod
    def space_time_containment(time_point_handler, time_duration_handler):
        """Returns the spatial intersection, and whether or not each spatial intersection
        is space-time contained or not in the "time_overlap" field"""
        import numpy as np
        # This line is for convenience
        t0 = time_duration_handler.t_start_field
        t1 = time_duration_handler.t_end_field
        t = time_point_handler.t_field

        # Create a spatial intersection
        spatial_intersection = gpd.sjoin(time_point_handler.gdf, time_duration_handler.gdf, how="left", op="intersects")

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
        """Returns whether or not each point has any containing duration event,
        in the 'has_overlap' column"""
        z = SpaceTimeContainment.space_time_containment(time_point_handler, time_duration_handler)
        z = z["time_overlap"].groupby(by=z.index).max().reset_index(name='has_overlap')[["has_overlap"]]
        return time_point_handler.gdf.join(z)

    @staticmethod
    def count_points_per_geography(polygon_handler, point_handler, collect_on=None):
        """Count the number of points contained per polygonal geography.
        Also returns a collected list of a field, if specified."""
        z = gpd.sjoin(polygon_handler.gdf, point_handler.gdf, how="left", op="intersects")
        z_counts = z["index_right"].groupby(by=z.index).count().reset_index(name="count")[["count"]]
        if collect_on is not None:
            z_collect = z[collect_on].groupby(by=z.index).apply(list).reset_index(name="collection")[["collection"]]
            z_counts = z_counts.join(z_collect)

        return polygon_handler.gdf.join(z_counts)


def get_equidistant_dataframe(gdf):
    from pyproj import CRS
    wkt = 'PROJCS["North_America_Equidistant_Conic",' \
          'GEOGCS["GCS_North_American_1983",' \
          'DATUM["D_North_American_1983",' \
          'SPHEROID["GRS_1980",6378137,298.257222101]],' \
          'PRIMEM["Greenwich",0],' \
          'UNIT["Degree",0.017453292519943295]],' \
          'PROJECTION["Equidistant_Conic"],' \
          'PARAMETER["False_Easting",0],' \
          'PARAMETER["False_Northing",0],' \
          'PARAMETER["central_meridian",-96],' \
          'PARAMETER["Standard_Parallel_1",20],' \
          'PARAMETER["Standard_Parallel_2",60],' \
          'PARAMETER["latitude_of_origin",40],' \
          'UNIT["Meter",1]]'
    cc = CRS(wkt)
    return gdf.to_crs(cc)

