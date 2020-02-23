"""For spatial analytics functionality that works on AbstractGeoHandlers"""
from AbstractXYTFunctionality.AbstractXYTFunc import *
import pandas as pd


class SpatialAnalytics:

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


def get_equidistant_dataframe(gdf):
    from pyproj import CRS
    wkt = 'PROJCS["North_America_Equidistant_Conic",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Equidistant_Conic"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["central_meridian",-96],PARAMETER["Standard_Parallel_1",20],PARAMETER["Standard_Parallel_2",60],PARAMETER["latitude_of_origin",40],UNIT["Meter",1]]'
    cc = CRS(wkt)
    return gdf.to_crs(cc)






