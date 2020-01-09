from configuration import RASTER_DIR
import os.path
from owslib.wcs import WebCoverageService
import rasterio
from rasterio.mask import mask


'''Starting work on a straightforward OWSLib/Rasterio wrapper for accessing raster data.
For more information, see https://geopython.github.io/OWSLib/'''

# TODO:
# In Progress
# - This should go into a general Geographic Functionality folder
# - Functions:
#   Cross against a GeoDataFrame
# - Mask statistics
# - Layer management within one WCS server


class RemoteWCSManager:

    def __init__(self, server, layer, name, version='1.0.0', crs='EPSG:4326', res=0.05):
        wcs = WebCoverageService(server, version=version)

        layer_to_use = wcs.contents[layer]
        bbox = layer_to_use.boundingBoxWGS84
        response = wcs.getCoverage(identifier=layer, bbox=bbox, format='GeoTIFF',
                                   crs=crs, resx=res, resy=res)

        tmp_path = os.path.join(RASTER_DIR, name + "_tmp.tif")
        with open(tmp_path, 'wb') as file:
            file.write(response.read())

        self.raster = rasterio.open(tmp_path, driver="GTiff")

    def mask(self, shape):
        out_img, out_transform = mask(dataset=self.raster, shapes=shape, crop=True)
        return out_img

    def get_geodataframe_mask(self):
        pass

    def apply_statistic(self, mask, handle_nulls_as):
        pass

    def get_plottable_representation(self):
        pass


rrm = RemoteWCSManager("https://sedac.ciesin.columbia.edu/geoserver/wcs",
                          "usgrid:usgrid-summary-file1-2000_usa-popsf1density-2000",
                          "us_2000_total_count")


def getFeatures(gdf, index):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][index]['geometry']]


from data_handler import StormDataHolder
harvey = StormDataHolder("Harvey")
harvey_warnings = harvey.storm_warnings



feature_to_extract = getFeatures(harvey_warnings.gdf, 30)
x = rrm.mask(feature_to_extract)

