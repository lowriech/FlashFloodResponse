from configuration import RASTER_DIR
import os.path
from owslib.wms import WebMapService
import rasterio
from rasterio.mask import mask

'''Starting work on a straightforward OWSLib wrapper for accessing raster data.
For more information, see https://geopython.github.io/OWSLib/'''

# TODO In Progress
class RemoteRasterManager:

    def __init__(self, server, layer, name):
        wms = WebMapService(server, version='1.1.1')
        img = wms.getmap(layers=[layer],
                         srs='EPSG:4326',
                         bbox=(-180.0, -90.0, 180.0, 90.0),
                         size=(500, 250),
                         format='image/jpeg',
                         transparent=True
                         )
        local_path = os.path.join(RASTER_DIR, name+".jpg")
        out = open(local_path, 'wb')
        out.write(img.read())
        out.close()
        self.local_path = local_path
        self.img = img
        self.raster = rasterio.open(local_path, crs='EPSG:4326')

    def mask(self, shape):
        out_img, out_transform = mask(dataset=self.raster, shapes=shape, crop=True)
        return out_img


rrm = RemoteRasterManager("https://sedac.ciesin.columbia.edu/geoserver/wms",
                          "usgrid:usgrid-summary-file1-2010_pop-count-2010",
                          "us_2010_total_count")


def getFeatures(gdf):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][0]['geometry']]


from data_handler import StormDataHolder
harvey = StormDataHolder("Harvey")
harvey_warnings = harvey.storm_warnings
feature_to_extract = getFeatures(harvey_warnings.gdf)
x = rrm.mask(feature_to_extract)

