from configuration import config
import os.path
from owslib.wcs import WebCoverageService
import rasterio
from rasterio.mask import mask
import pandas as pd
from rasterio.plot import show
import matplotlib.pyplot as plt
import numpy as np
from utils import get_tmp_path
from shapely.geometry import mapping


"""
OWSLib/Rasterio wrapper for common functionality.
This will be used if the project ends up requiring Social Vulnerability Indices or DEM data.

Still in progress, code not yet clean.
"""


class VectorRasterAssociation:
    # TODO create basic raster/vector accessor methods, and move the plotting functionality to its own class

    def __init__(self, gdf, rasters, master_raster):

        self.geometry = gdf
        self.rasters = rasters
        self.master_raster = master_raster

    def get_merged_view(self):
        return self.geometry.join(self.rasters)

    def get_raster_handlers(self):
        return pd.DataFrame(
            [RasterHandler(
                numpy_array=self.rasters['raster'][i],
                meta=self.rasters['meta'][i])
                for i in self.rasters.index],
            index=self.rasters.index
        )

    def plot_master(self):
        master_copy = self.master_raster
        f, (ax1, ax2, ax3) = plt.subplots(1, 3, sharex=True)
        # Plot 1
        rasterio.plot.show(master_copy.get_open_raster(), ax=ax1, cmap='OrRd')

        # Plot 2
        self.geometry.plot(ax=ax2, facecolor='k', edgecolor='k', alpha=0.15)

        # Plot 3
        rasterio.plot.show(master_copy.get_open_raster(), ax=ax3, cmap='OrRd')
        self.geometry.plot(ax=ax3, facecolor='k', edgecolor='k', alpha=0.1)

        plt.show()

    def plot_association_by_index(self, index):
        fig, ax = plt.subplots(figsize=(15, 15))
        r = self.rasters[self.rasters.index == index]
        # TODO .item() is deprecated, replace
        rh = RasterHandler(numpy_array=r["raster"].item(), meta=r["meta"].item())
        rasterio.plot.show(rh.get_open_raster(), cmap='OrRd', ax=ax)
        self.geometry[self.geometry.index == index].plot(ax=ax, facecolor='none', edgecolor='k', alpha=0.5)
        txt_string = "{} average\n{} total\n{} warning area (still in Decimal Degrees for now)"
        plt.text(0, 0, txt_string.format(
            str(rh.get_statistic(np.nanmean)),
            str(rh.get_statistic(np.nansum)),
            str(self.geometry[self.geometry.index == index]["geometry"].item().area)),
                 transform=ax.transAxes
                 )
        plt.title("Statistics based off {}".format(self.master_raster.name))
        plt.show()


class WCSWrapper:

    def __init__(self, server, version='1.0.0', crs='EPSG:4326'):
        self.server = server
        self.version = version
        self.crs = crs

    def get_layer(self, layer, res=0.05):
        import time
        wcs = WebCoverageService(self.server, version=self.version)
        layer_to_use = wcs.contents[layer]
        bbox = layer_to_use.boundingBoxWGS84
        response = wcs.getCoverage(identifier=layer, bbox=bbox, format='GeoTIFF',
                                   crs=self.crs, resx=res, resy=res)
        tmp_path = os.path.join(config.tmp, "tmp_" + str(int(time.time()*100)) + ".tif")
        with open(tmp_path, 'wb') as file:
            file.write(response.read())

        return tmp_path


class RasterHandler:

    def __init__(self,
                 server=None, layer=None,
                 in_file=None,
                 numpy_array=None, meta=None,
                 name=None,
                 version='1.0.0', crs='EPSG:4326', res=0.05):

        self.crs = crs
        self.name = name

        if in_file:
            with rasterio.open(in_file, driver="GTiff") as r:
                self.numpy_array = r.read()
                self.meta = r.meta.copy()
                self.raster_path = in_file

        elif server:
            wcs = WCSWrapper(server, version, crs)
            tmp_path = wcs.get_layer(layer, res)
            with rasterio.open(tmp_path, driver="GTiff") as r:
                self.numpy_array = r.read()
                self.meta = r.meta.copy()
                self.raster_path = tmp_path

        else:
            self.numpy_array = numpy_array
            self.meta = meta
            self.write_numpy_to_raster()

    def write_numpy_to_raster(self):
        tmp_path = get_tmp_path(TMP_DIR, ".tif")
        with rasterio.open(
            tmp_path,
            'w',
            driver='GTiff',
            height=self.numpy_array.shape[1],
            width=self.numpy_array.shape[2],
            count=1,
            dtype=self.numpy_array.dtype,
            crs=self.crs,
            transform=self.meta["transform"],
        ) as r:
            r.write(self.numpy_array[0], 1)
        self.raster_path = tmp_path
        return tmp_path

    def get_open_raster(self):
        return rasterio.open(self.raster_path, driver="GTiff")

    def get_statistic(self, f):
        return f(self.numpy_array)

    def get_vector_raster_associations(self, geo_handler):

        tmp = geo_handler.gdf
        geojson = tmp["geometry"].map(lambda shape: mapping(shape)).map(self.mask_to_single_vector)

        x = pd.DataFrame(
            geojson.values.tolist(), index=geojson.index
        )
        x.columns = ("raster", "meta")

        return VectorRasterAssociation(tmp, x, self)

    def set_no_data_mask(self, relate, cut, set_to=np.nan):
        import operator

        def get_truth(relate, cut):
            ops = {'>': operator.gt,
                   '<': operator.lt,
                   '>=': operator.ge,
                   '<=': operator.le,
                   '=': operator.eq}
            return ops[relate](self.numpy_array, cut)

        self.numpy_array[get_truth(relate, cut)] = set_to
        self.write_numpy_to_raster()

    def mask_to_single_vector(self, geojson):
        with rasterio.open(self.raster_path) as r:
            out_img, out_transform = mask(dataset=r, shapes=[geojson], crop=True, nodata=np.nan)
            out_meta = r.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": out_img.shape[1],
                "width": out_img.shape[2],
                "transform": out_transform
            })
            return out_img, out_meta

