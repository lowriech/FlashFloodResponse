import os.path
from os import walk
import geopandas as gpd
import pandas as pd


def get_by_extension(folder, ext):
    for root, dirs, files in walk(folder):
        buffer = []
        for filename in files:
            if filename.endswith(ext):
                buffer.append(filename)
        return buffer


def get_gdf_by_directory(directory, traverse_subdirs=False, ext=".shp"):
    data = []
    for i in get_by_extension(directory, ext):
        p = os.path.join(directory, i)
        data.append(gpd.read_file(p))
    if traverse_subdirs:
        for root, dirs, files in walk(directory):
            for d in dirs:
                p = os.path.join(root, d)
                for i in get_by_extension(p, ext):
                    data.append(gpd.read_file(os.path.join(p, i)))
    return pd.concat(data)


def get_tmp_path(base_dir, suffix):
    import uuid
    return os.path.join(base_dir, "tmp_" + str(uuid.uuid1()) + suffix)
