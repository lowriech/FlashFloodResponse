from datetime import datetime
import os.path
from os import walk

def get_by_extension(folder, ext):
    for root, dirs, files in walk(folder):
        buffer = []
        for filename in files:
            if filename.endswith(ext):
                buffer.append(filename)
        return buffer

# TODO: Deprecate, in favor of get_by_extension
def get_dotshp_from_shpdir(shpdir):
    for root, dirs, files in walk(shpdir):
        for filename in files:
            if filename.endswith(".shp"):
                print(root)
                return os.path.join(root, filename)