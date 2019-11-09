from datetime import datetime
import os.path
from os import walk

def convert_numeric_to_datetime(time):
    time = str(time)
    return datetime(int(time[0:4]),
                    int(time[4:6]),
                    int(time[6:8]),
                    int(time[8:10]),
                    int(time[10:12]))

def get_dotshp_from_shpdir(shpdir):
    for root, dirs, files in walk(shpdir):
        for filename in files:
            if filename.endswith(".shp"):
                print(root)
                return os.path.join(root, filename)