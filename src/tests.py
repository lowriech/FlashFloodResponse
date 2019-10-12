from data_grabber import *
from waze import *


def test_waze():
    fetchAllWaze(DATA_PATH)

def test_storm_reports():
    time0 = datetime(2019, 10, 11, 4, 0)
    time1 = datetime(2019, 10, 12, 4, 0)
    x = StormReportHandler()
    x.fetch_storm_reports(time0, time1)

#test_waze()
# x = getWazeAsDataFrame(local_data_path + "waze/waze_harvey.txt")
# y = DataSet(local_data_path + "iowa_env_mesonet/2017_tsmf_sbw/wwa_201701010000_201712312359.shp").data
# index = analyzeWaze_x_NWS(x, y)
#https://mesonet.agron.iastate.edu/cgi-bin/request/gis/watchwarn.py?&year1=2019&month1=10&day1=11&hour1=04&minute1=00&year2=2019&month2=10&day2=12&hour2=16&minute2=00
