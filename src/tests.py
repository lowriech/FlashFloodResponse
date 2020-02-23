from data_handler import *
from waze import *

#TODO: Robust Test Cases


def test_waze():
    fetchAllWaze(DATA_PATH)
    for w in WAZE_REGISTRY:
        data = StormDataHolder(w["event"])


def test_waze_k_function():
    waze = WazeHandler("Harvey")
    waze.prep_data()
    x = waze.k_function()
    y = pd.DataFrame(x.keys(), x.values())
    y.plot()
    plt.show()

    return x


def test_storm_reports():
    waze = WazeHandler("Harvey")
    extent = {
            "temporal": waze.get_temporal_extent(as_datetime=True),
            "spatial": waze.get_spatial_extent()
        }

    storm_reports = iterative_fetch(extent, StormReportPointHandler)
    storm_reports.prep_data()
    x = storm_reports.k_function()
    y = pd.DataFrame(x.keys(), x.values())
    # import math
    # a = pd.Series(list(x.keys()))
    # print(a)
    # b = a*a*math.pi
    # z = pd.DataFrame(a, b)

    y.plot()
    plt.show()

    return x

test_storm_reports()
