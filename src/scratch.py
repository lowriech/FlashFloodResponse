# from data_handler import *
# from AbstractXYTFunctionality.AbstractXYTFunc import *

# def construct_zcta_summary(self):
#     y = self.get_geographies_and_waze_times()
#     pov = self.geographies.gdf[["ZCTA5", "PercPov"]].set_index("ZCTA5")
#     x = y.groupby('ZCTA5')['time']
#     x_counts = x.count().reset_index(name="count")["count"]
#     x = x.apply(list).reset_index(name="times")
#     x["counts"] = x_counts
#     x = x.set_index("ZCTA5")
#     x = x.merge(pov, left_index=True, right_index=True)
#     return x
#
# def plot_time_density_for_zcta(self, zcta):
#     zcta = str(zcta)
#     x = self.get_geographies_and_waze_times().groupby('ZCTA5')['time'].apply(list).reset_index(name='times')
#     times = x[x["ZCTA5"] == zcta]['times'].values[0]
#     sbn.set_style('darkgrid')
#     sbn.distplot(times)
#     plt.show()
#
# def cdc_vector_to_raster():
#     from data_handler import AbstractGeoHandler
#     x = AbstractGeoHandler(
#         local_shp_path='/Users/christopherjlowrie/Repos/FlashFloodResponse/data/cdc_sovi/SVI2016_US/SVI2016_US.shp'
#     )
#     return x.rasterize_from_template('RPL_THEMES')
