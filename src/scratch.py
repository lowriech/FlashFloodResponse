# from data_handler import *
# from spacetime.AbstractXYTFunc import *

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
#
# def main():
#     from waze import WAZE_REGISTRY
#     print("Select a storm from the Waze Registry\n")
#
#     for i in [i["event"] for i in WAZE_REGISTRY]:
#         print("- {}".format(i))
#
#     print("\n====\n")
#     storm = input("What storm?\n").capitalize()
#
#     # Initiate a raster
#     rrm = RasterHandler(
#         name="cdc_sovi",
#         in_file=os.path.join(config.tmp, '/tmp/tmp_c98cd518-43a0-11ea-a2ff-3035addaccec.tif')
#     )
#     rrm.set_no_data_mask("<", -1, np.nan)
#
#     # Initiate a storm
#     harvey = StormDataHolder(storm)
#     harvey_warnings = harvey.storm_warnings
#     extent = harvey_warnings.get_spatial_extent(buffer=0.1, as_geometry=True)
#
#     # Update the raster to the extent of the storm
#     new_rst = rrm.mask_to_single_vector(mapping(extent))
#     rrm = RasterHandler(numpy_array=new_rst[0], meta=new_rst[1])
#
#     # Get the average svi of the storm region
#     regional_average_svi = rrm.get_statistic(np.nanmean)
#     print("Regional Average SVI: {}".format(str(regional_average_svi)))
#
#     # Map the warnings to underlying populations
#     harvey_warning_raster_associations = rrm.get_vector_raster_associations(harvey_warnings)
#
#     # Evaluate the populations more and less vulnerable than average
#     more_less_svi = harvey_warning_raster_associations.get_raster_handlers().applymap(
#         lambda x: x.get_statistic(np.nanmean)).applymap(
#         lambda x: x < regional_average_svi
#     )
#     less_svi_than_ave = more_less_svi[more_less_svi[0] == 1]
#     print("Number of warnings with less vulnerability than average: {}".format(str(len(less_svi_than_ave))))
#     more_svi_than_ave = more_less_svi[more_less_svi[0] == 0]
#     print("Number of warnings with more vulnerability than average: {}".format(str(len(more_svi_than_ave))))
#
#     harvey_warning_raster_associations.plot_master()
#     harvey.plot_space_time_containment()
