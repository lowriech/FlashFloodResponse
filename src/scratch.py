from data_handler import *

waze = WazeHandler("Harvey")
warn = StormWarningHandler(2017)
warn.prep_data_variables()
poly = AbstractGeoHandler(local_shp_path=ZCTA_PATH)
poly.cut_to_extent(waze.get_spatial_extent())
warn.cut_to_extent(waze.get_spatial_extent())

poly.gdf.plot()
waze.gdf.plot()

plt.subplots(2,1)
ax = poly.gdf.plot()
waze.gdf.plot(ax=ax, column="time", markersize=3)
plt.show()

x = pd.read_csv("/Users/christopherjlowrie/Repos/FlashFloodResponse/data/us_census/zcta5/resources/poverty/ACS_17_5YR_B17003_with_ann.csv")
x = x[['GEO.id2', 'HD01_VD01']].set_index('GEO.id2')

z1 = AbstractSpaceTimeFunctionality.space_time_containment(waze, warn)
print(z1)

z2 = AbstractSpaceTimeFunctionality.get_distinct_points_by_space_time_coverage(waze, warn)

ax1 = poly.gdf.plot()
z2[z2["has_overlap"] == 1].plot(ax=ax1, color="red", markersize=3)
ax2 = poly.gdf.plot()
z2[z2["has_overlap"] == 0].plot(ax=ax2, color="blue", markersize=3)
plt.show()

z3 = AbstractSpaceTimeFunctionality.count_points_per_geography(poly, waze, collect_on="time")
print(z3)