from raster_manager import *
from data_handler import StormDataHolder
from shapely.geometry import mapping

# Initiate a raster
rrm = RasterHandler(
    name="cdc_sovi",
    in_file='/Users/christopherjlowrie/Repos/FlashFloodResponse/tmp/tmp_c98cd518-43a0-11ea-a2ff-3035addaccec.tif'
)
rrm.set_no_data_mask("<", -1, np.nan)

# Initiate a storm
harvey = StormDataHolder("Harvey")
harvey_warnings = harvey.storm_warnings
extent = harvey_warnings.get_spatial_extent(buffer=0.1, as_geometry=True)
new_rst = rrm.mask_to_single_vector(mapping(extent))
rrm = RasterHandler(numpy_array=new_rst[0], meta=new_rst[1])

harvey_warning_raster_associations = rrm.get_vector_raster_associations(harvey_warnings)