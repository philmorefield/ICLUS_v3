import os

from arcpy.sa import Con, IsNull

OUTPUT_GEODATABASE = 'D:\\OneDrive\\ICLUS_v3\\geospatial\\iclusv3_geospatial.gdb'
COUNTY_RASTER = os.path.join(OUTPUT_GEODATABASE, 'conus_ak_hi_2020')
NLCD_RASTER = os.path.join(OUTPUT_GEODATABASE, 'nlcd_2021_land_cover_l48_20230630_NN')


def main():
    output_raster = Con(~IsNull(NLCD_RASTER), Con(IsNull(COUNTY_RASTER), 1, COUNTY_RASTER))
    output_raster.save("D:\\OneDrive\\ICLUS_v3\\geospatial\\iclusv3_geospatial.gdb\\conus_ak_hi_2020_nibble")

if __name__ == '__main__':
    main()