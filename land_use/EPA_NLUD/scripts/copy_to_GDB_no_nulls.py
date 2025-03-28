import glob
import os

import arcpy as ap

from arcpy.sa import IsNull, Con

ap.env.overwriteOutput = True
ap.env.parallelProcessingFactor = "100%"
ap.env.scratchWorkspace = 'D:\\temp'


def main():
    out_gdb = 'F:\\data\\NLCD\\landcover.gdb'
    input_folder = 'F:\\data\\NLCD'
    in_rasters = glob.glob(os.path.join(input_folder, '*20230630.img'))

    for in_raster in in_rasters:
        out_name = os.path.basename(in_raster)[:-4]
        out_raster_nn = Con(~IsNull(in_raster), in_raster)
        out_raster = Con(out_raster_nn > 0, out_raster_nn)
        out_raster.save(os.path.join(out_gdb, out_name))
        print("Finished:", out_name)


if __name__ == '__main__':
    main()
