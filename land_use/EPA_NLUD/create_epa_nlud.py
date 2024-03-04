"""
Author: Phil Morefield
Purpose: EPA NLUD for ICLUS version 3
Created: 8/22/2018

20201106 - this version will use a uniform set of class breaks when assigning
           residential classes so - for example - urban residential pixels may
           be found even in what we would traditionally call rural areas. The
           "fancy residential" version restricts residential classes that
           overlap with Census UAC shapefiles to suburban and greater.
"""
from multiprocessing import Pool
import os

import arcpy as ap
from arcpy.sa import Con, IsNull

import nlud


ap.CheckOutExtension('Spatial')

ap.env.overwriteOutput = True
ap.env.parallelProcessingFactor = 8
ap.env.pyramid = "PYRAMIDS -1 NEAREST LZ77 NO_SKIP"

RASTER_VALUES = {'Transportation': 21,
                 'InstitutionalDeveloped': 20,
                 'InstitutionalUndeveloped': 19,
                 'Industrial': 18,
                 'Commercial': 17,
                 'MixedUse': 16,
                 'HighDensityUrban': 15,
                 'Urban': 14,
                 'Grayfield': 13,
                 'Suburban': 12,
                 'Exurban': 11,
                 'Rural': 10,
                 'ParksGolf': 9,
                 'Cropland': 8,
                 'Pasture': 7,
                 'PrivateGrassShrub': 6,
                 'PrivateForest': 5,
                 'RecreationExtraction': 4,
                 'Conservation': 3,
                 'Wetlands': 2,
                 'Water': 1}


def combine_all_layers(kwargs):
    print(f"[Region {kwargs['REGION_NUMBER']}]   Combining all land use layers...")

    REGION_NUMBER = kwargs['REGION_NUMBER']
    OUTPUTS = kwargs['OUTPUTS']
    input_CLR_file = 'D:\\projects\\EPA_NLUD\\secondary\\EPA_NLUD_colormap_compare_20191230.clr'

    raster_list = [os.path.join(OUTPUTS, f'FINAL_{key.upper()}') for key in RASTER_VALUES]

    # Census Block
    transportation = ap.Raster(os.path.join(OUTPUTS, 'FINAL_TRANSPORTATION'))
    water = ap.Raster(os.path.join(OUTPUTS, 'FINAL_WATER'))
    wetlands = ap.Raster(os.path.join(OUTPUTS, 'FINAL_WETLANDS'))
    conservation = ap.Raster(os.path.join(OUTPUTS, 'FINAL_CONSERVATION'))
    parksgolf = ap.Raster(os.path.join(OUTPUTS, 'FINAL_PARKSGOLF'))

    max_value_raster = ap.sa.CellStatistics(in_rasters_or_constants=raster_list,
                                            statistics_type='MAXIMUM')
    out_raster = Con(~IsNull(transportation), transportation, Con(~IsNull(water), water, Con(~IsNull(wetlands), wetlands, Con(~IsNull(conservation), conservation, Con(~IsNull(parksgolf), parksgolf, max_value_raster)))))
    out_raster.save(os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}'))

    in_raster = os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}')

    ap.AddColormap_management(in_raster=in_raster, input_CLR_file=input_CLR_file)

    # Census Block Group
    BGs = ('Commercial', 'Industrial', 'MixedUse', 'HighDensityUrban', 'Urban', 'Suburban', 'Exurban', 'Rural')
    raster_list = [os.path.join(OUTPUTS, f'FINAL_{key.upper()}_BG') if key in BGs else os.path.join(OUTPUTS, f'FINAL_{key.upper()}') for key in RASTER_VALUES]

    max_value_raster = ap.sa.CellStatistics(in_rasters_or_constants=raster_list,
                                            statistics_type='MAXIMUM')
    out_raster = Con(~IsNull(transportation), transportation, Con(~IsNull(water), water, Con(~IsNull(wetlands), wetlands, Con(~IsNull(conservation), conservation, Con(~IsNull(parksgolf), parksgolf, max_value_raster)))))
    out_raster.save(os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}_BG'))

    in_raster = os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}_BG')
    ap.AddColormap_management(in_raster=in_raster, input_CLR_file=input_CLR_file)

    print(f"[Region {kwargs['REGION_NUMBER']}]   Finished!")


def mosaic_regions(region_numbers):
    '''
    Census Block
    '''
    input_rasters = []
    for num in region_numbers:
        input_rasters.append(f'D:\\projects\\EPA_NLUD\\outputs\\regional_wksps\\region_{num}\\OUTPUTS_2010.gdb\\NLUD_2010_R{num}')

    print("Mosaicking CONUS rasters...", end='')
    ap.MosaicToNewRaster_management(input_rasters=input_rasters,
                                    output_location='D:\\projects\\EPA_NLUD\\outputs\\CONUS.gdb',
                                    raster_dataset_name_with_extension='EPA_NLUD_2010_CONUS',
                                    pixel_type='8_BIT_UNSIGNED',
                                    cellsize=30,
                                    number_of_bands=1)

    # Census Block Group
    input_rasters = []
    for num in region_numbers:
        input_rasters.append(f'D:\\projects\\EPA_NLUD\\outputs\\regional_wksps\\region_{num}\\OUTPUTS_2010.gdb\\NLUD_2010_R{num}_BG')

    ap.MosaicToNewRaster_management(input_rasters=input_rasters,
                                    output_location='D:\\projects\\EPA_NLUD\\outputs\\CONUS.gdb',
                                    raster_dataset_name_with_extension='EPA_NLUD_2010_CONUS_BG',
                                    pixel_type='8_BIT_UNSIGNED',
                                    cellsize=30,
                                    number_of_bands=1)

    print("finished!")


def worker_function(region_number=None):
    kws = {'REGION_NUMBER': region_number}
    kws['INPUTS'] = f'D:\\projects\\EPA_NLUD\\outputs\\regional_wksps\\region_{region_number}\\INPUTS_2010.gdb'

    kws['OUTPUTS'] = f'D:\\projects\\EPA_NLUD\\outputs\\regional_wksps\\region_{region_number}\\OUTPUTS_2010.gdb'
    kws['INTERMEDIATE'] = f'D:\\projects\\EPA_NLUD\\outputs\\regional_wksps\\region_{region_number}\\INTERMEDIATE_2010.gdb'

    if not ap.Exists(kws['INTERMEDIATE']):
        ap.CreateFileGDB_management(out_folder_path=os.path.dirname(kws['INTERMEDIATE']),
                                    out_name=os.path.basename(kws['INTERMEDIATE']))

    if not ap.Exists(kws['OUTPUTS']):
        ap.CreateFileGDB_management(out_folder_path=os.path.dirname(kws['OUTPUTS']),
                                    out_name=os.path.basename(kws['OUTPUTS']))

    # when some files need to be deleted, e.g., naming convention change
    if ap.Exists(os.path.join(kws['INTERMEDIATE'], 'SILVIS_GRAYFIELD_BG')):
        in_data = os.path.join(kws['INTERMEDIATE'], 'SILVIS_GRAYFIELD_BG')
        print("\n\tDeleting", in_data, "\n")
        ap.Delete_management(in_data=in_data)

    if ap.Exists(os.path.join(kws['OUTPUTS'], 'FINAL_GRAYFIELD_BG')):
        in_data = os.path.join(kws['OUTPUTS'], 'FINAL_GRAYFIELD_BG')
        print("\n\tDeleting", in_data, "\n")
        ap.Delete_management(in_data=in_data)

    kws['comm'] = ap.Raster(os.path.join(kws['INPUTS'], 'COMMDEN10'))
    kws['indust'] = ap.Raster(os.path.join(kws['INPUTS'], 'INDUSTDEN10'))
    kws['huden'] = ap.Raster(os.path.join(kws['INPUTS'], 'HUDEN10'))
    kws['popden'] = ap.Raster(os.path.join(kws['INPUTS'], 'POPDEN10'))
    kws['jobden'] = ap.Raster(os.path.join(kws['INPUTS'], 'JOBDEN10'))

    kws['comm_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'COMMDEN10_BG'))
    kws['indust_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'INDUSTDEN10_BG'))
    kws['huden_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'HUDEN10_BG'))
    kws['popden_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'POPDEN10_BG'))
    kws['jobden_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'JOBDEN10_BG'))

    kws['urban_areas'] = ap.Raster(os.path.join(kws['INPUTS'], 'UAC10'))
    kws['place'] = ap.Raster(os.path.join(kws['INPUTS'], 'PLACE10'))
    kws['nlcd_ras'] = ap.Raster(os.path.join(kws['INPUTS'], 'NLCD_2011_Land_Cover_L48_20190424_roads_thinned'))

    kws['RASTER_VALUES'] = RASTER_VALUES

    ap.env.snapRaster = kws['nlcd_ras']
    ap.env.extent = kws['nlcd_ras']
    ap.env.mask = kws['nlcd_ras']

    nlud.Transportation(kwargs=kws)
    # nlud.Institutional(kwargs=kws)
    # nlud.MixedUse(kwargs=kws)
    # nlud.Industrial(kwargs=kws)
    # nlud.Commercial(kwargs=kws)
    # nlud.HighDensityUrban(kwargs=kws)
    # nlud.Urban(kwargs=kws)
    # nlud.Grayfield(kwargs=kws)
    # nlud.Suburban(kwargs=kws)
    # nlud.Exurban(kwargs=kws)
    # nlud.Rural(kwargs=kws)
    # nlud.ParksGolf(kwargs=kws)
    # nlud.Cropland(kwargs=kws)
    # nlud.Pasture(kwargs=kws)
    # nlud.PrivateGrassShrub(kwargs=kws)
    # nlud.PrivateForest(kwargs=kws)
    # nlud.RecreationExtraction(kwargs=kws)
    # nlud.Conservation(kwargs=kws)
    # nlud.Wetlands(kwargs=kws)
    # nlud.Water(kwargs=kws)

    combine_all_layers(kws)


def blend_layers():
    '''
    Use the Block Group version of the NLUD where ever the Block version has a
    null value.
    '''
    print("Blending NLUD layers...", end="")
    inputs_gdb = 'D:\\projects\\EPA_NLUD\\inputs\\geodatabases\\INPUTS_2010.gdb'
    nlcd = ap.Raster(os.path.join(inputs_gdb, 'NLCD_2011_Land_Cover_L48_20190424_roads_thinned'))

    gdb = 'D:\\projects\\EPA_NLUD\\outputs\\CONUS.gdb'
    block_raster = ap.Raster(os.path.join(gdb, 'EPA_NLUD_2010_CONUS'))
    block_group_raster = ap.Raster(os.path.join(gdb, 'EPA_NLUD_2010_CONUS_BG'))

    wetlands_value = RASTER_VALUES['Wetlands']

    # Use the Block Group-level NLUD to fill-in gaps in the Block-level NLUD
    # If there are any remaining gaps, fill them in with NLCD wetlands as
    # appropriate.
    final = Con(IsNull(block_raster), Con(~IsNull(block_group_raster), block_group_raster, Con((nlcd == 90) | (nlcd == 95), wetlands_value)), block_raster)

    final.save(os.path.join(gdb, 'EPA_NLUD_2010_CONUS_FINAL'))

    in_raster = os.path.join(gdb, 'EPA_NLUD_2010_CONUS_FINAL')
    input_CLR_file = 'D:\\projects\\EPA_NLUD\\secondary\\EPA_NLUD_colormap_compare_20191230.clr'
    ap.AddColormap_management(in_raster=in_raster, input_CLR_file=input_CLR_file)

    ap.BuildPyramidsandStatistics_management(in_workspace=in_raster,
                                             include_subdirectories='NONE',
                                             skip_existing='OVERWRITE')

    print("finished!")


def main():

    regions_to_process = range(1, 8)
    regions_to_mosaick = range(1, 8)

    pool = Pool(processes=7, maxtasksperchild=1)
    pool.map(func=worker_function, iterable=regions_to_process)
    pool.close()
    pool.terminate()

    mosaic_regions(regions_to_mosaick)
    blend_layers()


if __name__ == '__main__':
    main()
