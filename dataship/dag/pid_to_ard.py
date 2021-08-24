import os
import logging
from dataship.classes.s1_prd_id import S1PrdIdInfo

logger = logging.getLogger(__name__)

def l2a_to_ard(product_id):
    """
    Convert an L2A product into EWoC ARD format
    :param l2a_folder: L2A SAFE folder
    :param work_dir: Output directory
    """

    platform = product_id.split("_")[0]
    processing_level = product_id.split("_")[1]
    date = product_id.split("_")[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split("_")[5][1:]
    unique_id = "".join(product_id.split("_")[3:6])
    folder_st = os.path.join(
        "OPTICAL",
        tile_id[:2],
        tile_id[2],
        tile_id[3:],
        year,
        date.split("T")[0],
    )
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    ard_folder = os.path.join(folder_st, dir_name)
    return ard_folder


def l8_to_ard(key,s2_tile,out_dir=None):
    product_id = os.path.split(key)[-1]
    platform = product_id.split('_')[0]
    processing_level = product_id.split('_')[1]
    date = product_id.split('_')[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = os.path.join('TIR', tile_id[:2], tile_id[2], tile_id[3:], year,date.split('T')[0])
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    out_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    raster_fn = os.path.join(folder_st, dir_name, out_name)
    if out_dir is not None:
        tmp = os.path.join(out_dir, folder_st, dir_name)
        if not os.path.exists(tmp):
            os.makedirs(tmp)
    return raster_fn


def to_ewoc_s1_ard(out_dirpath,
                   s1_prd_info,
                   s2_tile_id):
    s1_prd_info = S1PrdIdInfo(s1_prd_info)  # Transformation to a S1 EO product
    orbit_direction = 'DES'  # TODO retrieve from GDAL MTD of the output s1_process file or from mtd of the input product
    relative_orbit = 'TODO'  # TODO retrieve from GDAL MTD of the output s1_process file or from mtd of the input product

    ewoc_output_dirname_elt = [s1_prd_info.mission_id,
                               s1_prd_info.start_time.strftime(s1_prd_info.FORMAT_DATETIME),
                               orbit_direction,
                               relative_orbit,
                               s1_prd_info.absolute_orbit_number + s1_prd_info.mission_datatake_id + s1_prd_info.product_unique_id,
                               s2_tile_id]
    ewoc_output_dirname = '_'.join(ewoc_output_dirname_elt)
    ewoc_output_dirpath = out_dirpath / 'SAR' / s2_tile_id[:2] / s2_tile_id[2] / s2_tile_id[3:] / \
                          str(s1_prd_info.start_time.year) / s1_prd_info.start_time.date().strftime(
        '%Y%m%d') / ewoc_output_dirname
    logger.debug('Create output directory: %s', ewoc_output_dirpath)
    ewoc_output_dirpath.mkdir(exist_ok=True, parents=True)

    calibration_type = 'SIGMA0'  # TODO retrieve from GDAL MTD of the output s1_process file or from parameters
    output_file_ext = '.tif'
    ewoc_output_filename_elt = ewoc_output_dirname_elt + [calibration_type]
    ewoc_output_filename_vv = '_'.join(ewoc_output_filename_elt + ['VV']) + output_file_ext
    ewoc_output_filepath_vv = ewoc_output_dirpath / ewoc_output_filename_vv
    logger.debug('Output VV filepath: %s', ewoc_output_filepath_vv)
    ewoc_output_filename_vh = '_'.join(ewoc_output_filename_elt + ['VH']) + output_file_ext
    ewoc_output_filepath_vh = ewoc_output_dirpath / ewoc_output_filename_vh
    logger.debug('Output VH filepath: %s', ewoc_output_filepath_vh)

    return ewoc_output_filepath_vv, ewoc_output_filepath_vh
