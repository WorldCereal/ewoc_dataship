import json
import logging
import os
import re
from datetime import datetime, timedelta

import boto3
import numpy as np
import rasterio
from eodag import EODataAccessGateway
from eotile.eotile_module import main

logger = logging.getLogger(__name__)


def get_geom_from_id(tile_id):
    """
    Get Sentinel-2 tile footprint from it's ID
    :param tile_id: S2 tile id
    :return: GeoDataFrame with the footprint geometry
    """
    res = main(tile_id)
    return res[0]


def get_dates_from_prod_id(product_id):
    """
    Get date from product ID
    :param product_id: Product ID from EOdag
    :return: date string and type of sensor
    """
    # TODO update this function to use the direct eodag id search
    pid = product_id.split("_")
    sat_name = pid[0]
    sensor = ""
    if "S1" in sat_name:
        res = re.search(r"(?<=\_)(\d){8}(?=T)", product_id)
        date_tmp = res.group()
        sensor = "S1"
    elif "S2" in sat_name:
        res = re.search(r"(?<=\_)(\d){8}(?=T)", product_id)
        date_tmp = res.group()
        sensor = "S2"
    elif "LC08" in sat_name:
        sensor = "L8"
        date_tmp = pid[3]
    year = int(date_tmp[:4])
    month = int(date_tmp[4:6])
    day = int(date_tmp[6:8])
    date = datetime(year, month, day)
    start_date = date - timedelta(days=1)
    end_date = date + timedelta(days=1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), sensor


def donwload_s1tiling_style(dag, eodag_product, out_dir):
    """
    Reformat the downloaded data for s1tiling
    :param dag: EOdag EODataAccessGateway
    :param eodag_product: EOdag product from the seach results
    :param out_dir: Output directory
    """
    tmp_dir = os.path.join(out_dir, "tmp_" + eodag_product.properties["id"])
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    dag.download(eodag_product, outputs_prefix=tmp_dir)
    prod = os.listdir(tmp_dir)[0]
    prod_id = prod.split("_")
    dwn_prod = os.path.join(tmp_dir, prod)
    os.system(f"mv {dwn_prod} {dwn_prod}.SAFE")
    os.system(f"mkdir {dwn_prod}")
    os.system(f"mv {dwn_prod}.SAFE {dwn_prod}")
    os.system(f"mv {dwn_prod} {out_dir}")
    vv_name = f"s1a-iw-grd-vv-{prod_id[4].lower()}-{prod_id[5].lower()}-{prod_id[6].lower()}-{prod_id[7].lower()}-{prod_id[8].lower()}-001"
    vh_name = f"s1a-iw-grd-vh-{prod_id[4].lower()}-{prod_id[5].lower()}-{prod_id[6].lower()}-{prod_id[7].lower()}-{prod_id[8].lower()}-002"
    base = f"{out_dir}/{prod}/{prod}.SAFE"
    os.rename(
        f'{base}/{"annotation"}/iw-vh.xml', f'{base}/{"annotation"}/{vh_name}.xml'
    )
    os.rename(
        f'{base}/{"annotation"}/iw-vv.xml', f'{base}/{"annotation"}/{vv_name}.xml'
    )
    os.rename(
        f'{base}/{"measurement"}/iw-vh.tiff', f'{base}/{"measurement"}/{vh_name}.tiff'
    )
    os.rename(
        f'{base}/{"measurement"}/iw-vv.tiff', f'{base}/{"measurement"}/{vv_name}.tiff'
    )
    os.system(f"rm -r {tmp_dir}")


def download_s3file(s3_full_key, out_file, bucket):
    """
    Download file from s3 object storage
    :param s3_full_key: Object full path (bucket name, prefix, and key)
    :param out_file: Full path and name of the output file
    :param bucket: Bucket name
    """
    key = s3_full_key.split(bucket + "/")[1]
    product_id = os.path.split(s3_full_key)[-1]
    band_num = "_".join(product_id.split("_")[7:9])
    out_file += "_" + band_num
    s3 = boto3.resource("s3")
    object = s3.Object(bucket, key)
    resp = object.get(RequestPayer="requester")
    with open(out_file, "wb") as f:
        for chunk in iter(lambda: resp["Body"].read(4096), b""):
            f.write(chunk)


def get_bounds(tile_id):
    """
    Get S2 tile bounds
    :param tile_id: S2 tile id
    :return: Bounds coordinates
    """
    res = main(tile_id)
    UL0 = list(res[0]["UL0"])[0]
    UL1 = list(res[0]["UL1"])[0]
    # Return LL, UR tuple
    return (UL0, UL1 - 109800, UL0 + 109800, UL1)


def get_l8_rasters(data_folder):
    """
    Find Landsat 8 rasters
    :param data_folder: Input folder (any level)
    """
    l8_rasters = []
    for root, dirs, files in os.walk(data_folder):
        for file in files:
            if file.endswith((".tif", ".TIF")) and "LC08" in file:
                l8_rasters.append(os.path.join(root, file))
    return l8_rasters


def copy_tirs_s3(s3_full_key, out_dir, s2_tile):
    """
    Copy L8 Thermal bands from S3 bucket
    :param s3_full_key: Object full path (bucket name, prefix, and key)
    :param out_dir:
    :param s2_tile:
    :return:
    """
    product_id = os.path.split(s3_full_key)[-1]
    platform = product_id.split("_")[0]
    processing_level = product_id.split("_")[1]
    date = product_id.split("_")[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    out_dir = os.path.join(out_dir, "TIR")
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = os.path.join(
        out_dir, tile_id[:2], tile_id[2], tile_id[3:], year, date.split("T")[0]
    )
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    out_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    raster_fn = os.path.join(folder_st, dir_name, out_name)
    tmp = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp):
        os.makedirs(tmp)
    bucket = "usgs-landsat"
    download_s3file(s3_full_key, raster_fn, bucket)
    qa_key = s3_full_key.replace("ST_B10", "ST_QA")
    download_s3file(qa_key, raster_fn, bucket)
    # TODO Use logger instead
    print("Done for TIRS copy")


def get_product_by_id(
    product_id, out_dir, provider=None, config_file=None, product_type=None
):
    """
    Get satellite product with id using eodag
    :param product_id: id like S2A_MSIL1C_20200518T135121_N0209_R024_T21HTC_20200518T153019
    :param out_dir: Ouput directory
    :param provider: This is your data provider needed by eodag, could be different from the cloud provider
    :param config_file: Credentials for eodag, if none provided the credentials will be selected from env vars
    :param product_type: Product type, extra arg for eodag useful for creodias
    """
    if config_file is None:
        dag = EODataAccessGateway()
    else:
        dag = EODataAccessGateway(config_file)
    if provider is None:
        provider = os.getenv("EWOC_DATA_PROVIDER")
    dag.set_preferred_provider(provider)
    if product_type is not None:
        products, _ = dag.search(
            id=product_id, provider=provider, productType=product_type
        )
    else:
        products, _ = dag.search(id=product_id, provider=provider)
    if not products:
        logging.error("No results return by eodag!")
        raise ValueError
    dag.download(products[0], outputs_prefix=out_dir)
    # delete zip file
    list_out = os.listdir(out_dir)
    for item in list_out:
        if product_id in item and item.endswith("zip"):
            os.remove(os.path.join(out_dir, item))


def get_prods_from_json(json_file, out_dir, provider, sat="S2", config_file=None):
    """
    Bulk download using json workplan
    :param json_file: Path to workplan json file
    :param out_dir: Ouput directory
    :param provider: Data provider (creodias, peps, astraea_eod, ...)
    :param sat: S2/S1 or L8
    :param config_file: eodag config file
    """
    # Read json plan
    prod_types = {"S2": "S2_PROC", "L8": "L8_PROC", "S1": "SAR_PROC"}
    sat = prod_types[sat]
    with open(json_file) as f:
        plan = json.load(f)
    for tile in plan:
        out_dir = os.path.join(out_dir, tile)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        if sat == "S2_PROC":
            prods = plan[tile][sat]["INPUTS"]
            for prod in prods:
                get_product_by_id(
                    prod["id"], out_dir, provider, config_file=config_file
                )
        else:
            prods = plan[tile][sat]["INPUTS"]
            for prod in prods:
                get_product_by_id(prod, out_dir, provider, config_file=config_file)


def find_l2a_band(l2a_folder, band_num, res):
    """
    Find L2A band at specific resolution
    :param l2a_folder: L2A product folder
    :param band_num: BXX/AOT/SCL/...
    :param res: resolution (10/20/60)
    :return: path to band
    """
    band_path = None
    id_jp2 = f"{band_num}_{str(res)}m.jp2"
    id_tif = f"{band_num}_{str(res)}m.tif"
    for root, dirs, files in os.walk(l2a_folder):
        for file in files:
            if file.endswith((id_jp2, id_tif)):
                band_path = os.path.join(root, file)
    return band_path


def get_s2_prodname(safe_path):
    """
    Get Sentinel-2 product name
    :param safe_path: Path to SAFE folder
    :type safe_path: str
    :return: Product name
    :rtype: str
    """
    safe_split = safe_path.split("/")
    prodname = [item for item in safe_split if ".SAFE" in item][0]
    prodname = prodname.replace(".SAFE", "")
    return prodname


def raster_to_ard(raster_path, band_num, raster_fn):
    """
    Read raster and update internals to fit ewoc ard specs
    :param raster_path: Path to raster file
    :param band_num: Band number, B02 for example
    :param raster_fn: Output raster path
    """
    with rasterio.Env(GDAL_CACHEMAX=2048):
        with rasterio.open(raster_path, "r") as src:
            raster_array = src.read()
            meta = src.meta.copy()
    meta["driver"] = "GTiff"
    meta["nodata"] = 0
    bands_10m = ["B02", "B03", "B04", "B08"]
    blocksize = 512
    if band_num in bands_10m:
        blocksize = 1024
    with rasterio.open(
        raster_fn,
        "w+",
        **meta,
        tiled=True,
        compress="deflate",
        blockxsize=blocksize,
        blockysize=blocksize,
    ) as out:
        out.write(raster_array)


def binary_scl(scl_file, raster_fn):
    """
    Convert L2A SCL file to binary cloud mask
    :param scl_file: Path to SCL file
    :param raster_fn: Output binary mask path
    """
    with rasterio.open(scl_file, "r") as src:
        scl = src.read(1)

    # Set the to-be-masked SCL values
    SCL_MASK_VALUES = [0, 1, 3, 8, 9, 10, 11]

    # Set the nodata value in SCL
    SCL_NODATA_VALUE = 0

    # Contruct the final binary 0-1-255 mask
    mask = np.zeros_like(scl)
    mask[scl == SCL_NODATA_VALUE] = 255
    mask[~np.isin(scl, SCL_MASK_VALUES)] = 1

    meta = src.meta.copy()
    meta["driver"] = "GTiff"
    dtype = rasterio.uint8
    meta["dtype"] = dtype
    meta["nodata"] = 255

    with rasterio.open(
        raster_fn,
        "w+",
        **meta,
        compress="deflate",
        tiled=True,
        blockxsize=512,
        blockysize=512,
    ) as out:
        out.write(mask.astype(rasterio.uint8), 1)


def l2a_to_ard(l2a_folder, work_dir, only_scl=False):
    """
    Convert an L2A product into EWoC ARD format
    :param l2a_folder: L2A SAFE folder
    :param work_dir: Output directory
    """
    if only_scl:
        bands = {
            "SCL": 20,
        }
    else:
        bands = {
            "B02": 10,
            "B03": 10,
            "B04": 10,
            "B08": 10,
            "B05": 20,
            "B06": 20,
            "B07": 20,
            "B11": 20,
            "B12": 20,
            "SCL": 20,
        }
    # Prepare ewoc folder name
    prod_name = get_s2_prodname(l2a_folder)
    product_id = prod_name
    platform = product_id.split("_")[0]
    processing_level = product_id.split("_")[1]
    date = product_id.split("_")[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split("_")[5][1:]
    atcor_algo = "L2A"
    unique_id = "".join(product_id.split("_")[3:6])
    folder_st = os.path.join(
        work_dir,
        "OPTICAL",
        tile_id[:2],
        tile_id[2],
        tile_id[3:],
        year,
        date.split("T")[0],
    )
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    tmp_dir = os.path.join(folder_st, dir_name)
    ard_folder = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Convert bands and SCL
    for band in bands:
        res = bands[band]
        band_path = find_l2a_band(l2a_folder, band, bands[band])
        band_name = os.path.split(band_path)[-1]
        band_name = band_name.replace(".jp2", ".tif").replace(f"_{str(res)}m", "")
        print("Processing band " + band_name)
        out_name = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_{band}.tif"
        raster_fn = os.path.join(folder_st, dir_name, out_name)
        if band == "SCL":
            out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
            raster_cld = os.path.join(folder_st, dir_name, out_cld)
            binary_scl(band_path, raster_cld)
            print("Done --> " + raster_cld)
            try:
                os.remove(raster_cld + ".aux.xml")
            except:
                print("Clean")

        else:
            raster_to_ard(band_path, band, raster_fn)
            print("Done --> " + raster_fn)
    return ard_folder
