import json
import logging
import os
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import boto3
import numpy as np
import rasterio
import requests
from eodag import EODataAccessGateway
from eotile.eotile_module import main
from rasterio.merge import merge

from dataship.dag.s3man import download_s1_prd_from_creodias, download_s3file as dwnld_s3file

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


def merge_rasters(rasters, bounds, output_fn):
    """
    Merge a list of rasters and clip using bounds
    :param rasters: List of raster paths
    :param bounds: Bounds from get_bounds()
    :param output_fn: Full path and name of the mosaic
    """
    sources = []
    for raster in rasters:
        src = rasterio.open(raster)
        sources.append(src)
    merge(sources, dst_path=output_fn, method="max", bounds=bounds)
    for src in sources:
        src.close()


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


def list_path_bands(data_folder):
    """
    Organize the files by date and path
    :param data_folder:
    :return: L8 rasters dict
    """
    l8_rasters = get_l8_rasters(data_folder)
    merge_dict = {}
    # List all rows for the same path and same day
    for prod in l8_rasters:
        meta = prod.split("_")
        date = meta[3]
        path = meta[2][:3]
        prod_path = prod
        band = re.findall("(?<=\_T1_)(.*?)(?=\.)", prod)[0]
        # Should be a better way to fill up this dict
        if date in merge_dict:
            if path in merge_dict[date]:
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band] = []
                    merge_dict[date][path][band].append(prod_path)
            else:
                merge_dict[date][path] = {}
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band] = []
                    merge_dict[date][path][band].append(prod_path)
        else:
            merge_dict[date] = {}
            if path in merge_dict[date]:
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band] = []
                    merge_dict[date][path][band].append(prod_path)
            else:
                merge_dict[date][path] = {}
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band] = []
                    merge_dict[date][path][band].append(prod_path)
    return merge_dict


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


def s1_db(raster_path):
    """
    Convert Sentinel-1 to decibel
    :param raster_path: path to Sentinel-1 tif file
    """
    ds = rasterio.open(raster_path, "r")
    meta = ds.meta.copy()
    band = ds.read(1)
    # mask 0 values
    mask = band != 0
    db_mask = list(mask)
    ds.close()
    decibel = 10 * np.log10(band, where=db_mask)
    dn = 10.0 ** ((decibel + 83) / 20)
    dn[~mask] = 0
    dtype = rasterio.uint16
    meta["dtype"] = dtype
    meta["driver"] = "GTiff"
    meta["nodata"] = 0
    blocksize = 512
    with rasterio.open(
        raster_path,
        "w",
        **meta,
        compress="deflate",
        tiled=True,
        blockxsize=blocksize,
        blockysize=blocksize,
    ) as out:
        out.write(dn.astype(dtype), 1)


def s1db_folder(folder):
    """
    Convert all the S1 tif files in a folder
    :param folder: path to folder
    """
    sar_files = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if "s1" in file.lower() and file.endswith(("tif", "TIF")):
                sar_files.append(os.path.join(root, file))
    for sar_file in sar_files:
        print(
            f"Converting {sar_file} to db -> 10*log10(linear) and uint16 -> dn = 10.0 ** ((db + 83) / 20)"
        )
        s1_db(sar_file)
    return len(sar_files)


def get_srtm(tile_id, full_name=False):
    """
    Get srtm hgt files id for an S2 tile
    :param tile_id:
    :return: List of hgt files ids
    """
    res = main(tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    srtm_df = res[2]
    list_ids = list(srtm_df.id)
    if full_name:
        out = [f"{tile}.SRTMGL1.hgt.zip" for tile in list_ids]
        return out
    else:
        return list_ids


def get_srtm1s(s2_tile_id: str, out_dirpath: Path, source: str = "esa") -> None:
    """
    Retrieve srtm 1s data for a Sentinel-2 tile id from the source into the output dir
    :param s2_tile_ids: Sentinel-2 tile id
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    """
    get_srtm1s_from_ids(get_srtm1s_ids(s2_tile_id), out_dirpath, source=source)


def get_srtm1s_from_ids(
    srtm_tile_ids: List[str], out_dir: Path, source: str = "esa"
) -> None:
    """
    Retrieve srtm 1s data from the source into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    """
    if source == "esa":
        logger.debug("Use ESA website to retrieve the srtm 1s data!")
        get_srtm_from_esa(srtm_tile_ids, out_dir)
    elif source == "creodias-bucket":
        logger.debug("Use creodias s3 bucket to retrieve srtm 1s data!")
        raise NotImplementedError
    elif source == "local-bucket":
        logger.debug("Use local s3 bucket to retrieve srtm 1s data!")
        get_srtm_from_local_bucket(srtm_tile_ids, out_dir)
    elif source == "usgs":
        logger.debug("Use usgs EE to retrieve srtm 1s data!")
        raise NotImplementedError
    else:
        logger.error("Source %s not supported!", source)


def get_srtm_from_local_bucket(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from local bucket into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    local_bucket_name = "world-cereal"
    for srtm_tile_id in srtm_tile_ids:
        srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
        srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
        dwnld_s3file("srtm30/" + srtm_tile_id_filename,
                     str(srtm_tile_id_filepath),
                     local_bucket_name)
        logger.debug("%s downloaded!", srtm_tile_id_filename)

        with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
            srtm_zipfile.extractall(out_dirpath)

        srtm_tile_id_filepath.unlink()


def get_srtm_from_esa(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from ESA website into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    ESA_WEBSITE_ROOT = "http://step.esa.int/auxdata/dem/SRTMGL1/"
    for srtm_tile_id in srtm_tile_ids:
        srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
        srtm_tile_id_url = ESA_WEBSITE_ROOT + srtm_tile_id_filename

        r = requests.get(srtm_tile_id_url)
        if r.status_code != requests.codes.ok:
            logger.error(
                "%s not dwnloaded (error_code: %s) from %s!",
                srtm_tile_id_filename,
                r.status_code,
                srtm_tile_id_url,
            )
            continue
        else:
            logger.debug("%s downloaded!", srtm_tile_id_filename)

        srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
        with open(srtm_tile_id_filepath, "wb") as srtm_file:
            srtm_file.write(r.content)

        with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
            srtm_zipfile.extractall(out_dirpath)

        srtm_tile_id_filepath.unlink()


def get_srtm1s_ids(s2_tile_id: str) -> None:
    """
    Get srtm 1s id for an S2 tile
    :param s2 tile_id:
    :return: List of srtm ids
    """
    res = main(s2_tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    return list(res[2].id)


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


def get_s1_product(prd_id:str, out_root_dirpath:Path, source:str='creodias_eodata', eodag_config_file=None):
    """
    Retrieve Sentinel-1 data via eodag or directly from a object storage
    """
    if source is None:
        source = os.getenv('EWOC_DATA_SOURCE')

    if source == 'creodias_finder':
        get_product_by_id(prd_id, out_root_dirpath, provider='creodias', config_file=eodag_config_file)
    elif source == 'creodias_eodata':
        download_s1_prd_from_creodias(prd_id, out_root_dirpath)
    elif source == 'aws_s3':
        raise NotImplementedError('Get S1 product from AWS bucket is not currently implemented!')
    else:
        if eodag_config_file is not None:
            data_provider=os.getenv('EWOC_DATA_PROVIDER')
            logging.info('Use EODAG to retrieve the Sentinel-1 product with the following data provider %s.', data_provider)
            get_product_by_id(prd_id, out_root_dirpath, provider=data_provider)
        else:
            raise NotImplementedError

def get_s1_product_by_id(product_id, out_dir, provider=None, config_file=None):
    """
    Wrapper around get_product_by_id adapted for Sentinel-1 on creodias
    :param product_id: something like S1B_IW_GRDH_1SDV_20200510T092220_20200510T092245_021517_028DAB_A416
    :param out_dir: Ouptut directory
    :param provider: Data provider (creodias)
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """
    if provider is None:
        provider = os.getenv("EWOC_DATA_PROVIDER")

    if provider == "creodias":
        get_product_by_id(
            product_id,
            out_dir,
            provider=provider,
            config_file=config_file,
            product_type="S1_SAR_GRD",
        )
    else:
        get_product_by_id(
            product_id, out_dir, provider=provider, config_file=config_file
        )


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
    id = f"{band_num}_{str(res)}m.jp2"
    for root, dirs, files in os.walk(l2a_folder):
        for file in files:
            if file.endswith(id):
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
        ds = src.read(1)
    # For SCL flag all cloud pixels
    ds[ds == 10] = 0
    ds[ds == 9] = 0
    ds[ds == 8] = 0
    ds[ds == 1] = 0
    ds[ds == 3] = 0
    ds[ds != 0] = 1
    meta = src.meta.copy()
    meta["driver"] = "GTiff"
    dtype = rasterio.uint8
    meta["dtype"] = dtype

    with rasterio.open(
        raster_fn,
        "w+",
        **meta,
        compress="deflate",
        tiled=True,
        blockxsize=512,
        blockysize=512,
    ) as out:
        out.write(ds.astype(rasterio.uint8), 1)


def l2a_to_ard(l2a_folder, work_dir):
    """
    Convert an L2A product into EWoC ARD format
    :param l2a_folder: L2A SAFE folder
    :param work_dir: Output directory
    """
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
