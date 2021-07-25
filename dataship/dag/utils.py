import json
import os
import re
import shutil
from datetime import datetime, timedelta

import boto3
import geopandas as gpd
import numpy as np
import pkg_resources
import rasterio
from eodag import EODataAccessGateway
from eotile.eotile_module import main
from rasterio.merge import merge
from tqdm import tqdm

# Replace this with eotile later
index_path = pkg_resources.resource_filename(__name__, os.path.join("../index", "s2_idx.geojson"))


def get_geom_from_id(tile_id):
    """
    Get Sentinel-2 tile footprint from it's ID
    :param tile_id: S2 tile id
    :return: GeoDataFrame with the footprint geometry
    """

    s2_grid = gpd.read_file(index_path)
    return s2_grid[s2_grid['Name'] == tile_id]


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
    if 'S1' in sat_name:
        res = re.search(r"(?<=\_)(\d){8}(?=T)", product_id)
        date_tmp = res.group()
        sensor = 'S1'
    elif 'S2' in sat_name:
        res = re.search(r"(?<=\_)(\d){8}(?=T)", product_id)
        date_tmp = res.group()
        sensor = 'S2'
    elif 'LC08' in sat_name:
        sensor = 'L8'
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
    tmp_dir = os.path.join(out_dir, 'tmp_' + eodag_product.properties['id'])
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    dag.download(eodag_product, outputs_prefix=tmp_dir)
    prod = os.listdir(tmp_dir)[0]
    prod_id = prod.split('_')
    dwn_prod = os.path.join(tmp_dir, prod)
    os.system(f'mv {dwn_prod} {dwn_prod}.SAFE')
    os.system(f'mkdir {dwn_prod}')
    os.system(f'mv {dwn_prod}.SAFE {dwn_prod}')
    os.system(f'mv {dwn_prod} {out_dir}')
    vv_name = f's1a-iw-grd-vv-{prod_id[4].lower()}-{prod_id[5].lower()}-{prod_id[6].lower()}-{prod_id[7].lower()}-{prod_id[8].lower()}-001'
    vh_name = f's1a-iw-grd-vh-{prod_id[4].lower()}-{prod_id[5].lower()}-{prod_id[6].lower()}-{prod_id[7].lower()}-{prod_id[8].lower()}-002'
    base = f"{out_dir}/{prod}/{prod}.SAFE"
    os.rename(f'{base}/{"annotation"}/iw-vh.xml', f'{base}/{"annotation"}/{vh_name}.xml')
    os.rename(f'{base}/{"annotation"}/iw-vv.xml', f'{base}/{"annotation"}/{vv_name}.xml')
    os.rename(f'{base}/{"measurement"}/iw-vh.tiff', f'{base}/{"measurement"}/{vh_name}.tiff')
    os.rename(f'{base}/{"measurement"}/iw-vv.tiff', f'{base}/{"measurement"}/{vv_name}.tiff')
    os.system(f'rm -r {tmp_dir}')

def download_s3file(s3_full_key,out_file, bucket):
    """
    Download file from s3 object storage
    :param s3_full_key: Object full path (bucket name, prefix, and key)
    :param out_file: Full path and name of the output file
    :param bucket: Bucket name
    """
    key = s3_full_key.split(bucket+"/")[1]
    product_id = os.path.split(s3_full_key)[-1]
    band_num = "_".join(product_id.split('_')[7:9])
    out_file+="_"+band_num
    s3 = boto3.resource("s3")
    object = s3.Object(bucket,key)
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
    # TODO update this function to match eotile version
    res = main(tile_id)
    UL = res[0][0].UL
    # Return LL, UR tuple
    return (UL[0],UL[1]-109800,UL[0]+109800,UL[1])

def merge_rasters(rasters,bounds,output_fn):
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
    merge(sources,dst_path=output_fn,method='max',bounds=bounds)
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
            if file.endswith(('.tif','.TIF')) and 'LC08' in file:
                l8_rasters.append(os.path.join(root,file))
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
        meta = prod.split('_')
        date = meta[3]
        path = meta[2][:3]
        prod_path = prod
        band = re.findall('(?<=\_T1_)(.*?)(?=\.)',prod)[0]
        # Should be a better way to fill up this dict
        if date in merge_dict:
            if path in merge_dict[date]:
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band]=[]
                    merge_dict[date][path][band].append(prod_path)
            else:
                merge_dict[date][path] = {}
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band] = []
                    merge_dict[date][path][band].append(prod_path)
        else:
            merge_dict[date]={}
            if path in merge_dict[date]:
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band]=[]
                    merge_dict[date][path][band].append(prod_path)
            else:
                merge_dict[date][path] = {}
                if band in merge_dict[date][path]:
                    merge_dict[date][path][band].append(prod_path)
                else:
                    merge_dict[date][path][band] = []
                    merge_dict[date][path][band].append(prod_path)
    return merge_dict

def copy_tirs_s3(s3_full_key,out_dir,s2_tile):
    """
    Copy L8 Thermal bands from S3 bucket
    :param s3_full_key: Object full path (bucket name, prefix, and key)
    :param out_dir:
    :param s2_tile:
    :return:
    """
    product_id = os.path.split(s3_full_key)[-1]
    platform = product_id.split('_')[0]
    processing_level = product_id.split('_')[1]
    date = product_id.split('_')[3]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = s2_tile
    out_dir = os.path.join(out_dir,'TIR')
    unique_id = f"{product_id.split('_')[2]}{product_id.split('_')[5]}{product_id.split('_')[6]}"
    folder_st = os.path.join(out_dir, tile_id[:2], tile_id[2], tile_id[3:], year,date.split('T')[0])
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    out_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    raster_fn = os.path.join(folder_st, dir_name, out_name)
    tmp = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp):
        os.makedirs(tmp)
    bucket = "usgs-landsat"
    download_s3file(s3_full_key,raster_fn,bucket)
    qa_key = s3_full_key.replace('ST_B10','ST_QA')
    download_s3file(qa_key, raster_fn, bucket)
    # TODO Use logger instead
    print('Done for TIRS copy')

def s1_db(raster_path):
    """
    Convert Sentinel-1 to decibel
    :param raster_path: path to Sentinel-1 tif file
    """
    ds = rasterio.open(raster_path,'r')
    meta = ds.meta.copy()
    band = ds.read(1)
    # mask 0 values
    mask = band!=0
    db_mask = list(mask)
    ds.close()
    decibel = 10 * np.log10(band,where=db_mask)
    dn = 10.0 ** ((decibel + 83) / 20)
    dn[~mask]=0
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
    sar_files=[]
    for root, dirs, files in os.walk(folder):
        for file in files:
            if 's1' in file.lower() and file.endswith(('tif','TIF')):
                sar_files.append(os.path.join(root,file))
    for sar_file in sar_files:
        print(f'Converting {sar_file} to db -> 10*log10(linear) and uint16 -> dn = 10.0 ** ((db + 83) / 20)')
        s1_db(sar_file)
    return len(sar_files)

def get_srtm(tile_id,full_name=False):
    """
    Get srtm hgt files id for an S2 tile
    :param tile_id:
    :return: List of hgt files ids
    """
    res= main(tile_id,srtm=True,overlap=True,no_l8=True,no_s2=True)
    srtm_df = res[2]
    list_ids = list(srtm_df.id)
    if full_name:
        out = [f"{tile}.SRTMGL1.hgt.zip" for tile in list_ids]
        return out
    else:
        return list_ids


def get_product_by_id(product_id, out_dir, provider, config_file=None):
    dag = EODataAccessGateway(config_file)
    dag.set_preferred_provider(provider)
    products,_ = dag.search(id=product_id,provider=provider)
    dag.download(products[0],outputs_prefix=out_dir)
    # delete zip file
    list_out = os.listdir(out_dir)
    for item in list_out:
        if product_id in item and item.endswith('zip'):
            os.remove(os.path.join(out_dir,item))

def get_prods_from_json(json_file, out_dir, provider,sat="S2", config_file=None):
    # Read json plan
    prod_types = {"S2":"S2_PROC","L8":"L8_PROC","S1":"SAR_PROC"}
    sat = prod_types[sat]
    with open(json_file) as f:
        plan = json.load(f)
    for tile in plan:
        out_dir = os.path.join(out_dir,tile)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        if sat == 'S2_PROC':
            prods = plan[tile][sat]['INPUTS']
            for prod in prods:
                get_product_by_id(prod['id'],out_dir,provider,config_file=config_file)
        else:
            prods = plan[tile][sat]['INPUTS']
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
                band_path= os.path.join(root,file)
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
    with rasterio.Env(GDAL_CACHEMAX=2048):
        with rasterio.open(raster_path,'r') as src:
            raster_array = src.read()
            meta = src.meta.copy()
    meta["driver"] = "GTiff"
    meta["nodata"] = 0
    bands_10m = ['B02','B03','B04','B08']
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

def binary_scl(scl_file,raster_fn):
    with rasterio.open(scl_file,"r") as src:
        ds = src.read(1)
    # For SCL flag all cloud pixels
    # TODO find a better way to do this
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

def l2a_to_ard(l2a_folder,work_dir):
    """
    Convert an L2A product into EWoC ARD format
    :param l2a_folder: L2A SAFE folder
    """
    bands = {'B02':10,'B03':10,'B04':10,'B08':10,'B05':20,'B06':20, 'B07': 20, 'B11':20,'B12':20, 'SCL':20}
    # Prepare ewoc folder name
    prod_name = get_s2_prodname(l2a_folder)
    product_id = prod_name
    platform = product_id.split('_')[0]
    processing_level = product_id.split('_')[1]
    date = product_id.split('_')[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split('_')[5][1:]
    atcor_algo = "L2A"
    unique_id = "".join(product_id.split('_')[3:6])
    folder_st = os.path.join(work_dir,'OPTICAL', tile_id[:2], tile_id[2], tile_id[3:], year, date.split('T')[0])
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    tmp_dir = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Convert bands and SCL
    for band in bands:
        res = bands[band]
        band_path = find_l2a_band(l2a_folder,band, bands[band])
        band_name = os.path.split(band_path)[-1]
        band_name = band_name.replace('.jp2','.tif').replace(f'_{str(res)}m','')
        print('Processing band '+band_name)
        out_name = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_{band}.tif"
        raster_fn = os.path.join(folder_st, dir_name, out_name)
        ard_folder = os.path.join(folder_st, dir_name)
        if band == 'SCL':
            binary_scl(band_path,raster_fn)
            print('Done --> ' + raster_fn)
            try:
                os.remove(raster_fn+'.aux.xml')
            except:
                print('Clean')

        else:
            raster_to_ard(band_path,band,raster_fn)
            print('Done --> ' + raster_fn)
    return ard_folder






