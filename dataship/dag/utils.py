#import logging
import re
import os
import boto3
from datetime import datetime, timedelta

import eotile.eotile_module
import geopandas as gpd
import rasterio
from rasterio.merge import merge
import numpy as np
import pkg_resources
from eotile.eotile_module import main
# Replace this with eotile later
index_path = pkg_resources.resource_filename(__name__, os.path.join("../index", "s2_idx.geojson"))

#logging.basicConfig(
#    format="dataship - %(levelname)s - %(message)s", level=logging.INFO
#)

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
    res=eotile.eotile_module.main(tile_id,srtm=True)
    srtm_df = res[2]
    list_ids = list(srtm_df.id)
    if full_name:
        out = [f"{tile}.SRTMGL1.hgt.zip" for tile in list_ids]
        return out
    else:
        return list_ids
