import re
import os
from datetime import datetime, timedelta

import geopandas as gpd
import pkg_resources

# Replace this with eotile later
index_path = pkg_resources.resource_filename(__name__, os.path.join("../index", "s2_idx.geojson"))
s2_grid = gpd.read_file(index_path)


def get_geom_from_id(tile_id):
    """
    Get Sentinel-2 tile footprint from it's ID
    :param tile_id: S2 tile id
    :return: GeoDataFrame with the footprint geometry
    """
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