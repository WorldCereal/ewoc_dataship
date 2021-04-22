import click

from eodag import EODataAccessGateway
from dataship.dag.utils import *
from dataship.dag.ship import merge_l8


@click.group()
def cli():
    click.secho("Unified data access using EOdag", fg="green",blink=True,bold=True)

@cli.command('download',help="Simple data download using EOdag")
@click.option('-t', '--tile_id', help="S2 tile id")
@click.option('-s', '--start_date', help="start date for your products search,format YYYY-mm-dd")
@click.option('-e', '--end_date', help="end date for your products search,format YYYY-mm-dd")
@click.option('-pt', '--product_type',
              help="Product type,for aws use generic types ex: sentinel1_l1c_grd/sentinel2_l1c/landsat8_l1tp")
@click.option('-pv', '--provider', help="EOdag provider ex astraea_eod/peps/theia")
@click.option('-o', '--out_dir', help="Output directory")
@click.option('-cfg', '--config_file', help="EOdag config file")
def eodag_prods(tile_id, start_date, end_date, product_type, out_dir, config_file, provider="peps"):
    df = get_geom_from_id(tile_id)
    bbox = df.total_bounds
    extent = {'lonmin': bbox[0], 'latmin': bbox[1], 'lonmax': bbox[2], 'latmax': bbox[3]}
    dag = EODataAccessGateway(config_file)
    dag.set_preferred_provider(provider)
    products, est = dag.search(productType=product_type, start=start_date, end=end_date, geom=extent,
                               items_per_page=200, cloudCover=70)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # Sometimes the search results show neighbouring tiles
    # Filter and keep only desired tile
    dag.download_all(products, outputs_prefix=out_dir)


@cli.command('eodag_id',help="Get products by ID from a previous EOdag search")
@click.option('-t', '--s2_tile_id', help="S2 tile id")
@click.option('-pid', '--product_id', help="Product id from the plan json")
@click.option('-pv', '--provider', help="EOdag provider ex astraea_eod/peps/theia", default='astraea_eod')
@click.option('-o', '--out_dir', help="Output directory")
@click.option('-cfg', '--config_file', help="EOdag config file")
def eodag_by_ids(s2_tile_id, product_id, out_dir, provider, config_file=None):
    # Extract dates and sensor from product id
    start_date, end_date, sensor = get_dates_from_prod_id(product_id)
    prods_types = {"S2": {"peps": "S2_MSI_L1C", "astraea_eod": "sentinel2_l1c"},
                   "S1": {"peps": "S1_SAR_GRD", "astraea_eod": "sentinel1_l1c_grd"},
                   "L8": {"astraea_eod": "landsat8_l1tp"}}
    product_type = prods_types[sensor][provider.lower()]
    # Get s2 tile footprint from external file (to be replaced by eotile)
    df = get_geom_from_id(s2_tile_id)
    bbox = df.total_bounds
    extent = {'lonmin': bbox[0], 'latmin': bbox[1], 'lonmax': bbox[2], 'latmax': bbox[3]}

    if config_file is None:
        dag = EODataAccessGateway()
        astraea_eod = '''
                    astraea_eod:
                        priority: 2 # Lower value means lower priority (Default: 0)
                        search:   # Search parameters configuration
                        auth:
                            credentials:
                                aws_access_key_id:
                                aws_secret_access_key:
                                aws_profile: 
                        download:
                            outputs_prefix:
            '''
        # Do not put the aws credentials here, they are parsed from env vars
        dag.update_providers_config(astraea_eod)
        dag.set_preferred_provider("astraea_eod")
    else:
        dag = EODataAccessGateway(config_file)
        # TODO Use logger instead
        print(provider)
        dag.set_preferred_provider(provider)
    products, est = dag.search(productType=product_type, start=start_date, end=end_date, geom=extent,
                               items_per_page=200, cloudCover=70)
    final_product = [prod for prod in products if prod.properties["id"] == product_id][0]

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    if provider == "astraea_eod":
        # Get manifest.safe s3 key from vv s3 key
        manifest_key = os.path.split(final_product.assets['vv']['href'])[0].replace('measurement', 'manifest.safe')
        # Add manifest to EOProduct assets
        final_product.assets['manifest'] = {}
        final_product.assets['manifest']['href'] = manifest_key
        # Download from aws and reformat the data
        donwload_s1tiling_style(dag, final_product, out_dir)
    else:
        # Download data for other providers
        dag.download(final_product, outputs_prefix=out_dir)

@cli.command('tirs_cp',help="Get L8 Thermal band from aws")
@click.option('-k', '--s3_full_key')
@click.option('-o', '--out_dir', help="Output directory")
def copy_tirs_s3(s3_full_key,out_dir):
    bucket = "usgs-landsat"
    download_s3file(s3_full_key,out_dir,bucket)
    qa_key = s3_full_key.replace('ST_B10','ST_QA')
    download_s3file(qa_key, out_dir, bucket)
    # TODO Use logger instead
    print('Done for TIRS copy')
@cli.command('package', help = "Harmonize Landsat-8 products")
@click.option('-f','--data_folder',help="Folder with L8 products")
@click.option('-t','--s2_tile_id', help="S2 tile id")
def pack_l8(data_folder,s2_tile_id):
    merge_l8(data_folder,s2_tile_id)

if __name__ == "__main__":
    cli()

