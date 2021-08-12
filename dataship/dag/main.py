import click

from dataship.dag.ship import merge_l8
from dataship.dag.utils import *


@click.group()
def cli():
    click.secho("Unified data access using EOdag", fg="green", bold=True)


@cli.command("download", help="Simple data download using EOdag")
@click.option("-t", "--tile_id", help="S2 tile id")
@click.option(
    "-s", "--start_date", help="start date for your products search,format YYYY-mm-dd"
)
@click.option(
    "-e", "--end_date", help="end date for your products search,format YYYY-mm-dd"
)
@click.option(
    "-pt",
    "--product_type",
    help="Product type,for aws use generic types ex: sentinel1_l1c_grd/sentinel2_l1c/landsat8_l1tp",
)
@click.option("-pv", "--provider", help="EOdag provider ex astraea_eod/peps/theia")
@click.option("-o", "--out_dir", help="Output directory")
@click.option("-cfg", "--config_file", help="EOdag config file")
def eodag_prods(
    tile_id, start_date, end_date, product_type, out_dir, config_file, provider="peps"
):
    df = get_geom_from_id(tile_id)
    bbox = df.total_bounds
    extent = {
        "lonmin": bbox[0],
        "latmin": bbox[1],
        "lonmax": bbox[2],
        "latmax": bbox[3],
    }
    dag = EODataAccessGateway(config_file)
    dag.set_preferred_provider(provider)
    products, est = dag.search(
        productType=product_type,
        start=start_date,
        end=end_date,
        geom=extent,
        items_per_page=200,
        cloudCover=70,
    )
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # Sometimes the search results show neighbouring tiles
    # Filter and keep only desired tile
    dag.download_all(products, outputs_prefix=out_dir)


@cli.command("eodag_id", help="Get products by ID from a previous EOdag search")
@click.option("-pid", "--product_id", help="Product id from the plan json")
@click.option(
    "-pv",
    "--provider",
    help="EOdag provider ex astraea_eod/peps/theia/creodias",
    default="astraea_eod",
)
@click.option("-o", "--out_dir", help="Output directory")
@click.option("-cfg", "--config_file", help="EOdag config file")
@click.option(
    "-sat",
    "--sat",
    help="Specify which Sat products to download when using a json file as a product_id (S2/S1/L8)",
)
def eodag_by_ids(product_id, out_dir, provider, config_file=None, sat="S2"):
    if product_id.endswith("json"):
        get_prods_from_json(product_id, out_dir, provider, sat, config_file=config_file)
    else:
        get_product_by_id(product_id, out_dir, provider, config_file=config_file)


@cli.command("tirs_cp", help="Get L8 Thermal band from aws")
@click.option("-k", "--s3_full_key")
@click.option("-t", "--s2_tile_id", help="S2 tile id")
@click.option("-o", "--out_dir", help="Output directory")
def tirs_cp(s3_full_key, out_dir, s2_tile_id):
    copy_tirs_s3(s3_full_key, out_dir, s2_tile=s2_tile_id)


@cli.command("package", help="Harmonize Landsat-8 products")
@click.option("-f", "--data_folder", help="Folder with L8 products")
@click.option("-t", "--s2_tile_id", help="S2 tile id")
def pack_l8(data_folder, s2_tile_id):
    merge_l8(data_folder, s2_tile_id)


@cli.command(
    "s1db",
    help="Convert S1 to db -> 10*log10(linear) then uint16 -> dn = 10.0 ** ((db + 83) / 20)",
)
@click.option("-f", "--folder", help="SAR folder")
def s1db(folder):
    s1db_folder(folder)


@cli.command("srtm_id", help="Get SRTM tiles ids for an S2 tile id")
@click.option("-t", "--s2_tile_id", help="S2 tile id")
@click.option("--full/--no-full", default=False)
def srtm_id(s2_tile_id, full):
    print(";".join(get_srtm(s2_tile_id, full_name=full)))


@cli.command("get_srtm", help="Get SRTM tiles for an S2 tile id")
@click.option("--s2_tile_id", help="S2 tile id")
@click.option("--out_dir")
@click.option("--source", default="esa")
def srtm_id(s2_tile_id, out_dir, source):
    print(";".join(get_srtm1s_ids(s2_tile_id)))
    get_srtm1s(s2_tile_id, Path(out_dir), source=source)


@cli.command("l2a_ard", help="Convert L2A SAFE to ewoc ard format")
@click.option("-f", "--l2a_folder", help="L2A SAFE folder")
@click.option("-o", "--out_dir", help="Output directory")
def l2a_ard(l2a_folder, out_dir):
    l2a_to_ard(l2a_folder, out_dir)


if __name__ == "__main__":
    cli()
