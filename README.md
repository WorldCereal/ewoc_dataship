# Dataship 🚤

Data access for EWoC processors. Powered by [EOdag](https://eodag.readthedocs.io/en/stable/)

## Installation
1. Clone this repository 
2. (Optional) create a venv
3. `pip install .`

Docker usage from this repository:

1 .`docker build -t dataship .`

2. `docker run -ti --rm dataship --help` (the entrypoint is the dataship CLI)

## Usage 
```bash
Usage: dataship [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  download  Simple data download using EOdag
  eodag_id  Get products by ID from a previous EOdag search
  package   Harmonize Landsat-8 products
  s1db      Convert S1 to db
  tirs_cp   Get L8 Thermal band from aws
```
Note that dataship is tailored for the needs of the EWoC pre-processing modules. 

For a more complete data access consider using [EOdag](https://eodag.readthedocs.io/en/stable/).

## Commands 

### Download 
Download S2/L8 products for a given S2 Tile id (ex 31TCJ)
```
Usage: dataship download [OPTIONS]

  Simple data download using EOdag

Options:
  -t, --tile_id TEXT        S2 tile id
  -s, --start_date TEXT     start date for your products search,format YYYY-
                            mm-dd

  -e, --end_date TEXT       end date for your products search,format YYYY-mm-
                            dd

  -pt, --product_type TEXT  Product type,for aws use generic types ex:
                            sentinel1_l1c_grd/sentinel2_l1c/landsat8_l1tp

  -pv, --provider TEXT      EOdag provider ex astraea_eod/peps/theia
  -o, --out_dir TEXT        Output directory
  -cfg, --config_file TEXT  EOdag config file
  --help                    Show this message and exit.
```
### Download by ID
Download a S2/L8 product with an ID
```bash
Usage: dataship eodag_id [OPTIONS]

  Get products by ID from a previous EOdag search

Options:
  -t, --s2_tile_id TEXT     S2 tile id
  -pid, --product_id TEXT   Product id from the plan json
  -pv, --provider TEXT      EOdag provider ex astraea_eod/peps/theia
  -o, --out_dir TEXT        Output directory
  -cfg, --config_file TEXT  EOdag config file
  --help                    Show this message and exit.
```
### Copy Landsat 8 thermal band
This command is useful to get L8 thermal bands from the usgs aws bucket
```bash
Usage: dataship tirs_cp [OPTIONS]

  Get L8 Thermal band from aws

Options:
  -k, --s3_full_key TEXT
  -o, --out_dir TEXT      Output directory
  --help                  Show this message and exit.
```
### Convert S1 to db
```bash
Usage: dataship s1db [OPTIONS]

  Convert S1 to db

Options:
  -f, --folder TEXT  SAR folder
  --help             Show this message and exit.
```
### Get SRTM tiles ids
```bash
Usage: dataship srtm_id [OPTIONS]

  Get SRTM tiles ids for an S2 tile id

Options:
  -t, --s2_tile_id TEXT  S2 tile id
  --help                 Show this message and exit.
```
Result of the CLI:
```bash
N38W001;N38W002;N37W001;N37W002
```
Result from python API
```python
from dataship.dag.utils import get_srtm

get_srtm('30SXH')

# Result: ['N38W001', 'N38W002', 'N37W001', 'N37W002']
```