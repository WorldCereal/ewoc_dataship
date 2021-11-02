# EWoc Data Access Gateway

Data access for EWoC processors based mainly on s3 bucket data retrieval (Optionaly [EOdag](https://eodag.readthedocs.io/en/stable/) can be used).

Currently you can acess EO data from the following sources:

| Data Provider                     | Sentinel-2 L1C   | Sentinel-2 L2A                  | Landsat 8 C2 L2  | Sentinel-1 GRD   |
| --------------------------------- | ---------------- | ------------------------------- | ---------------- | ---------------- |
| AWS buckets                       | Yes <sup>1</sup> | Yes <sup>1,</sup>  <sup>2</sup> | Yes <sup>1</sup> | Yes <sup>1</sup> |
| Creodias DIAS bucket <sup>4</sup> | Yes              | Partially <sup>3</sup>          | No               | Yes              |
| Creodias Finder <sup>5</sup>      | Yes              | Partially <sup>3</sup>          | No               | Yes              |

Currently you can acess DEM data from the following sources:

| Data Provider                     | SRTM 1s   | SRTM 3s  | COP DEM 1s | COP DEM 3s |
| --------------------------------- | --------- | -------- | ---------- | ---------- |
| EWoC private bucket               | Partially | Yes      | No         | No         |
| AWS buckets                       | No        | No       | Yes        | Yes        |
| Creodias DIAS bucket <sup>4</sup> | Yes       | No       | Yes        | Yes        |
| ESA website                       | Yes       | No       | No         | No         |

<sup>1</sup> Access payed by requester.

<sup>2</sup> L2A cogs are available.

<sup>3</sup> Rolling archive of 1 year.

<sup>4</sup> Creodias bucket can be acessible only from creodias network

<sup>5</sup> Through EODAG

## Installation

1. Retrieve the package
2. Create a venv
3. `pip install /path/to/ewoc_dag.archive`

## Usage

### CLI

Two cli are provided:

- the first one `ewoc_get_eo_data` provided the EO data needed by the EWoC processors based by their product ID.
- the second one `ewoc_get_dem_data` provided the dem data needed by the EWoC processors based on the S2 tile ID.

### Python

If you want to acess to EO data from other python code you can use directly the get functions in the python module. For example:

```python
from pathlib import Path
from ewoc_dag.landsat8 import get_l8_product

get_l8_product('l8_c2l2_prd_id', Path('/tmp'))

```

### Data access credentials

To access to the different sources you need to set some credentials:

| Data Provider        | Credentials |
| -------------------- | ----------- |
| EWoC                 | Set env variables: EWOC_S3_ACCESS_KEY_ID and EWOC_S3_SECRET_ACCESS_KEY |
| Creodias bucket      | N/A but access possible only from creodias cluster |
| AWS                  | Set credentials as described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) |
| ESA website          | N/A but access not useable for production |
| EODAG                | cf. [EODAG documentation](https://eodag.readthedocs.io/en/stable/)|

## How to release

TODO
