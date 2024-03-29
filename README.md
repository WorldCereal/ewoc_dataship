# EWoc Data Access Gateway

Data access (download, search, upload) for EWoC processors based mainly on s3 buckets. Optionaly [EOdag](https://eodag.readthedocs.io/en/stable/) for search and download can be used.

## Data downloading
Currently you can acess EO data from the following sources:

| Data Provider                     | Sentinel-2 L1C   | Sentinel-2 L2A                  | Landsat 8 C2 L2  | Sentinel-1 GRD   |
| --------------------------------- | ---------------- | ------------------------------- | ---------------- | ---------------- |
| AWS buckets                       | Yes <sup>1</sup> | Yes <sup>1,</sup>  <sup>2</sup> | Yes <sup>1</sup> | Yes <sup>1</sup> |
| Creodias DIAS bucket <sup>4</sup> | Yes              | Partially <sup>3</sup>          | No               | Yes              |
| Creodias Finder <sup>5</sup>      | Yes              | Partially <sup>3</sup>          | No               | Yes              |

Currently you can acess DEM data from the following sources:

| Data Provider                                           | SRTM 1s   | SRTM 3s | COP DEM 1s | COP DEM 3s |
| ------------------------------------------------------- | --------- | ------- | ---------- | ---------- |
| EWoC private bucket                                     | Partially | Yes     | No         | No         |
| AWS buckets                                             | No        | No      | Yes        | Yes        |
| Creodias DIAS bucket <sup>4</sup>                       | Yes       | No      | Yes        | Yes        |
| [ESA website](http://step.esa.int/auxdata/dem/SRTMGL1/) | Yes       | No      | No         | No         |

<sup>1</sup> Access payed by requester.

<sup>2</sup> L2A cogs are available.

<sup>3</sup> Rolling archive of 1 year.

<sup>4</sup> Creodias bucket can be acessible only from creodias network

<sup>5</sup> Through EODAG

## EWoC Data search

You can list the data from the EWoC ARD and Aux data private buckets by scanning the bucket from a specific prefix.

## EWoC Data upload

You can upload data to the EWoC private buckets: ARD or PRD.

## Installation

1. Retrieve the package
2. Create a venv
3. `pip install /path/to/ewoc_dag.archive`

## Usage

### Credentials

To manage access to the different data provider or buckets you need to set some credentials:

| Data Provider or buckets        | Credentials                                                                     |
| ------------------------------- | ------------------------------------------------------------------------------- |
| EWoC private bucket on CreoDias | Set env variables: `EWOC_S3_ACCESS_KEY_ID` and `EWOC_S3_SECRET_ACCESS_KEY`      |
| Creodias DIAS bucket            | N/A but access possible only from creodias cluster                              |
| AWS buckets (public or private) | Set env variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` <sup>6</sup> |
| ESA website                     | N/A but access not useable for production                                       |
| EODAG                           | cf. [EODAG documentation](https://eodag.readthedocs.io/en/stable/)              |

<sup>6</sup> Other methods are described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials)

### Special environment variables

It is possible to control some other points through environment variables:

- `EWOC_CLOUD_PROVIDER` is the env variable which indicates where the processing system is deployed: `aws` or `creodias` are currently supported. This env variable is used to define the default provider of the processing system for Sentinel-2, Copernicus DEM and Sentinel-1 data. The idea is to use the buckets of the cloud provider.
- `EWOC_COPDEM_SOURCE` is the env variable which indicates which provider is used as default to retrieve Copernicus DEM data: `aws` or `creodias` or `eodag`
- `EWOC_S1_PROVIDER` is the env variable which indicates which provider is used as default to retrieve Sentinel-1 data: `aws` or `creodias` or `eodag`
- `EWOC_S2_PROVIDER` is the env variable which indicates which provider is used as default to retrieve Sentinel-2 data: `aws` or `creodias` or `eodag`
- `EWOC_EODAG_PROVIDER` is the env variable which indicates which EODAG provider you want to use to retrieve data with EODAG.
- `EWOC_DEV_MODE` is the env variable which indicates to use the EWoC dev buckets (ARD or PRD) to upload data

### CLI

Two cli are provided:

- the first one `ewoc_get_eo_data` provided the EO data needed by the EWoC processors based by their product ID.
- the second one `ewoc_get_dem_data` provided the dem data needed by the EWoC processors based on the S2 tile ID.

### Python

If you want to acess to EO data from other python code you can use directly the get functions in the python module. For example:

```python
from ewoc_dag.landsat8 import get_l8c2l2_product

get_l8c2l2_product('l8_c2l2_prd_id')
```

:grey_exclamation: Don't forget to set crendentials associated to the provider used.

If you want to upload data to the EWoC buckets:

```python
from pathlib import Path
from ewoc_dag.bucker.ewoc import EWOCARDBucket

ewoc_ard_bucket = EWOCARDBucket(ewoc_dev_mode=True)
ewoc_ard_bucket._upload_file(Path('/tmp/upload.file'),'test.file')
ewoc_ard_bucket._upload_prd(Path('/tmp/upload_test_dir'),'test_up_dir')
```

:grey_exclamation: Don't forget to set EWoC bucket credentials

If you want to list the content of the some part of the ARD bucket and save as a satio collection:

```python
from ewoc_dag.bucket.ewoc import EWOCARDBucket

ewoc_ard_bucket = EWOCARDBucket(ewoc_dev_mode=True)
ewoc_ard_bucket.sar_to_satio_csv("31TCJ", "0000_0_09112021223005")
ewoc_ard_bucket.optical_to_satio_csv("31TCJ", "0000_0_09112021223005")
ewoc_ard_bucket.tir_to_satio_csv("31TCJ", "0000_0_09112021223005")
```

or to list the AgERA5 data

```python
from ewoc_dag.bucket.ewoc import EWOCAuxDataBucket

ewoc_auxdata_bucket = EWOCAuxDataBucket()
ewoc_auxdata_bucket.agera5_to_satio_csv()
```

:grey_exclamation: Don't forget to set EWoC bucket credentials

## How to release

TODO
