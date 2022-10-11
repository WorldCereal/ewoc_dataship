# -*- coding: utf-8 -*-
""" SAFE format utilities module
"""
import json
import logging
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo

logger = logging.getLogger(__name__)


class S1SafeConversionError(Exception):
    """Exception raised for errors in the S1 SAFE conversion format on AWS."""

    def __init__(self, error):
        self._error = error
        self._message = "Error during S1 SAFE conversion:"
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message} {self._error} !"


def aws_to_safe(
    out_dirpath: Path,
    prd_id: str,
    out_safe_dirroot: Optional[Path] = None,
) -> Path:
    """Translate from format used by some AWS buckets to SAFE format


    Args:
        out_dirpath (Path): Path to the AWS product
        prd_id (str): Product ID
        out_safe_dirroot (Path, optional): Path where to write the SAFE product.
            Defaults to None.
            If None, the product will be written side by side of the original one.
    Returns:
        Path: Path to the SAFE product directory
    """

    if prd_id.endswith(".SAFE"):
        safe_prd_id = prd_id
    else:
        safe_prd_id = prd_id + ".SAFE"

    if out_safe_dirroot is None:
        out_safe_dirpath = out_dirpath.parent / safe_prd_id
    else:
        out_safe_dirpath = out_safe_dirroot / safe_prd_id

    out_safe_dirpath.mkdir(exist_ok=True)

    if S1PrdIdInfo.is_valid(prd_id):
        if (out_dirpath / "productInfo.json").exists():
            prd_safe_dirpath = aws_s1_to_safe(out_dirpath, out_safe_dirpath)
        else:
            out_safe_dirpath.rmdir()
            raise S1SafeConversionError(
                f'No conversion file for {prd_id.split(".")[0]}'
            )
    elif S2PrdIdInfo.is_valid(prd_id) and S2PrdIdInfo.is_l1c(prd_id):
        prd_safe_dirpath = aws_s2_l1c_to_safe(out_dirpath, out_safe_dirpath)
    else:
        raise ValueError("Product ID not supported!")

    shutil.rmtree(out_dirpath)

    return prd_safe_dirpath


def aws_s1_to_safe(
    out_dirpath: Path,
    out_safe_dirpath: Path,
) -> Path:
    """Translate from AWS S1 product format to SAFE format

    Args:
        out_dirpath (Path): Path to the AWS S1 product
        out_safe_dirroot (Path): Path where to write the S1 SAFE product.
    Returns:
        Path: Path to the S1 SAFE product directory
    """
    with open(
        out_dirpath / "productInfo.json", mode="r", encoding="utf8"
    ) as prd_info_file:
        prd_info = json.load(prd_info_file)

    for filename_key, filename_value in prd_info["filenameMap"].items():
        source_filepath = out_dirpath / filename_value
        target_filepath = out_safe_dirpath / filename_key
        logger.debug("Rename from %s to %s", source_filepath, target_filepath)
        (target_filepath.parent).mkdir(exist_ok=True, parents=True)
        (source_filepath).rename(target_filepath)

    return out_safe_dirpath


def aws_s2_l1c_to_safe(
    out_dirpath: Path,
    out_safe_dirpath: Path,
) -> Path:
    """Translate from AWS S2 L1C format to SAFE format

    Args:
        out_dirpath (Path): Path to the AWS S2 L1C product
        prd_id (str): Sentinel-2 product ID
        out_safe_dirpath (Path): Path where to write the S2 SAFE product.
    Returns:
        Path: Path to the S2 SAFE product directory
    """

    # Find the manifest.safe file in the product folder
    manifest_safe = out_dirpath / "tile" / "manifest.safe"

    # Parse the manifest

    safe_struct: Dict["str", List[Path]] = {
        "DATASTRIP": [],
        "GRANULE": [],
        "root": [],
        "HTML": [],
    }

    for file_loc in ET.parse(manifest_safe).getroot().findall(".//fileLocation"):

        loc_elt = file_loc.get("href")
        if loc_elt is None:
            raise ValueError("No href key in manifest.safe")

        loc = Path(loc_elt)
        loc_parts = loc.parts
        if len(loc_parts) == 1:
            safe_struct["root"].append(loc)
        elif len(loc_parts) > 1:
            safe_struct[loc_parts[0]].append(loc)

    # Copy manifest.safe
    shutil.copy(manifest_safe, out_safe_dirpath / manifest_safe.name)

    # Create rep_info folder (Empty folder missing info on aws)
    (out_safe_dirpath / "rep_info").mkdir(parents=True, exist_ok=True)

    # Create HTML folder (Empty folder missing info on aws)
    (out_safe_dirpath / "HTML").mkdir(parents=True, exist_ok=True)

    # Create AUX_DATA folder (Empty folders)
    (out_safe_dirpath / "AUX_DATA").mkdir(parents=True, exist_ok=True)

    # Copy inspire xml
    shutil.copy(out_dirpath / "tile" / "inspire.xml", out_safe_dirpath / "INSPIRE.xml")

    # Copy tile/metadata.xml to MTD_MSIL1C.xml
    shutil.copy(
        out_dirpath / "tile" / "metadata.xml", out_safe_dirpath / "MTD_MSIL1C.xml"
    )

    ##########################
    # DATASTRIP
    # Create DATASTRIP folders
    for datastrip_loc in safe_struct["DATASTRIP"]:
        (out_safe_dirpath / datastrip_loc.parent).mkdir(parents=True, exist_ok=True)

    # Copy MTD_DS.xml
    for ds_elt in safe_struct["DATASTRIP"]:
        if ds_elt.name == "MTD_DS.xml":
            safe_ds_mtd = ds_elt
            break
    shutil.copy(
        sorted(out_dirpath.glob("tile/*/*/metadata.xml"))[0],
        out_safe_dirpath / safe_ds_mtd,
    )

    # Copy report files
    for ds_elt in safe_struct["DATASTRIP"]:
        if ds_elt.parts[-2] == "QI_DATA":
            safe_ds_qi_data_dir = ds_elt.parent
            break
    for aws_qi_report in sorted(out_dirpath.glob("tile/*/*/*/*report.xml")):
        safe_report_name = aws_qi_report.name.replace("_report", "")
        shutil.copy(
            aws_qi_report,
            out_safe_dirpath / safe_ds_qi_data_dir / safe_report_name,
        )

    ##########################
    # GRANULE

    # Create GRANULE folders
    for granule_loc in safe_struct["GRANULE"]:
        (out_safe_dirpath / granule_loc.parent).mkdir(parents=True, exist_ok=True)

    # Copy GRANULE/QI_DATA gml files
    for gr_elt in safe_struct["GRANULE"]:
        if gr_elt.parts[-2] == "QI_DATA":
            safe_gr_qi_data_dir = gr_elt.parent
            break

    aws_gr_gml = sorted(out_dirpath.glob("product/qi/*.gml"))
    for gr_gml in aws_gr_gml:
        shutil.copy(
            gr_gml,
            out_safe_dirpath / safe_gr_qi_data_dir / gr_gml.name,
        )

    # Copy GRANULE/QI_DATA xml files
    qi_xml_qa = sorted(out_dirpath.glob("product/qi/*.xml"))
    for qi_xml in qi_xml_qa:
        shutil.copy(
            qi_xml,
            out_safe_dirpath / safe_gr_qi_data_dir / qi_xml.name,
        )

    # Copy GRANULE/AUX_DATA files
    for gr_elt in safe_struct["GRANULE"]:
        if gr_elt.parts[-2] == "AUX_DATA":
            safe_gr_aux_data_dir = gr_elt.parent
            break
    if "safe_gr_aux_data_dir" not in locals():
        safe_gr_aux_data_dir = safe_gr_qi_data_dir.parents[0] / "AUX_DATA"
    try:
        shutil.copy(
            sorted(out_dirpath.glob("product/*/ECMWFT"))[0],
            out_safe_dirpath / safe_gr_aux_data_dir / "AUX_ECMWFT",
        )
    except:
        logger.warning("No ECMWFT data to copy")
        (out_safe_dirpath / safe_gr_aux_data_dir / "AUX_DATA").mkdir(
            parents=True, exist_ok=True
        )

    # Copy GRANULE/IMG_DATA files
    img_jp2 = [el for el in safe_struct["GRANULE"] if el.parts[-2] == "IMG_DATA"]
    img_data_folder = [
        el.parent for el in safe_struct["GRANULE"] if el.parts[-2] == "IMG_DATA"
    ][0]
    for img in img_jp2:
        band = img.name.split("_")[-1]
        band_aws = list(out_dirpath.glob(f"product/{band}"))[0]
        shutil.copy(
            band_aws,
            out_safe_dirpath / img_data_folder / img.name,
        )

    # Copy product/metadata.xml to GRANULE/*/MTD_TL.xml
    shutil.copy(
        out_dirpath / "product" / "metadata.xml",
        (out_safe_dirpath / safe_gr_qi_data_dir).parent / "MTD_TL.xml",
    )

    return out_safe_dirpath
