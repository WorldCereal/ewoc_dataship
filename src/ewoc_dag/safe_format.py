# -*- coding: utf-8 -*-
""" SAFE format utilities module
"""
import logging
import json
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def aws_s1_to_safe(out_dirpath: Path, prd_id: str) -> Path:

    if prd_id.endswith(".SAFE"):
        safe_prd_id = prd_id
    else:
        safe_prd_id = prd_id + ".SAFE"

    out_safe_dirpath = out_dirpath.parent / safe_prd_id
    out_safe_dirpath.mkdir(exist_ok=True)

    with open(
        out_dirpath / "productInfo.json", mode="r", encoding="utf8"
    ) as prd_info_file:
        prd_info = json.load(prd_info_file)

    for filename_key, filename_value in prd_info["filenameMap"].items():
        source_filepath = out_dirpath / filename_value
        target_filepath = out_safe_dirpath / filename_key
        logger.info("Rename from %s to %s", source_filepath, target_filepath)
        (target_filepath.parent).mkdir(exist_ok=True, parents=True)
        (source_filepath).rename(target_filepath)

    shutil.rmtree(out_dirpath)

    return out_safe_dirpath


def aws_s2_l1c_to_safe(out_dirpath: Path, prd_id: str) -> Path:
    """
    Create SAFE folder from an L1C Sentinel-2 product
    from an AWS download
    :param out_dirpath:
    :param safe_dest_folder:
    :return: SAFE folder path
    """
    # Create root folder
    if prd_id.endswith(".SAFE"):
        safe_prd_id = prd_id
    else:
        safe_prd_id = prd_id + ".SAFE"

    out_safe_dirpath = out_dirpath.parent / safe_prd_id
    out_safe_dirpath.mkdir(exist_ok=True)

    # Find the manifest.safe file in the product folder
    manifest_safe = out_dirpath / "tile" / "manifest.safe"
    # Parse the manifest

    tree = ET.parse(manifest_safe)
    root = tree.getroot()

    safe_struct = {"DATASTRIP": [], "GRANULE": [], "root": [], "HTML": []}

    for file_loc in root.findall(".//fileLocation"):
        loc = Path(file_loc.get("href"))
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
    aws_ds_mtd = sorted(out_dirpath.glob("tile/*/*/metadata.xml"))[0]
    shutil.copy(aws_ds_mtd, out_safe_dirpath / safe_ds_mtd)

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
    print(safe_struct["GRANULE"])

    # Create GRANULE folders
    for granule_loc in safe_struct["GRANULE"]:
        (out_safe_dirpath / granule_loc.parent).mkdir(parents=True, exist_ok=True)

    # Copy GRANULE/QI_DATA gml files
    qi_data_gr_folder = [
        el.parent for el in safe_struct["GRANULE"] if el.parts[-2] == "QI_DATA"
    ][0]

    for gr_elt in safe_struct["GRANULE"]:
        if gr_elt.parts[-2] == "QI_DATA":
            safe_gr_qi_data_dir = gr_elt.parent
            break
    print(safe_gr_qi_data_dir)

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
    ecmwft = sorted(out_dirpath.glob("product/*/ECMWFT"))[0]

    for gr_elt in safe_struct["GRANULE"]:
        if gr_elt.parts[-2] == "AUX_DATA":
            safe_gr_aux_data_dir = gr_elt.parent
            break
    shutil.copy(ecmwft, out_safe_dirpath / safe_gr_aux_data_dir / "AUX_ECMWFT")

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
        (out_safe_dirpath / qi_data_gr_folder).parent / "MTD_TL.xml",
    )

    return out_safe_dirpath
