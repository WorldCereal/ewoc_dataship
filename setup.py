# Copyright (c) 2021 CS Group.
# All rights reserved.
from setuptools import find_packages, setup

setup(
    name="dataship",
    version="0.1.4",
    description="Data access using eodag",
    author="Fahd Benatia",
    author_email="fahd.benatia@csgroup.eu",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    license="Copyright (c) 2021 CS Group",
    python_requires=">=3",
    install_requires=[
       "eodag==2.3.2",
       "geopandas==0.9.0",
        "rasterio==1.2.2",
        "eotile==0.2rc3"
    ],
    entry_points={"console_scripts": ["dataship=dataship.dag.main:cli"]},
)