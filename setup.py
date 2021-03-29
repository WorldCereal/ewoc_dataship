# Copyright (c) 2021 CS Group.
# All rights reserved.
from setuptools import find_packages, setup

setup(
    name="dataship",
    version="0.0.1",
    description="Data access using eodag",
    author="Fahd Benatia",
    author_email="fahd.benatia@csgroup.eu",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    license="Copyright (c) 2021 CS Group",
    python_requires=">=3",
    package_data={"": ["index/s2_idx.geojson"]},
    entry_points={"console_scripts": ["dataship=dataship.dag.main:cli"]},
)