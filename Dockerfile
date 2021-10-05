# Set base image
FROM ubuntu:20.04
#Set the working directory in the container
WORKDIR /work

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install system dependencies
RUN apt-get update -y && apt-get install -y python3.8 && apt-get install -y python3-pip git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir -U pip

RUN pip3 install --no-cache-dir -U setuptools setuptools_scm wheel

COPY src setup* /work/
RUN pip3 install --no-cache-dir .

ENTRYPOINT ["ewoc_dag"]
