# Set base image
FROM ubuntu:18.04
#Set the working directory in the container
WORKDIR /work

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install system dependencies
RUN apt-get update -y && apt-get install -y python3.7 && apt-get install -y python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir -U pip

COPY src setup* /work/
RUN pip install --no-cache-dir .
ENTRYPOINT ["ewoc_dag"]
