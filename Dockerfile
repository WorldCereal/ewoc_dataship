# Set base image
FROM ubuntu:18.04
#Set the working directory in the container
WORKDIR work
COPY . /work
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
RUN apt-get update -y && apt-get install -y python3.7 && apt-get install -y python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir -U pip

ARG EOTILE_VERSION=0.2rc3
LABEL EOTILE="${EOTILE_VERSION}"
# Install eotile from local whl
RUN python3 -m pip install --no-cache-dir eotile-${EOTILE_VERSION}-py3-none-any.whl

RUN pip install --no-cache-dir .
ENTRYPOINT ["dataship"]
