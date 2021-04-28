# Set base image
FROM ubuntu:18.04
#Set the working directory in the container
WORKDIR work
COPY . /work
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
RUN apt-get update -y && apt-get install -y python3.7 && apt-get install -y python3-pip
RUN python3 -m pip install -U pip && python3 -m pip install -r requirements.txt
# Install eotile from local whl
RUN python3 -m pip install eotile-0.1-py3-none-any.whl
RUN pip install .
ENTRYPOINT ["dataship"]