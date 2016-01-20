FROM quay.io/jeremiahsavage/cdis_base

USER root
RUN apt-get update && apt-get install -y --force-yes \
    openjdk-8-jre-headless

USER ubuntu
ENV HOME /home/ubuntu

ENV cocleaning-tool 0.1a

RUN mkdir -p ${HOME}/tools/cocleaning-tool
ADD docker/GenomeAnalysisTK.jar ${HOME}/tools/
ADD cocleaning-tool ${HOME}/tools/cocleaning-tool/
ADD setup.* ${HOME}/tools/cocleaning-tool/

RUN /bin/bash -c "source ${HOME}/.local/bin/virtualenvwrapper.sh \
    && source ~/.virtualenvs/p3/bin/activate \
    && cd ~/tools/cocleaning-tool \
    && pip install -e ."

WORKDIR ${HOME}
