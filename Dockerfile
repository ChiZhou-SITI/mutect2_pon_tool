FROM quay.io/jeremiahsavage/cdis_base

USER root
RUN apt-get update && apt-get install -y --force-yes \
    openjdk-8-jre-headless

USER ubuntu
ENV HOME /home/ubuntu

ENV mutect2-pon-tool 0.1c

RUN mkdir -p ${HOME}/tools/mutect2-pon-tool
ADD docker/GenomeAnalysisTK.jar ${HOME}/tools/
ADD mutect2-pon-tool ${HOME}/tools/mutect2-pon-tool/
ADD setup.* ${HOME}/tools/mutect2-pon-tool/

RUN /bin/bash -c "source ${HOME}/.local/bin/virtualenvwrapper.sh \
    && source ~/.virtualenvs/p3/bin/activate \
    && cd ~/tools/mutect2-pon-tool \
    && pip install -e ."

WORKDIR ${HOME}
