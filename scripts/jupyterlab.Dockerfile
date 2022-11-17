FROM cluster-base

ARG spark_version=$SPARK_VERSION
ARG jupyterlab_version=$JUPYTERLAB_VERSION

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    apt-get install -y r-base && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=