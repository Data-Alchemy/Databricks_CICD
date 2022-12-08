FROM docker.io/bitnami/spark:3.3
LABEL maintainer="sebastian.hansen@dataalchemy.dev"
# Switch to root user for access to apt-get install

ENV VIRTUAL_ENV=/usr/databricks_cicd/app/.venv/
ENV POETRY_HOME=/opt/bitnami/poetry/
ENV POETRY_VENV=/opt/bitnami/poetry/poetry-venv
ENV POETRY_CACHE_DIR=/opt/.cache
ENV PATH=/opt/bitnami/poetry/bin:$PATH
ENV PATH=/usr/databricks_cicd/app/.venv/bin:$PATH
#########################################
# installing dependencies 
USER root
RUN     sudo su & \
        apt-get -y update  && \
        apt-get install -y sudo && \
        apt-get install -y curl 

# add poetry for python 
RUN     curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/bitnami/poetry python3 - 
RUN     pip uninstall pyspark
RUN     poetry config virtualenvs.in-project true
RUN     poetry config virtualenvs.prefer-active-python true


#########################################

# Create app directory
RUN mkdir -p /usr/databricks_cicd/app/
WORKDIR /usr/databricks_cicd/app/

ADD     poetry.lock /usr/databricks_cicd/app/poetry.lock
ADD     pyproject.toml /usr/databricks_cicd/app/pyproject.toml
#COPY    ./src /usr/databricks_cicd/app/src

RUN     cd /usr/databricks_cicd/app/ && \
        poetry env use python3.8 && \
        poetry check && \
        poetry install --no-interaction --no-cache

ENTRYPOINT $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master