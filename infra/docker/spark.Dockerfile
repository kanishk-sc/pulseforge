FROM ghcr.io/astral-sh/uv:0.11.16 AS uv
FROM spark:3.5.9-scala2.12-java17-python3-ubuntu

ARG SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.13,org.apache.hadoop:hadoop-aws:3.3.4"

USER root
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv python install 3.12.13 \
    && uv venv /opt/pulseforge/.venv --python 3.12.13 \
    && uv pip install --python /opt/pulseforge/.venv/bin/python "psycopg[binary]==3.2.10" \
    && mkdir -p /opt/spark/checkpoints /home/spark/.ivy2 \
    && chown -R 185:185 /opt/pulseforge /opt/spark/checkpoints /home/spark

COPY --chown=185:185 infra/docker/prefetch_spark_packages.py /opt/pulseforge/prefetch.py

ENV HOME=/home/spark \
    PATH=/opt/pulseforge/.venv/bin:${PATH} \
    PYTHONPATH=/opt/pulseforge/src \
    PYSPARK_DRIVER_PYTHON=/opt/pulseforge/.venv/bin/python \
    PYSPARK_PYTHON=/opt/pulseforge/.venv/bin/python \
    SPARK_PACKAGES=${SPARK_PACKAGES}

USER 185
RUN /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/home/spark/.ivy2 \
    --packages "${SPARK_PACKAGES}" \
    /opt/pulseforge/prefetch.py

COPY --chown=185:185 src /opt/pulseforge/src

ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["/opt/spark/bin/spark-submit --conf spark.jars.ivy=/home/spark/.ivy2 --packages \"${SPARK_PACKAGES}\" /opt/pulseforge/src/pulseforge/streaming/main.py"]
