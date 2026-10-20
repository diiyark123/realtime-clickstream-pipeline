# Official Apache Spark image with Python 3
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
COPY transforms.py streaming_job.py ./
ENV PATH="/opt/spark/bin:${PATH}"
# The Kafka connector is pulled from Maven Central on first start and cached in /root/.ivy2
CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "--conf", "spark.jars.ivy=/root/.ivy2", \
     "--py-files", "/app/transforms.py", \
     "/app/streaming_job.py"]
