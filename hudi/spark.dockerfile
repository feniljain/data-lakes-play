# docker run -v ~/Projects/work/e6data/delta-play/hudi/data:/data --name spark-playground --detach spark-playground

FROM apache/spark

USER root
RUN <<EOF
apt-get update
apt-get install -y vim
cp /opt/spark/bin/spark-sql /usr/local/bin/spark-sql
mkdir /opt/spark/conf
touch /opt/spark/conf/spark-defaults.conf
EOF

ENTRYPOINT ["tail", "-f", "/dev/null"]

# TODO: Include this in above script itself to add to /opt/spark/conf/spark-defaults.conf

# Hoodi Config

# spark.jars.packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0
# spark.sql.extensions org.apache.spark.sql.hudi.HoodieSparkSessionExtension

# spark.sql.defaultCatalog spark_catalog
# spark.serializer org.apache.spark.serializer.KryoSerializer
# spark.sql.catalog.spark_catalog org.apache.spark.sql.hudi.catalog.HoodieCatalog
# spark.kryo.registrator org.apache.spark.HoodieSparkKryoRegistrar
# spark.sql.warehouse.dir /data

# Delta config

# spark.jars.packages io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.2
# spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
# spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
# spark.hadoop.fs.s3a.access.key=VPP0fkoCyBZx8YU0QTjH
# spark.hadoop.fs.s3a.secret.key=iFq6k8RLJw5B0faz0cKCXeQk0w9Q8UdtaFzHuw4J
# spark.hadoop.fs.s3a.endpoint=http://minio-0:9000
# spark.hadoop.fs.s3a.path.style.access=true
# spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp"

# =======

# spark.jars.packages io.delta:delta-spark_2.12:3.2.0
# spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
# spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
# 
# spark.hadoop.fs.s3a.access.key=VPP0fkoCyBZx8YU0QTjH
# spark.hadoop.fs.s3a.secret.key=iFq6k8RLJw5B0faz0cKCXeQk0w9Q8UdtaFzHuw4J
# spark.hadoop.fs.s3a.endpoint=http://minio-0:9000
# spark.hadoop.fs.s3a.path.style.access=true
# spark.driver.extraJavaOptions="-Divy.cache.dir=/data/cache -Divy.home=/data/cache"
