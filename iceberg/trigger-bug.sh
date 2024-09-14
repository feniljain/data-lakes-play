#!/bin/bash

# build local image of our project
docker --debug build -t delta-play ../

# get all dependencies working
echo ">>>>>>>waiting for iceberg-rest-catalog-rs server to start listening"

docker compose -p iceberg-bug -f iceberg-bug.yml up --detach
until curl http://localhost:8060 -s
do
    sleep 5
done

echo ">>>>>>>catalog server has started"

# create warehouse
curl -X POST http://localhost:8060/management/v1/warehouse -H "Content-Type: application/json" -d '{
    "warehouse-name": "test",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "storage-profile": {
        "type": "s3",
        "bucket": "warehouse",
        "key-prefix": "initial-warehouse",
        "assume-role-arn": null,
        "endpoint": "http://minio:9000",
        "region": "local-01",
        "path-style-access": true,
        "sts_role_arn": null,
        "flavor": "minio",
        "sts-enabled": false
    },
    "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": "admin",
        "aws-secret-access-key": "password"
    }
}'

echo ">>>>>>>created warehouse"

# create table
docker exec -it delta-play sh -c "delta-play create"

echo ">>>>>>>created table"

# REST catalog config for spark
docker exec -it spark-iceberg_1 sh -c "echo \"spark.jars.packages                                  org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1\nspark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\n\nspark.sql.defaultCatalog                             local\nspark.sql.catalog.local                              org.apache.iceberg.spark.SparkCatalog\nspark.sql.catalog.local.catalog-impl                 org.apache.iceberg.rest.RESTCatalog\nspark.sql.catalog.local.uri                          http://iceberg-rest-catalog-rs:8060/catalog\nspark.sql.catalog.local.token                        dummy\nspark.sql.catalog.local.warehouse                    00000000-0000-0000-0000-000000000000/test\n\" >> /opt/spark/conf/spark-defaults.conf"

echo ">>>>>>>wrote spark config"

# insert data into table
docker exec -it spark-iceberg_1 sh -c "spark-sql -e 'insert into ns_1.tbl values(1, \"1\");'"

echo ">>>>>>>successfully inserted data into table"

# write doesn't work :(
docker exec -it delta-play sh -c "delta-play write"

echo ">>>>>>>successfully wrote data into table through delta-play"

# to confirm write hasn't happened
docker exec -it spark-iceberg_1 sh -c "spark-sql -e 'select * from ns_1.tbl;'"

echo ">>>>>>>check current items in table"

# try to read snapshot created from insert stmt executed using spark, this fails due to auth issues
docker exec -it delta-play sh -c "delta-play read-snapshot"

echo ">>>>>>>read snapshot"
