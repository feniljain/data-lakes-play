ROOT_CLIENT_ID=95b5795b647510d5
ROOT_CLIENT_SECRET=3c8f137bc4e2fd77dc63b22a08c9b8ab

# realm: default-realm root principal credentials: 95b5795b647510d5:3c8f137bc4e2fd77dc63b22a08c9b8ab

# tokenpayload=$(echo "grant_type=client_credentials&client_id=$ROOT_CLIENT_ID&client_secret=$ROOT_CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL")
#
# curl -i -X POST \
#   http://localhost:8181/api/catalog/v1/oauth/tokens \
#   -d "$tokenpayload"
#
# exit 0;

curl -i -X POST -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://localhost:8181/api/management/v1/catalogs \
  -d '{"catalog":{"name": "polariscatalog", "type": "INTERNAL", "properties": {
        "default-base-location": "s3://kafka-testing-files/iceberg_polaris_test_dir/tbl_dir"
    },"storageConfigInfo": {
        "roleArn": "arn:aws:iam::670514002493:role/kafka",
        "storageType": "S3",
        "allowedLocations": [
            "s3://kafka-testing-files/iceberg_polaris_test_dir/tbl_dir/"
        ]
    } } }'

curl -X POST "http://localhost:8181/api/management/v1/principals" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"name": "polarisuser", "type": "user"}'

curl -X POST "http://localhost:8181/api/management/v1/principal-roles" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "polarisuserrole"}}'

curl -X PUT "http://localhost:8181/api/management/v1/principals/polarisuser/principal-roles" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "polarisuserrole"}}'

curl -X POST "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "polariscatalogrole"}}'

curl -X PUT "http://localhost:8181/api/management/v1/principal-roles/polarisuserrole/catalog-roles/polariscatalog" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "polariscatalogrole"}}'

curl -X PUT "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'

curl -X PUT "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "TABLE_FULL_METADATA"}}'

curl -X PUT "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "VIEW_FULL_METADATA"}}'

curl -X PUT "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants" \
  -H "Authorization: Bearer principal:root;password:$ROOT_CLIENT_SECRET;realm:default-realm;role:ALL" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "NAMESPACE_FULL_METADATA"}}'
