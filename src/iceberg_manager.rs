use super::data::DataGen;

use std::{collections::HashMap, sync::Arc};

use iceberg::{
    io::FileIOBuilder,
    spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type},
    writer::file_writer::{
        location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        FileWriter, FileWriterBuilder, ParquetWriterBuilder,
    },
    Catalog, NamespaceIdent, TableCreation, TableIdent,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::file::properties::WriterProperties;

pub struct IcebergManager {
    table_name: String,
    catalog: RestCatalog,
    namespace_id: NamespaceIdent,
}

impl IcebergManager {
    pub fn new(table_name: String, catalog_uri: String, namespace_id: String) -> Self {
        let namespace_id = NamespaceIdent::new(namespace_id);

        let config = RestCatalogConfig::builder()
            .uri(catalog_uri)
            .warehouse(String::from("test"))
            .build();

        let catalog = RestCatalog::new(config);

        Self {
            table_name,
            catalog,
            namespace_id,
        }
    }

    pub async fn create_and_populate_table(&self, datagen: DataGen) -> anyhow::Result<()> {
        // init namespace
        if !self.catalog.namespace_exists(&self.namespace_id).await? {
            self.catalog
                .create_namespace(&self.namespace_id, HashMap::new())
                .await?;
        }

        // create table

        let schema_builder = Schema::builder();

        let schema = schema_builder
            .with_fields(vec![
                Arc::new(NestedField {
                    id: 0,
                    name: "id".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }),
                Arc::new(NestedField {
                    id: 1,
                    name: "vaule".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }),
            ])
            .build()?;

        // let schema = arrow_schema_to_schema(&datagen.arrow_schema)?; // This does not work

        println!("built schema");

        let table_idents = self.catalog.list_tables(&self.namespace_id).await?;

        let tbl_ident_opt = table_idents
            .iter()
            .find(|tbl_ident| tbl_ident.name() == self.table_name);

        let tbl_metadata = if let Some(tbl_ident) = tbl_ident_opt {
            self.catalog
                .load_table(&tbl_ident)
                .await?
                .metadata()
                .clone()
        } else {
            let table_id = TableIdent::new(self.namespace_id.clone(), self.table_name.clone());

            let table_creation = TableCreation::builder()
                .name(table_id.name.clone())
                .schema(schema.clone())
                .build();

            let table = self
                .catalog
                .create_table(&table_id.namespace, table_creation)
                .await?;

            println!("Table created: {:?}", table.metadata());

            table.metadata().clone()
        };

        let mut props = HashMap::new();

        props.insert("s3.endpoint", "s3://warehouse/");
        props.insert("s3.access-key-id", "admin");
        props.insert("s3.secret-access-key", "password");
        props.insert("s3.region", "local-01");

        let file_io = FileIOBuilder::new("s3").with_props(props).build()?;

        println!("File IO created");

        let loc_gen = DefaultLocationGenerator::new(tbl_metadata)?;
        let file_name_gen =
            DefaultFileNameGenerator::new(String::new(), None, DataFileFormat::Parquet);

        // register a writer
        let mut writer = ParquetWriterBuilder::new(
            WriterProperties::new(),
            Arc::new(schema),
            file_io,
            loc_gen,
            file_name_gen,
        )
        .build()
        .await?;

        println!("Created Parquet Writer");

        // write generated data to it
        let record_batch = datagen.convert_to_arrow_record_batch(datagen.gen_n_data(5));

        writer.write(&record_batch).await?;

        println!("Wrote data to it: {:?}", record_batch);

        Ok(())
    }

    pub async fn list_tables(&self) -> anyhow::Result<()> {
        let tables = self.catalog.list_tables(&self.namespace_id).await?;
        for table in tables {
            println!("{}", table.name);
        }

        Ok(())
    }

    pub async fn drop_table(&self, tbl_name: String) -> anyhow::Result<()> {
        let table_id = TableIdent::new(self.namespace_id.clone(), tbl_name);
        self.catalog.drop_table(&table_id).await?;

        Ok(())
    }

    //pub fn read_delta_table_at_version(self, version: u64) -> anyhow::Result<()> {}
}

/*
* 1. paste:
* ```
spark.jars.packages                                  org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1
spark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

spark.sql.defaultCatalog                             local
spark.sql.catalog.local                              org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.warehouse                    /home/iceberg/warehouse
spark.sql.catalog.local.catalog-impl                 org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.local.uri                          http://localhost:8060/catalog
spark.sql.catalog.local.token                        dummy
spark.sql.catalog.local.warehouse                    00000000-0000-0000-0000-000000000000/test

spark.sql.catalog.spark_catalog                      org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type                 hive
* ```
* 2. create namespace ns_1;
* 3. create table ns_1.tbl(id bigint, value string) using iceberg;
* 4. insert into ns_1.tbl values(1, "1");
*/

/*
* Warehouse ID: bc836228-6ead-11ef-91cf-4ba7d2ac8cd3
*/
