mod data;
mod delta_manager;
mod iceberg_manager;

use data::DataGen;
//use delta_manager::Delta;
use iceberg_manager::IcebergManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let datagen = DataGen::new();

    //let mut delta = DeltaManager::new();
    //let versions = delta.create_and_populate_table(datagen).await?;
    //delta.read_delta_table_at_version(versions[2])?;

    let iceberg = IcebergManager::new(
        String::from("tbl"),
        String::from("http://localhost:8060/catalog"),
        String::from("ns_1"),
    );

    iceberg.create_and_populate_table(datagen).await?;

    //iceberg.list_tables().await?;

    //iceberg
    //    .drop_table(String::from("./iceberg-play-table"))
    //    .await?;

    Ok(())
}
