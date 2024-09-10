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
        String::from("http://localhost:8060/catalog"),
        String::from("ns_1"),
    );

    let (tbl, tbl_metadata) = iceberg
        .create_and_populate_table(String::from("tbl"), datagen)
        .await?;

    let snapshot_ids = iceberg.list_snapshots(tbl_metadata).await?;

    iceberg.read_table_at_version(tbl, snapshot_ids[0]).await?;

    //iceberg.list_tables().await?;

    //iceberg
    //    .drop_table(String::from("./iceberg-play-table"))
    //    .await?;

    Ok(())
}
