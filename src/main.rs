mod data;
mod delta_manager;
mod iceberg_manager;

use data::DataGen;
//use delta_manager::Delta;
use iceberg_manager::IcebergManager;

use std::env::args;

use anyhow::bail;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let datagen = DataGen::new();

    let iceberg = IcebergManager::new(
        String::from("http://localhost:8060/catalog"),
        String::from("ns_1"),
    )?;
    let tbl_name = String::from("tbl");

    let action = args().skip(1).next();
    match action {
        Some(action_string) => match action_string.as_str() {
            "create" => iceberg.create_table(tbl_name.clone()).await?,
            "write" => iceberg.write_data(&tbl_name, datagen).await?,
            "read-snapshot" => {
                let tbl = iceberg.load_table(&tbl_name).await?;

                let snapshot_ids = iceberg.list_snapshots(tbl.metadata().clone()).await?;
                if let None = snapshot_ids.get(0) {
                    bail!("No snapshot found");
                }

                iceberg.read_table_at_version(tbl, snapshot_ids[0]).await?;
            }
            _ => bail!("invalid action passed"),
        },
        None => bail!("no action passed"),
    };

    //iceberg.list_tables().await?;

    //iceberg
    //    .drop_table(String::from("./iceberg-play-table"))
    //    .await?;

    //let mut delta = DeltaManager::new();
    //let versions = delta.create_and_populate_table(datagen).await?;
    //delta.read_delta_table_at_version(versions[2])?;

    Ok(())
}
