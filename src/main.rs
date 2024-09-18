mod data;
mod delta_manager;
mod hudi_manager;
mod iceberg_manager;

use data::DataGen;
//use delta_manager::Delta;
//use iceberg_manager::IcebergManager;
use hudi_manager::HudiManager;

use std::env::args;

use anyhow::bail;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let datagen = DataGen::new();

    // =====================================================

    // let tbl_name = String::from("tbl");
    // let iceberg = IcebergManager::new(
    //     String::from("http://iceberg-rest-catalog-rs:8060/catalog"),
    //     String::from("ns_1"),
    //     String::from("test"),
    // )
    // .await?;

    // let action = args().skip(1).next();
    // match action {
    //     Some(action_string) => match action_string.as_str() {
    //         "create" => iceberg.create_table(tbl_name.clone()).await?,
    //         "write" => iceberg.write_data(&tbl_name, datagen).await?,
    //         "read-snapshot" => {
    //             let tbl = iceberg.load_table(&tbl_name).await?;

    //             let snapshot_ids = iceberg.list_snapshots(tbl.metadata().clone()).await?;
    //             if let None = snapshot_ids.get(0) {
    //                 bail!("No snapshot found");
    //             }

    //             iceberg.read_table_at_version(tbl, snapshot_ids[0]).await?;
    //         }
    //         _ => bail!("invalid action passed"),
    //     },
    //     None => bail!("no action passed"),
    // };

    //iceberg.list_tables().await?;

    //iceberg
    //    .drop_table(String::from("./iceberg-play-table"))
    //    .await?;

    // =====================================================

    //let mut delta = DeltaManager::new();
    //let versions = delta.create_and_populate_table(datagen).await?;
    //delta.read_delta_table_at_version(versions[2])?;

    // =====================================================

    let hudi_manager = HudiManager::new();

    // this internally uses `Uri::parse_from_file_path()` and it accepts dir path when given like:
    // `/Users/feniljain` or `/tmp/data` (basically starting with root)
    // But when same method is given paths like `../hudi/data`, it does not accept it
    //
    // I believe hudi-rs should use same methods as delta-rs or iceberg-rs
    hudi_manager
        .read_table("/Users/feniljain/Projects/work/e6data/delta-play/hudi/data/tbl")
        .await?;

    Ok(())
}
