use super::data::DataGen;

use std::fs::create_dir_all;

use delta_kernel::{engine::sync::SyncEngine as DKSyncEngine, table::Table as DKTable};
use deltalake::{protocol::checkpoints::create_checkpoint, DeltaOps};

pub struct DeltaManager {
    table_path: String,
}

impl DeltaManager {
    pub fn new() -> Self {
        Self {
            table_path: String::from("./delta-play-table"),
        }
    }

    pub async fn create_and_populate_table(
        &mut self,
        datagen: DataGen,
    ) -> anyhow::Result<Vec<u64>> {
        create_dir_all(&self.table_path)?;

        let mut versions = vec![];
        for i in 1..=25 {
            let rows = datagen.gen_n_data(5);

            let record_batch = datagen.convert_to_arrow_record_batch(rows);

            let ops = DeltaOps::try_from_uri(&self.table_path)
                .await
                .expect("could not build delta ops for creating delta lake");

            let table = ops
                .write(vec![record_batch])
                .await
                .expect("could not write to table");

            versions.push(table.version() as u64);

            if i % 5 == 0 {
                create_checkpoint(&table)
                    .await
                    .expect("could not create checkpoint");
            }
        }

        println!("Delta table created successfully at {}", self.table_path);

        Ok(versions)
    }

    pub fn read_delta_table_at_version(self, version: u64) -> anyhow::Result<()> {
        // delta-kernel table
        let dk_table = DKTable::try_from_uri(self.table_path)
            .expect("could nto create a table from delta_kernel");

        let dk_engine = DKSyncEngine::new();

        let version_opt = Some(version);

        println!("Generating snapshot of version: {:?}", version_opt);

        let dk_snapshot = dk_table
            .snapshot(&dk_engine, version_opt)
            .expect("could not create snapshot");

        assert!(dk_snapshot.version() == version);

        let dk_metadata = dk_snapshot.metadata();

        println!("schema created_time: {:?}", dk_metadata.created_time);
        println!("schema ID: {}", dk_metadata.id);
        println!("schema name: {:?}", dk_metadata.name);
        println!("schema string: {}", dk_metadata.schema_string);

        Ok(())
    }
}
