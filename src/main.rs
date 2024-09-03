use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::ReaderBuilder;
use delta_kernel::{engine::sync::SyncEngine as DKSyncEngine, table::Table as DKTable};
use deltalake::{protocol::checkpoints::create_checkpoint, DeltaOps};
use rand::{distributions::Alphanumeric, rngs::ThreadRng, seq::IteratorRandom, Rng};
use serde::Serialize;
use std::fs::create_dir_all;
use std::sync::Arc;

#[derive(Serialize)]
struct Data {
    id: i32,
    value: String,
}

impl Data {
    fn new(mut rng: ThreadRng) -> Self {
        Data {
            id: rng.gen_range(0..100),
            value: rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();

    let table_path = "./delta-play-table";
    create_dir_all(table_path)?;

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]);

    let mut decoder = ReaderBuilder::new(Arc::new(schema))
        .build_decoder()
        .expect("could not bulid decoder");

    let mut versions = vec![];
    for i in 1..=25 {
        let mut cnt = 0;

        let rows = Vec::from_iter(std::iter::from_fn(|| {
            let rng_c = rng.clone();

            cnt += 1;
            if cnt < 5 {
                Some(Data::new(rng_c))
            } else {
                None
            }
        }));

        decoder.serialize(&rows).expect("could not serialize rows");

        let batch = vec![decoder
            .flush()
            .expect("could not flush data to recordbatch")
            .expect("flush returned None")];

        let ops = DeltaOps::try_from_uri(table_path)
            .await
            .expect("could not build delta ops for creating delta lake");

        let table = ops.write(batch).await.expect("could not write to table");

        versions.push(table.version() as u64);

        if i % 5 == 0 {
            create_checkpoint(&table)
                .await
                .expect("could not create checkpoint");
        }
    }

    println!("Delta table created successfully at {}", table_path);

    // delta-kernel table
    let dk_table =
        DKTable::try_from_uri(table_path).expect("could nto create a table from delta_kernel");

    let dk_engine = DKSyncEngine::new();

    let _version_opt = versions.clone().into_iter().choose(&mut rng);
    let version_opt = versions.into_iter().last();

    println!("Generating snapshot of version: {:?}", version_opt);

    let dk_snapshot = dk_table
        .snapshot(&dk_engine, version_opt)
        .expect("could not create snapshot");

    assert!(dk_snapshot.version() == version_opt.unwrap());

    let dk_metadata = dk_snapshot.metadata();

    println!("schema created_time: {:?}", dk_metadata.created_time);
    println!("schema ID: {}", dk_metadata.id);
    println!("schema name: {:?}", dk_metadata.name);
    println!("schema string: {}", dk_metadata.schema_string);

    Ok(())
}
