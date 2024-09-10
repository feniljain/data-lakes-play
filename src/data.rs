use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema},
    json::ReaderBuilder,
};
use rand::{distributions::Alphanumeric, rngs::ThreadRng, Rng};
use serde::Serialize;

#[derive(Serialize)]
pub struct Data {
    id: i32,
    value: String,
}

impl Data {
    pub fn new(mut rng: ThreadRng) -> Self {
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

pub struct DataGen {
    rng: ThreadRng,
    pub arrow_schema: Schema,
}

impl DataGen {
    pub fn new() -> Self {
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        Self {
            rng: rand::thread_rng(),
            arrow_schema,
        }
    }

    pub fn gen_n_data(&self, n: u32) -> Vec<Data> {
        let mut cnt = 0;

        Vec::from_iter(std::iter::from_fn(|| {
            let rng_c = self.rng.clone();

            cnt += 1;
            if cnt < n {
                Some(Data::new(rng_c))
            } else {
                None
            }
        }))
    }

    pub fn convert_to_arrow_record_batch(&self, rows: Vec<Data>) -> RecordBatch {
        let mut decoder = ReaderBuilder::new(Arc::new(self.arrow_schema.clone()))
            .build_decoder()
            .expect("could not build decoder");

        decoder.serialize(&rows).expect("could not serialize rows");

        decoder
            .flush()
            .expect("could not flush data to recordbatch")
            .expect("flush returned None")
    }
}
