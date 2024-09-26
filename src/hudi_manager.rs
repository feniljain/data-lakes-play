// use std::sync::Arc;

use std::collections::HashMap;

use hudi::table::Table;

pub struct HudiManager {}

impl HudiManager {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn read_table(&self, path: &str) -> anyhow::Result<()> {
        // let ctx = SessionContext::new();
        // let hudi = HudiDataSource::new(path).await?;
        //
        // ctx.register_table("tbl", Arc::new(hudi))?;
        //
        // let df: DataFrame = ctx.sql("SELECT * from tbl").await?;
        // df.show().await?;

        let opts: HashMap<String, String> = HashMap::new();

        // use this opts to read a time travel query:
        // let opts = HashMap::from([("hoodie.read.as.of.timestamp", "0")]);

        let tbl = Table::new_with_options(path, opts).await?;
        let data = tbl.read_snapshot().await?;

        println!("data: {data:?}");

        Ok(())
    }
}
