// use std::sync::Arc;

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

        let tbl = Table::new(path).await?;
        let data = tbl.read_snapshot().await?;

        println!("data: {data:?}");

        Ok(())
    }
}

