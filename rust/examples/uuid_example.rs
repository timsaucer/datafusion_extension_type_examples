use datafusion::{
    arrow::{
        array::{ArrayRef},
        record_batch::RecordBatch,
    },
};

use datafusion::arrow::array::Int32Array;
use datafusion::error::Result;
use datafusion::logical_expr::{ScalarUDF};
use datafusion::prelude::*;
use df_extension_types::string_conversion::StringToUuid;
use std::sync::Arc;
use df_extension_types::uuid_version::UuidVersion;

fn create_context() -> Result<SessionContext> {
    let a: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
    let batch = RecordBatch::try_from_iter(vec![("a", a)])?;
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // get a DataFrame from the context
    let mut df = ctx.table("t").await?;

    // Create the string UUIDs
    df = df.select(vec![uuid().alias("string_uuid")])?;

    // Convert string UUIDs to canonical extension UUIDs
    let string_to_udf = ScalarUDF::new_from_impl(StringToUuid::default());
    df = df.with_column("uuid",
        string_to_udf.call(vec![col("string_uuid")]),
    )?;

    // Extract version number from canonical extension UUIDs
    let version = ScalarUDF::new_from_impl(UuidVersion::default());
    df = df.with_column("version", version.call(vec![col("uuid")]))?;

    df.show().await?;

    Ok(())
}
