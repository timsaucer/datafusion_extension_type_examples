use datafusion::arrow::{array::ArrayRef, record_batch::RecordBatch};

use datafusion::arrow::array::Int32Array;
use datafusion::error::Result;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::*;
use df_extension_types::string_to_uuid::StringToUuid;
use df_extension_types::uuid_to_string::UuidToString;
use df_extension_types::uuid_version::UuidVersion;
use std::sync::Arc;

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
    let string_to_uuid = ScalarUDF::new_from_impl(StringToUuid::default());
    df = df.with_column("uuid", string_to_uuid.call(vec![col("string_uuid")]))?;

    // Extract version number from canonical extension UUIDs
    let version = ScalarUDF::new_from_impl(UuidVersion::default());
    df = df.with_column("version", version.call(vec![col("uuid")]))?;

    // Convert back to a string
    let uuid_to_string = ScalarUDF::new_from_impl(UuidToString::default());
    df = df.with_column("string_round_trip", uuid_to_string.call(vec![col("uuid")]))?;

    df = df.with_column(
        "version_from_string",
        version.call(vec![col("string_uuid")]),
    )?;

    df.show().await?;

    Ok(())
}
