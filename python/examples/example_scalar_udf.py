from __future__ import annotations

from datafusion import SessionContext, col, udf, functions as F
from df_extension_types_py import StringToUuidUDF, UuidVersionUDF, UuidToStringUDF
import pyarrow as pa

def main():
    ctx = SessionContext()

    string_to_uuid = udf(StringToUuidUDF())
    uuid_version = udf(UuidVersionUDF())
    uuid_to_string = udf(UuidToStringUDF())

    batch = pa.record_batch({"idx": pa.array(range(10))})
    df = (
        ctx.create_dataframe([[batch]])
        .with_column("uuid_string", F.uuid())
        .with_column("uuid", string_to_uuid(col("uuid_string")))
        .with_column("uuid_string_round_trip", uuid_to_string(col("uuid")))
        .with_column("uuid_version", uuid_version(col("uuid")))
        .with_column("uuid_version_from_string", uuid_version(col("uuid_string")))
    )

    df.show()

    print("\nField names and data types:")
    for field in df.schema():
        print(f"\t{field.name} {field.type}")

if __name__ == "__main__":
    main()