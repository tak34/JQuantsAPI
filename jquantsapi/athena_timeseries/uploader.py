from typing import Dict, Optional

import awswrangler
import pandas as pd


def upload(
    *,
    boto3_session,
    glue_db_name: str,
    s3_path: str,
    table_name: str,
    df: pd.DataFrame,
    dtype: Optional[Dict[str, str]] = None,
):
    _dtype = {
        "partition_dt": "date",
        "dt": "timestamp",
        "symbol": "string",
    }

    for key, value in _dtype.items():
        if key not in df.columns:
            raise ValueError(f"Column {key} must be given with dtype {value}")

    if dtype is not None:
        for k, v in dtype.items():
            _dtype[k] = v

    return awswrangler.s3.to_parquet(
        df=df,
        partition_cols=["partition_dt", "symbol"],
        dataset=True,
        database=glue_db_name,
        table=table_name,
        path=f"{s3_path}/{table_name}",
        boto3_session=boto3_session,
        mode="overwrite_partitions",
        concurrent_partitioning=True,
        dtype=_dtype,
    )
