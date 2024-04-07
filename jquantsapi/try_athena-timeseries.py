import athena_timeseries
import boto3
import numpy as np
import pandas as pd

boto3_session = boto3.Session(region_name="ap-northeast-1")

tsdb = athena_timeseries.AthenaTimeSeries(
    boto3_session=boto3_session,
    glue_db_name="jquantsapi_list",
    s3_path="s3://japanese-stocks/jquants-api",
)

# Prepare example data, your data need to have 3 columns named symbol, dt, partition_dt
df = pd.DataFrame(np.random.randn(100, 4))

df.columns = ["open", "high", "low", "close"]

# symbol represent a group of data for given data columns
df["symbol"] = "BTCUSDT"

# timestamp should be UTC timezone but without tz info
df["dt"] = pd.date_range("2022-01-01", "2022-05-01", freq="D")[:100]

# partition_dt must be date, data will be updated partition by partition with use of this column.
# Every time, you have to upload all the data for a given partition_dt, otherwise older will be gone.
df["partition_dt"] = df["dt"].dt.date.map(lambda x: x.replace(day=1))

tsdb.upload(table_name="example_table", df=df)
