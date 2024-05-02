"""
- JQuants APIのPremium planに加入してる前提で作成している
- 既存データの更新のみ行う。新規に取得することは考慮してない。
― list, price, topixの3つのテーブルのみに対応
"""

import datetime as dt

import athena_timeseries
import boto3
import pandas as pd
from lib import utils
from lib.jquantsapi import JQuantsAPI

# JQuantsAPI利用するときに使うIDを記載したもの
PATH_ID = "keys/id.csv"
# 取得したデータの保存場所
RAW = "data"

# ファイル名に記入する日付
save_date = dt.datetime.now().strftime("%Y%m%d")

# IDとか読み込み
df_id = pd.read_csv(PATH_ID, index_col=0)
address = df_id.at["address", "value"]
passcode = df_id.at["pass", "value"]
discord_url = df_id.at["discord_url", "value"]
line_token = df_id.at["line_token", "value"]
jqapi = JQuantsAPI(address=address, passcode=passcode)

# athena_timeseriesの設定
boto3_session = boto3.Session(region_name="ap-northeast-1")
tsdb = athena_timeseries.AthenaTimeSeries(
    boto3_session=boto3_session,
    glue_db_name="jquants_api",
    s3_path="s3://japanese-stocks/jquants-api",
)


###########################
# 更新に使う関数
###########################
def check_adjustment_factor(df_price):
    """
    株価データ（df_price）更新時にAdjustmentFactorが1ではない銘柄を抽出する。
    もしadjustment_factorが1ではない銘柄があれば、discordに通知する。
    """
    # adjustment_factorが1ではない銘柄を抽出
    df_not_one = df_price[df_price["AdjustmentFactor"] != 1]
    if not df_not_one.empty:
        print(f"adjustment_factor is not 1 in df_price: {df_not_one['Code'].unique()}")
        # discordに通知
        utils.discord_notify(
            f"adjustment_factor is not 1 in df_price: {df_not_one['Code'].unique()}",
            discord_url,
            line_token,
        )


def fetch_latest_data(table_name, start_dt, end_dt):
    """
    J-QuantsのAPIから最新のデータを取得する。
    S3にアップロードするための前処理も行う。
    """
    # 最新のデータを取得する
    if table_name == "list":
        df_latest = jqapi.get_list_range(start_dt=start_dt, end_dt=end_dt)
    elif table_name == "price":
        df_latest = jqapi.get_price_range(start_dt=start_dt, end_dt=end_dt)
    elif table_name == "topix":
        df_latest = jqapi.get_topix(start_dt=start_dt, end_dt=end_dt)
    else:
        raise ValueError(f"Invalid table_name: {table_name}")

    # 取得したデータが空ならNoneを返す
    if df_latest is None or df_latest.empty:
        return None

    if table_name == "price":
        # AdjustmentFactorが1ではない銘柄を抽出し、ログに残す
        check_adjustment_factor(df_latest)

    # なんかdtアクセサリのエラーが出るので追加。lambdaでのpandasのバージョンの問題っぽい
    df_latest.loc[:, "date"] = pd.to_datetime(df_latest["Date"], format="%Y-%m-%d")
    df_latest.drop(columns=["Date"], axis=1, inplace=True)

    # J-QuantsのデータをS3に入れる用に前処理する
    # 列名をすべて小文字にする
    df_latest.columns = df_latest.columns.str.lower()
    # symbol represent a group of data for given data columns
    df_latest["symbol"] = "jquants_api"
    # timestamp should be UTC timezone but without tz info
    df_latest["dt"] = df_latest["date"].dt.tz_localize(None)
    # partition_dt must be date, data will be updated partition by partition with use of this column.
    # Every time, you have to upload all the data for a given partition_dt, otherwise older will be gone.
    df_latest["partition_dt"] = df_latest["dt"].dt.date.map(lambda x: x.replace(day=1))

    return df_latest


def update_jquants_api(table_name):
    """
    J-QuantsのAPIからデータを取得し、S3にアップロードする。
    （既存データの更新のみ。期間を指定した新規データの取得は行えない）
    """
    dt_now = dt.datetime.now()
    start_dt_s3 = pd.Timestamp(
        year=dt_now.year, month=dt_now.month, day=1
    ) - pd.DateOffset(months=1)

    # 既存のデータを更新する
    # その月のデータを取得する。無ければ空のテーブルが返ってくる
    df_old = tsdb.query(
        table_name=table_name,
        field="*",
        start_dt=start_dt_s3.strftime("%Y-%m-%d %H:%M:%S"),
        symbols=["jquants_api"],
    )
    start_dt_jquants = pd.Timestamp(
        df_old["date"].iloc[-1], tz="Asia/Tokyo"
    ) + dt.timedelta(days=1)

    end_dt = pd.Timestamp.now(tz="Asia/Tokyo")

    # J-Quantsからデータ取得
    if start_dt_jquants <= end_dt:
        # 最新のデータを取得する
        df_new = fetch_latest_data(
            table_name=table_name, start_dt=start_dt_jquants, end_dt=end_dt
        )
        if df_new is not None:
            # データ型をS3のデータに合わせる
            df_new = df_new.astype(df_old.dtypes)
            # 最新データがあった場合は既存データと縦に結合する
            cols_subset = ["code", "date"]
            if "code" not in df_old.columns:
                cols_subset = ["date"]
            df_updated = (
                pd.concat((df_old, df_new))
                .drop_duplicates(subset=cols_subset, keep="last")
                .sort_values(by="date")
                .reset_index(drop=True)
            )
            # ちゃんと更新できてれば列数は増えないはず。ここで確認
            assert df_updated.shape[1] == df_old.shape[1]
            # メモリ削減
            df_updated = utils.reduce_mem_usage(df_updated)
            # S3にアップロード
            tsdb.upload(table_name=table_name, df=df_updated)
            # ログ残して終わり
            print(f"Renewed and uploaded: {table_name}")
            utils.discord_notify(
                f"Renewed and uploaded: {table_name}",
                discord_url,
                line_token,
            )
        else:
            print(f"There's no new data in {table_name}.")
            print(
                f"({start_dt_jquants.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})"
            )
            utils.discord_notify(
                f"There's no new data in {table_name}. ({start_dt_jquants.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})",
                discord_url,
                line_token,
            )
    else:
        # 既に最新のデータがあるか、日付の設定がおかしいのでデータは取ってこない
        print("It's already the latest data or start_dt is not appropriate.")
        print(
            f"({table_name}: {start_dt_jquants.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})"
        )
        utils.discord_notify(
            f"Failed to update {table_name}. ({start_dt_jquants.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})",
            discord_url,
            line_token,
        )


###########################
# 各情報の更新
###########################
# 銘柄情報の更新
update_jquants_api(table_name="list")
# 株価データの更新
update_jquants_api(table_name="price")
# TOPIX指数の更新
update_jquants_api(table_name="topix")


# tmp = pd.DataFrame([["a", 2], ["b", 1]], columns=["Code", "AdjustmentFactor"])
# check_adjustment_factor(tmp)
