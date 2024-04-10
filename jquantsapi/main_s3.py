"""
JQuants APIのPremium planに加入してる前提で作成している
"""

import datetime as dt
import gc
import glob
import json
import os
import warnings
from pathlib import Path

import athena_timeseries
import boto3
import numpy as np
import pandas as pd
import requests
from dateutil import tz
from lib import utils
from lib.jquantsapi import JQuantsAPI

# JQuantsAPI利用するときに使うIDを記載したもの
PATH_ID = "keys/id.csv"
# 全期間のデータ使う場合はTrue（Falseは更新）
FETCH_ALL_DATA = False
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
# 銘柄情報の更新（df_list）
###########################
table_name = "list"
dt_now = dt.datetime.now()
start_dt_s3 = pd.Timestamp(year=dt_now.year, month=dt_now.month, day=1)

if not FETCH_ALL_DATA:
    # 既存のデータを更新する
    # その月のデータを取得する。無ければ空のテーブルが返ってくる
    df_list = tsdb.query(
        table_name=table_name,
        field="*",
        start_dt=start_dt_s3.strftime("%Y-%m-%d %H:%M:%S"),
        symbols=["jquants_api"],
    )
    start_dt_jquants = pd.Timestamp(
        df_list["date"].iloc[-1], tz="Asia/Tokyo"
    ) + dt.timedelta(days=1)
else:
    # 以下の日にち以降の全データを指定する
    start_dt_jquants = pd.Timestamp(year=2024, month=1, day=1, tz="Asia/Tokyo")

end_dt = pd.Timestamp.now(tz="Asia/Tokyo")

# J-Quantsからデータ取得
if start_dt_jquants <= end_dt:
    df_l = jqapi.get_list_range(start_dt=start_dt_jquants, end_dt=end_dt)
    if not FETCH_ALL_DATA:
        # 既存のデータを更新する
        if df_l is not None:
            # J-QuantsのデータをS3に入れる前に前処理する
            # df_lの列名をすべて小文字にする
            df_l.columns = df_l.columns.str.lower()
            # Codeを文字列に変換
            df_l["code"] = df_l["code"].astype(str)
            # symbol represent a group of data for given data columns
            df_l["symbol"] = "jquants_api"
            # timestamp should be UTC timezone but without tz info
            df_l["dt"] = df_l["date"].dt.tz_localize(None)
            # partition_dt must be date, data will be updated partition by partition with use of this column.
            # Every time, you have to upload all the data for a given partition_dt, otherwise older will be gone.
            df_l["partition_dt"] = df_l["dt"].dt.date.map(lambda x: x.replace(day=1))

            df_list = (
                pd.concat((df_list, df_l))
                .drop_duplicates(subset=["code", "date"], keep="last")
                .sort_values(by="date")
                .reset_index(drop=True)
            )
            # ちゃんと更新できてれば列数は増えないはず。ここで確認
            assert df_list.shape[1] == df_l.shape[1]

            df_list = utils.reduce_mem_usage(df_list)
            tsdb.upload(table_name=table_name, df=df_list)
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
        # 全期間のデータを取得する
        df_l = utils.reduce_mem_usage(df_l)
        # symbol represent a group of data for given data columns
        df_l["symbol"] = "jquants_api"
        # timestamp should be UTC timezone but without tz info
        df_l["dt"] = df_l["Date"].dt.tz_localize(None)
        # partition_dt must be date, data will be updated partition by partition with use of this column.
        # Every time, you have to upload all the data for a given partition_dt, otherwise older will be gone.
        df_l["partition_dt"] = df_l["dt"].dt.date.map(lambda x: x.replace(day=1))
        tsdb.upload(table_name=table_name, df=df_l)
        # ログ残して終わり
        print(f"Uploaded: {table_name}")
        utils.discord_notify(
            f"Uploaded: {table_name}",
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

import sys

sys.exit()

###########################
# 株価データの更新（df_price）
###########################
if not FETCH_ALL_DATA:
    # 既存のデータを更新する
    # 古いやつのパスを取得しておく（新しいのを保存してから消す）
    path_price = glob.glob(RAW + "/price_*.pkl")
    assert len(path_price) == 1
    df_price = pd.read_pickle(path_price[0])
    start_dt = pd.Timestamp(df_price["Date"].iloc[-1], tz="Asia/Tokyo") + dt.timedelta(
        days=1
    )
else:
    start_dt = pd.Timestamp(year=2024, month=1, day=1, tz="Asia/Tokyo")

end_dt = pd.Timestamp.now(tz="Asia/Tokyo")
if end_dt.hour < 19:
    # データ更新時間前の場合は日付を1日ずらします。
    end_dt -= pd.Timedelta(1, unit="D")

if start_dt <= end_dt:
    df_p = jqapi.get_price_range(start_dt=start_dt, end_dt=end_dt)
    if not FETCH_ALL_DATA:
        # 既存のデータを更新する
        if df_p is not None:
            df_price = (
                pd.concat((df_price, df_p))
                .drop_duplicates(subset=["Code", "Date"], keep="last")
                .sort_values(by="Date")
                .reset_index(drop=True)
            )
            df_price = utils.reduce_mem_usage(df_price)
            df_price.to_pickle(RAW + f"/price_20240101_{save_date}.pkl")
            print(f"save file: price_20240101_{save_date}.pkl")
            # 古いファイル消す
            os.remove(path_price[0])
            print(f"removed old file: {path_price[0]}")
            del df_price

        else:
            print("There's no new data.")
            print(
                f"(df_price: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})"
            )

    else:
        # 全期間のデータを取得する
        df_p = utils.reduce_mem_usage(df_p)
        df_p.to_pickle(RAW + f"/price_20240101_{save_date}.pkl")
        print(f"save file:  price_20240101_{save_date}.pkl")
else:
    # 既に最新のデータがあるか、日付の設定がおかしいのでデータは取ってこない
    print("It's already the latest data or start_dt is not appropriate.")
    print(f"(df_price: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})")

###########################
# TOPIX指数の更新 (df_topix)
###########################
if not FETCH_ALL_DATA:
    # 既存のデータを更新する
    # 古いやつのパスを取得しておく（新しいのを保存してから消す）
    path_topix = glob.glob(RAW + "/topix_*.pkl")
    assert len(path_topix) == 1
    df_topix = pd.read_pickle(path_topix[0])
    start_dt = pd.Timestamp(df_topix["Date"].iloc[-1], tz="Asia/Tokyo") + dt.timedelta(
        days=1
    )
else:
    start_dt = pd.Timestamp(year=2024, month=1, day=1, tz="Asia/Tokyo")

end_dt = pd.Timestamp.now(tz="Asia/Tokyo")
if end_dt.hour < 19:
    # データ更新時間前の場合は日付を1日ずらします。
    end_dt -= pd.Timedelta(1, unit="D")

if start_dt <= end_dt:
    df_t = jqapi.get_topix(start_dt=start_dt, end_dt=end_dt)
    if not FETCH_ALL_DATA:
        # 既存のデータを更新する
        if df_t is not None:
            df_topix = (
                pd.concat((df_topix, df_t))
                .drop_duplicates(subset=["Date"], keep="last")
                .sort_values(by="Date")
                .reset_index(drop=True)
            )
            df_topix = utils.reduce_mem_usage(df_topix)
            df_topix.to_pickle(RAW + f"/topix_20240101_{save_date}.pkl")
            print(f"save file: topix_20240101_{save_date}.pkl")
            # 古いファイル消す
            os.remove(path_topix[0])
            print(f"removed old file: {path_topix[0]}")
            del df_topix
        else:
            print("There's no new data.")
            print(
                f"(df_topix: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})"
            )

    else:
        # 全期間のデータを取得する
        df_t = utils.reduce_mem_usage(df_t)
        df_t.to_pickle(RAW + f"/topix_20240101_{save_date}.pkl")
        print(f"save file:  topix_20240101_{save_date}.pkl")

else:
    # 既に最新のデータがあるか、日付の設定がおかしいのでデータは取ってこない
    print("It's already the latest data or start_dt is not appropriate.")
    print(f"(df_topix: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})")
