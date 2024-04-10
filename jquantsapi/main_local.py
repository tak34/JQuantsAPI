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


df_id = pd.read_csv(PATH_ID, index_col=0)
address = df_id.at["address", "value"]
passcode = df_id.at["pass", "value"]
discord_url = df_id.at["discord_url", "value"]
line_token = df_id.at["line_token", "value"]
jqapi = JQuantsAPI(address=address, passcode=passcode)


###########################
# 銘柄情報の更新（df_list）
###########################
if not FETCH_ALL_DATA:
    # 既存のデータを更新する
    # 古いやつのパスを取得しておく（新しいのを保存してから消す）
    path_list = glob.glob(RAW + "/list_*.pkl")
    assert len(path_list) == 1
    df_list = pd.read_pickle(path_list[0])
    # 20240210 Codeに文字列が入るようになったのでstr型に変更
    df_list["Code"] = df_list["Code"].astype(str)
    start_dt = pd.Timestamp(df_list["Date"].iloc[-1], tz="Asia/Tokyo") + dt.timedelta(
        days=1
    )
else:
    start_dt = pd.Timestamp(year=2024, month=1, day=1, tz="Asia/Tokyo")

end_dt = pd.Timestamp.now(tz="Asia/Tokyo")

if start_dt <= end_dt:
    df_l = jqapi.get_list_range(start_dt=start_dt, end_dt=end_dt)
    if not FETCH_ALL_DATA:
        # 既存のデータを更新する
        if df_l is not None:
            df_l["Code"] = df_l["Code"].astype(str)
            df_list = (
                pd.concat((df_list, df_l))
                .drop_duplicates(subset=["Code", "Date"], keep="last")
                .sort_values(by="Date")
                .reset_index(drop=True)
            )
            df_list = utils.reduce_mem_usage(df_list)
            df_list.to_pickle(RAW + f"/list_20240101_{save_date}.pkl")
            print(f"save file: list_20240101_{save_date}.pkl")
            # 古いファイル消す
            os.remove(path_list[0])
            print(f"removed old file: {path_list[0]}")
            del df_list
        else:
            print("There's no new data.")
            print(
                f"(df_list: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})"
            )
            utils.discord_notify(
                f"Failed to update list data. (df_list: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})",
                discord_url,
                line_token,
            )

    else:
        # 全期間のデータを取得する
        df_l = utils.reduce_mem_usage(df_l)
        df_l.to_pickle(RAW + f"/list_20240101_{save_date}.pkl")
        print(f"save file:  list_20240101_{save_date}.pkl")
else:
    # 既に最新のデータがあるか、日付の設定がおかしいのでデータは取ってこない
    print("It's already the latest data or start_dt is not appropriate.")
    print(f"(df_list: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})")
    utils.discord_notify(
        f"Failed to update list data. (df_list: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})",
        discord_url,
        line_token,
    )

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
