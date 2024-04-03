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
jqapi = JQuantsAPI(address=address, passcode=passcode)

###########################
# 銘柄情報の更新（df_list）
###########################
if not FETCH_ALL_DATA:
    # 既存のデータを更新する
    # 古いやつのパスを取得しておく（新しいのを保存してから消す）
    path_list = glob.glob(RAW + f"/list_*.pkl")
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
df_l = jqapi.get_list_range(start_dt=start_dt, end_dt=end_dt)

if not FETCH_ALL_DATA:
    if df_l is None:
        print(
            f"There's no new data. ({start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')})"
        )
    else:
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
    df_l = utils.reduce_mem_usage(df_l)
    df_l.to_pickle(RAW + f"/list_20240101_{save_date}.pkl")
    print(f"save file:  list_20240101_{save_date}.pkl")
