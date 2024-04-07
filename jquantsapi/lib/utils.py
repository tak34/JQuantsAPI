import numpy as np
import requests


def reduce_mem_usage(df):
    """iterate through all the columns of a dataframe and modify the data type
    to reduce memory usage.
    """
    start_mem = df.memory_usage().sum() / 1024**2
    print("Memory usage of dataframe is {:.2f} MB".format(start_mem))

    for col in df.columns:
        col_type = df[col].dtype

        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == "int":
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                try:
                    #                 if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    #                     df[col] = df[col].astype(np.float16)
                    #                 elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    #                     df[col] = df[col].astype(np.float32)
                    if (
                        c_min > np.finfo(np.float32).min
                        and c_max < np.finfo(np.float32).max
                    ):
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
                except:
                    continue
    #         else:
    #             df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    print("Memory usage after optimization is: {:.2f} MB".format(end_mem))
    print("Decreased by {:.1f}%".format(100 * (start_mem - end_mem) / start_mem))

    return df


def line_notify(message, token):
    # https://qiita.com/pontyo4/items/10aa0ba0a17aee19e88e
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization": "Bearer " + token}
    payload = {"message": message}
    _ = requests.post(url, headers=headers, params=payload)


def discord_notify(message, discord_url, line_token):
    data = {"content": message}
    try:
        # メッセージの送信
        response_body = requests.post(discord_url, data=data)
        response_body.raise_for_status()

    except Exception as e:
        # Discordに問題があった場合、ラインへ送信
        line_notify(e, line_token)
