import gc
import glob
import json
import os
import warnings
from datetime import datetime

# import japanize_matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns
from dateutil import tz

RAW = "/content/drive/MyDrive/日本株/raw/J-QuantsAPI"
PATH_ID = f"{RAW}/id.csv"

FETCH_ALL_DATA = False
