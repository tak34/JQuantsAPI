import json
import os
import warnings
from datetime import datetime

# import japanize_matplotlib
# import numpy as np
import pandas as pd
import requests
from dateutil import tz
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# import gc


warnings.simplefilter("ignore")


class JQuantsAPI:
    """
    J-Quants API からデータを取得する
    ref. https://jpx.gitbook.io/j-quants-api/
    """

    JQUANTS_API_BASE = "https://api.jquants.com/v1"

    def __init__(self, address: str, passcode: str) -> None:
        """
        Args:
            address : J-Quantsにログインするのに使うアドレス
            passcode: J-Quantsにログインするのに使うパスワード
        """
        self.address = address
        self.passcode = passcode
        self._refresh_token = ""
        self._id_token = ""
        self._id_token_expire = pd.Timestamp.utcnow()

    def _base_headers(self) -> dict:
        """
        J-Quants API にアクセスする際にヘッダーにIDトークンを設定
        """
        if not self._refresh_token:
            self.get_refresh_token()
        headers = {"Authorization": f"Bearer {self.get_id_token()}"}
        return headers

    def _request_session(
        self,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    ):
        """
        requests の session 取得

        リトライを設定

        Args:
            N/A
        Returns:
            requests.session
        """
        retry_strategy = Retry(
            total=3, status_forcelist=status_forcelist, allowed_methods=method_whitelist
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        s = requests.Session()
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def _get(self, url: str, params: dict = None) -> requests.Response:
        """
        requests の get 用ラッパー

        ヘッダーにアクセストークンを設定
        タイムアウトを設定

        Args:
            url: アクセスするURL
            params: パラメーター

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session()

        headers = self._base_headers()
        ret = s.get(url, params=params, headers=headers, timeout=30)
        ret.raise_for_status()
        return ret

    def _post(
        self, url: str, payload: json = None, headers: dict = None
    ) -> requests.Response:
        """
        requests の get 用ラッパー

        ヘッダーにアクセストークンを設定
        タイムアウトを設定

        Args:
            url: アクセスするURL
            params: パラメーター

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session(method_whitelist=["POST"])

        ret = s.post(url, data=payload, headers=headers, timeout=30)
        ret.raise_for_status()
        return ret

    def get_refresh_token(self) -> None:
        """
        リフレッシュトークンを取得する

        """
        data = {"mailaddress": self.address, "password": self.passcode}
        # r_post = requests.post("https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
        url = f"{self.JQUANTS_API_BASE}/token/auth_user"
        ret = self._post(url, payload=json.dumps(data))
        refresh_token = ret.json()["refreshToken"]
        self._refresh_token = refresh_token

    def get_id_token(self) -> str:
        """
        IDトークンを取得する

        """
        if self._id_token_expire > pd.Timestamp.utcnow():
            return self._id_token

        url = f"{self.JQUANTS_API_BASE}/token/auth_refresh?refreshtoken={self._refresh_token}"
        ret = self._post(url)
        id_token = ret.json()["idToken"]
        self._id_token = id_token
        self._id_token_expire = pd.Timestamp.utcnow() + pd.Timedelta(23, unit="hour")
        return self._id_token

    def get_listed_info(
        self, code: str = "", date: str = "", light_plan=False
    ) -> pd.DataFrame:
        """
        銘柄一覧を取得

        Args:
            code: 銘柄コード (Optional)
            date: 基準となる日付 (Optional)

        Returns:
            pd.DataFrame: 銘柄一覧
        """
        url = f"{self.JQUANTS_API_BASE}/listed/info"
        params = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        ret = self._get(url, params)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["info"])

        cols = [
            "Date",
            "Code",
            #   "CompanyName",
            "CompanyNameEnglish",
            "Sector17Code",
            #   "Sector17CodeName",
            "Sector33Code",
            #   "Sector33CodeName",
            "ScaleCategory",
            "MarketCode",
            "MarketCodeName",
            "MarginCode",
            "MarginCodeName",
        ]
        if not light_plan:
            # standard以上のみで以下の列が取れる
            cols += ["MarginCode", "MarginCodeName"]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)

        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values("Code", inplace=True)

        return df[cols]

    def get_list_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
        light_plan: bool = True,
    ) -> pd.DataFrame:
        """
        全銘柄情報を日付範囲指定して取得（7日ごと）

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 銘柄情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="W-MON")
        counter = 1
        for s in dates:
            df = self.get_listed_info(date=s.strftime("%Y%m%d"), light_plan=light_plan)
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_prices_daily_quotes(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        premium_plan: bool = True,
    ) -> pd.DataFrame:
        """
        株価情報を取得

        Args:
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: 株価情報
        """
        url = f"{self.JQUANTS_API_BASE}/prices/daily_quotes"
        params = {}
        if code:
            params["code"] = code
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["daily_quotes"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["daily_quotes"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "Date",
            "Code",
            "Open",
            "High",
            "Low",
            "Close",
            "UpperLimit",
            "LowerLimit",
            "Volume",
            "TurnoverValue",
            "AdjustmentFactor",
            "AdjustmentOpen",
            "AdjustmentHigh",
            "AdjustmentLow",
            "AdjustmentClose",
            "AdjustmentVolume",
        ]
        if premium_plan:
            cols += [
                "MorningOpen",
                "MorningHigh",
                "MorningLow",
                "MorningClose",
                "MorningUpperLimit",
                "MorningLowerLimit",
                "MorningVolume",
                "MorningTurnoverValue",
                "MorningAdjustmentOpen",
                "MorningAdjustmentHigh",
                "MorningAdjustmentLow",
                "MorningAdjustmentClose",
                "MorningAdjustmentVolume",
                "AfternoonOpen",
                "AfternoonHigh",
                "AfternoonLow",
                "AfternoonClose",
                "AfternoonUpperLimit",
                "AfternoonLowerLimit",
                "AfternoonVolume",
                "AfternoonAdjustmentOpen",
                "AfternoonAdjustmentHigh",
                "AfternoonAdjustmentLow",
                "AfternoonAdjustmentClose",
                "AfternoonAdjustmentVolume",
            ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df.sort_values(["Date", "Code"]).reset_index(drop=True)
        return df[cols]

    def get_price_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
        premium_plan: bool = True,
    ) -> pd.DataFrame:
        """
        全銘柄の株価情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 株価情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_prices_daily_quotes(
                date_yyyymmdd=s.strftime("%Y%m%d"), premium_plan=premium_plan
            )
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_fins_statements(
        self, code: str = "", date_yyyymmdd: str = ""
    ) -> pd.DataFrame:
        """
        財務情報取得

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)

        Returns:
            pd.DataFrame: 財務情報
        """
        url = f"{self.JQUANTS_API_BASE}/fins/statements"
        params = {
            "code": code,
            "date": date_yyyymmdd,
        }
        ret = self._get(url, params)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["statements"])
        cols = [
            "DisclosedDate",
            "DisclosedTime",
            "LocalCode",
            "DisclosureNumber",
            "TypeOfDocument",
            "TypeOfCurrentPeriod",
            "CurrentPeriodStartDate",
            "CurrentPeriodEndDate",
            "CurrentFiscalYearStartDate",
            "CurrentFiscalYearEndDate",
            "NextFiscalYearStartDate",
            "NextFiscalYearEndDate",
            "NetSales",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
            "EarningsPerShare",
            "DilutedEarningsPerShare",
            "TotalAssets",
            "Equity",
            "EquityToAssetRatio",
            "BookValuePerShare",
            "CashFlowsFromOperatingActivities",
            "CashFlowsFromInvestingActivities",
            "CashFlowsFromFinancingActivities",
            "CashAndEquivalents",
            "ResultDividendPerShare1stQuarter",
            "ResultDividendPerShare2ndQuarter",
            "ResultDividendPerShare3rdQuarter",
            "ResultDividendPerShareFiscalYearEnd",
            "ResultDividendPerShareAnnual",
            "DistributionsPerUnit(REIT)",
            "ResultTotalDividendPaidAnnual",
            "ResultPayoutRatioAnnual",
            "ForecastDividendPerShare1stQuarter",
            "ForecastDividendPerShare2ndQuarter",
            "ForecastDividendPerShare3rdQuarter",
            "ForecastDividendPerShareFiscalYearEnd",
            "ForecastDividendPerShareAnnual",
            "ForecastDistributionsPerUnit(REIT)",
            "ForecastTotalDividendPaidAnnual",
            "ForecastPayoutRatioAnnual",
            "NextYearForecastDividendPerShare1stQuarter",
            "NextYearForecastDividendPerShare2ndQuarter",
            "NextYearForecastDividendPerShare3rdQuarter",
            "NextYearForecastDividendPerShareFiscalYearEnd",
            "NextYearForecastDividendPerShareAnnual",
            "NextYearForecastDistributionsPerUnit(REIT)",
            "NextYearForecastPayoutRatioAnnual",
            "ForecastNetSales2ndQuarter",
            "ForecastOperatingProfit2ndQuarter",
            "ForecastOrdinaryProfit2ndQuarter",
            "ForecastProfit2ndQuarter",
            "ForecastEarningsPerShare2ndQuarter",
            "NextYearForecastNetSales2ndQuarter",
            "NextYearForecastOperatingProfit2ndQuarter",
            "NextYearForecastOrdinaryProfit2ndQuarter",
            "NextYearForecastProfit2ndQuarter",
            "NextYearForecastEarningsPerShare2ndQuarter",
            "ForecastNetSales",
            "ForecastOperatingProfit",
            "ForecastOrdinaryProfit",
            "ForecastProfit",
            "ForecastEarningsPerShare",
            "NextYearForecastNetSales",
            "NextYearForecastOperatingProfit",
            "NextYearForecastOrdinaryProfit",
            "NextYearForecastProfit",
            "NextYearForecastEarningsPerShare",
            "MaterialChangesInSubsidiaries",
            "ChangesBasedOnRevisionsOfAccountingStandard",
            "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
            "ChangesInAccountingEstimates",
            "RetrospectiveRestatement",
            "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
            "NumberOfTreasuryStockAtTheEndOfFiscalYear",
            "AverageNumberOfShares",
            "NonConsolidatedNetSales",
            "NonConsolidatedOperatingProfit",
            "NonConsolidatedOrdinaryProfit",
            "NonConsolidatedProfit",
            "NonConsolidatedEarningsPerShare",
            "NonConsolidatedTotalAssets",
            "NonConsolidatedEquity",
            "NonConsolidatedEquityToAssetRatio",
            "NonConsolidatedBookValuePerShare",
            "ForecastNonConsolidatedNetSales2ndQuarter",
            "ForecastNonConsolidatedOperatingProfit2ndQuarter",
            "ForecastNonConsolidatedOrdinaryProfit2ndQuarter",
            "ForecastNonConsolidatedProfit2ndQuarter",
            "ForecastNonConsolidatedEarningsPerShare2ndQuarter",
            "NextYearForecastNonConsolidatedNetSales2ndQuarter",
            "NextYearForecastNonConsolidatedOperatingProfit2ndQuarter",
            "NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter",
            "NextYearForecastNonConsolidatedProfit2ndQuarter",
            "NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter",
            "ForecastNonConsolidatedNetSales",
            "ForecastNonConsolidatedOperatingProfit",
            "ForecastNonConsolidatedOrdinaryProfit",
            "ForecastNonConsolidatedProfit",
            "ForecastNonConsolidatedEarningsPerShare",
            "NextYearForecastNonConsolidatedNetSales",
            "NextYearForecastNonConsolidatedOperatingProfit",
            "NextYearForecastNonConsolidatedOrdinaryProfit",
            "NextYearForecastNonConsolidatedProfit",
            "NextYearForecastNonConsolidatedEarningsPerShare",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "DisclosedDate"] = pd.to_datetime(
            df["DisclosedDate"], format="%Y-%m-%d"
        )
        df.loc[:, "DisclosureNumber"] = pd.to_numeric(
            df["DisclosureNumber"], errors="coerce"
        )
        df.loc[:, "CurrentPeriodStartDate"] = pd.to_datetime(
            df["CurrentPeriodStartDate"], format="%Y-%m-%d"
        )
        df.loc[:, "CurrentPeriodEndDate"] = pd.to_datetime(
            df["CurrentPeriodEndDate"], format="%Y-%m-%d"
        )
        df.loc[:, "CurrentFiscalYearStartDate"] = pd.to_datetime(
            df["CurrentFiscalYearStartDate"], format="%Y-%m-%d"
        )
        df.loc[:, "CurrentFiscalYearEndDate"] = pd.to_datetime(
            df["CurrentFiscalYearEndDate"], format="%Y-%m-%d"
        )
        df.loc[:, "NextFiscalYearStartDate"] = pd.to_datetime(
            df["NextFiscalYearStartDate"], format="%Y-%m-%d"
        )
        df.loc[:, "NextFiscalYearEndDate"] = pd.to_datetime(
            df["NextFiscalYearEndDate"], format="%Y-%m-%d"
        )
        df.sort_values(["DisclosedDate", "DisclosedTime"], inplace=True)
        return df[cols]

    def get_fins_announcement(self) -> pd.DataFrame:
        """
        翌日の決算発表情報の取得

        Args:
            N/A

        Returns:
            pd.DataFrame: 翌日決算発表情報
        """
        url = f"{self.JQUANTS_API_BASE}/fins/announcement"
        ret = self._get(url)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["announcement"])
        cols = [
            "Date",
            "Code",
            "CompanyName",
            "FiscalYear",
            "SectorName",
            "FiscalQuarter",
            "Section",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]

    def get_statements_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 財務情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            # fetch data via API or cache file
            cache_file = f"fins_statements_{s.strftime('%Y%m%d')}.csv.gz"
            if (cache_dir != "") and os.path.isfile(
                f"{cache_dir}/{s.strftime('%Y')}/{cache_file}"
            ):
                df = pd.read_csv(f"{cache_dir}/{s.strftime('%Y')}/{cache_file}")
            else:
                df = self.get_fins_statements(date_yyyymmdd=s.strftime("%Y%m%d"))
                if cache_dir != "":
                    # create year directory
                    os.makedirs(f"{cache_dir}/{s.strftime('%Y')}", exist_ok=True)
                    # write cache file
                    df.to_csv(
                        f"{cache_dir}/{s.strftime('%Y')}/{cache_file}", index=False
                    )

            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_options(
        self,
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        オプション情報を取得

        Args:
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: オプション情報
        """
        url = f"{self.JQUANTS_API_BASE}/option/index_option"
        params = {}
        params["date"] = date_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["index_option"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["daily_quotes"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "Date",
            "Code",
            "WholeDayOpen",
            "WholeDayHigh",
            "WholeDayLow",
            "WholeDayClose",
            "NightSessionOpen",
            "NightSessionHigh",
            "NightSessionLow",
            "NightSessionClose",
            "DaySessionOpen",
            "DaySessionHigh",
            "DaySessionLow",
            "DaySessionClose",
            "Volume",
            "OpenInterest",
            "TurnoverValue",
            "ContractMonth",
            "StrikePrice",
            "Volume(OnlyAuction)",
            "EmergencyMarginTriggerDivision",
            "PutCallDivision",
            "LastTradingDay",
            "SpecialQuotationDay",
            "SettlementPrice",
            "TheoreticalPrice",
            "BaseVolatility",
            "UnderlyingPrice",
            "ImpliedVolatility",
            "InterestRate",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df.sort_values(["Date", "Code"]).reset_index(drop=True)
        return df[cols]

    def get_options_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        全銘柄のオプション情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: オプション情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_options(date_yyyymmdd=s.strftime("%Y%m%d"))
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_markets(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        投資部門別売買状況を取得

        Args:
            from_yyyymmdd: fromの指定(全期間取得の場合は指定しない)
            to_yyyymmdd  : toの指定(全期間取得の場合は指定しない)
        Returns:
            pd.DataFrame: 投資部門別売買状況の情報
        """
        url = f"{self.JQUANTS_API_BASE}/markets/trades_spec"
        params = {}
        if len(from_yyyymmdd) > 0:
            params["from"] = from_yyyymmdd
            params["to"] = to_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["trades_spec"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["trades_spec"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "PublishedDate",
            "StartDate",
            "EndDate",
            "Section",
            "ProprietarySales",
            "ProprietaryPurchases",
            "ProprietaryTotal",
            "ProprietaryBalance",
            "BrokerageSales",
            "BrokeragePurchases",
            "BrokerageTotal",
            "BrokerageBalance",
            "TotalSales",
            "TotalPurchases",
            "TotalTotal",
            "TotalBalance",
            "IndividualsSales",
            "IndividualsPurchases",
            "IndividualsTotal",
            "IndividualsBalance",
            "ForeignersSales",
            "ForeignersPurchases",
            "ForeignersTotal",
            "ForeignersBalance",
            "SecuritiesCosSales",
            "SecuritiesCosPurchases",
            "SecuritiesCosTotal",
            "SecuritiesCosBalance",
            "InvestmentTrustsSales",
            "InvestmentTrustsPurchases",
            "InvestmentTrustsTotal",
            "InvestmentTrustsBalance",
            "BusinessCosSales",
            "BusinessCosPurchases",
            "BusinessCosTotal",
            "BusinessCosBalance",
            "OtherCosSales",
            "OtherCosPurchases",
            "OtherCosTotal",
            "OtherCosBalance",
            "InsuranceCosSales",
            "InsuranceCosPurchases",
            "InsuranceCosTotal",
            "InsuranceCosBalance",
            "CityBKsRegionalBKsEtcSales",
            "CityBKsRegionalBKsEtcPurchases",
            "CityBKsRegionalBKsEtcTotal",
            "CityBKsRegionalBKsEtcBalance",
            "TrustBanksSales",
            "TrustBanksPurchases",
            "TrustBanksTotal",
            "TrustBanksBalance",
            "OtherFinancialInstitutionsSales",
            "OtherFinancialInstitutionsPurchases",
            "OtherFinancialInstitutionsTotal",
            "OtherFinancialInstitutionsBalance",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "PublishedDate"] = pd.to_datetime(
            df["PublishedDate"], format="%Y-%m-%d"
        )
        df.loc[:, "StartDate"] = pd.to_datetime(df["StartDate"], format="%Y-%m-%d")
        df.loc[:, "EndDate"] = pd.to_datetime(df["EndDate"], format="%Y-%m-%d")
        df = df.sort_values(["PublishedDate"]).reset_index(drop=True)
        return df[cols]

    def get_weekly_interest(
        self,
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        信用取引週末残高を取得
        Args:
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: 信用取引週末残高情報
        """
        url = f"{self.JQUANTS_API_BASE}/markets/weekly_margin_interest"
        params = {}
        params["date"] = date_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["weekly_margin_interest"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["weekly_margin_interest"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "Date",
            "Code",
            "ShortMarginTradeVolume",
            "LongMarginTradeVolume",
            "ShortNegotiableMarginTradeVolume",
            "LongNegotiableMarginTradeVolume",
            "ShortStandardizedMarginTradeVolume",
            "LongStandardizedMarginTradeVolume",
            "IssueType",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df.sort_values(["Date", "Code"]).reset_index(drop=True)
        return df[cols]

    def get_weekly_interest_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        全銘柄の信用取引週末残高情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 信用取引週末残高情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_weekly_interest(date_yyyymmdd=s.strftime("%Y%m%d"))
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_short_selling(
        self,
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        業種別空売り比率を取得
        Args:
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: 業種別空売り比率情報
        """
        url = f"{self.JQUANTS_API_BASE}/markets/short_selling"
        params = {}
        params["date"] = date_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["short_selling"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["short_selling"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "Date",
            "Sector33Code",
            "SellingExcludingShortSellingTurnoverValue",
            "ShortSellingWithRestrictionsTurnoverValue",
            "ShortSellingWithoutRestrictionsTurnoverValue",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df.sort_values(["Date", "Sector33Code"]).reset_index(drop=True)
        return df[cols]

    def get_short_selling_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        全銘柄の業種別空売り比率情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 業種別空売り比率情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_short_selling(date_yyyymmdd=s.strftime("%Y%m%d"))
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_markets_breakdown(
        self,
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        売買内訳データを取得
        Args:
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: 売買内訳データ
        """
        url = f"{self.JQUANTS_API_BASE}/markets/breakdown"
        params = {}
        params["date"] = date_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["breakdown"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["breakdown"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "Date",
            "Code",
            "LongSellValue",
            "ShortSellWithoutMarginValue",
            "MarginSellNewValue",
            "MarginSellCloseValue",
            "LongBuyValue",
            "MarginBuyNewValue",
            "MarginBuyCloseValue",
            "LongSellVolume",
            "ShortSellWithoutMarginVolume",
            "MarginSellNewVolume",
            "MarginSellCloseVolume",
            "LongBuyVolume",
            "MarginBuyNewVolume",
            "MarginBuyCloseVolume",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df.sort_values(["Date", "Code"]).reset_index(drop=True)
        return df[cols]

    def get_markets_breakdown_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        全銘柄の売買内訳データを日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 売買内訳データ情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_markets_breakdown(date_yyyymmdd=s.strftime("%Y%m%d"))
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)

    def get_topix(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        TOPIX指数データを日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: TOPIX指数データ
        """
        url = f"{self.JQUANTS_API_BASE}/indices/topix"
        params = {}
        params["from"] = start_dt.strftime("%Y%m%d")
        params["to"] = end_dt.strftime("%Y%m%d")
        ret = self._get(url, params)
        data = ret.json()["topix"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["topix"]

        df = pd.DataFrame.from_dict(data)
        cols = ["Date", "Open", "High", "Low", "Close"]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df.sort_values("Date").reset_index(drop=True)
        return df[cols]

    def get_dividend(
        self,
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        配当金情報を取得
        Args:
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: 配当金情報
        """
        url = f"{self.JQUANTS_API_BASE}/fins/dividend"
        params = {}
        params["date"] = date_yyyymmdd
        ret = self._get(url, params)
        data = ret.json()["dividend"]

        # https://jpx.gitbook.io/j-quants-ja/api-reference#resuponsunopjingunitsuite
        # 大容量データが返却された場合の再検索
        # データ量により複数ページ取得できる場合があるため、pagination_keyが含まれる限り、再検索を実施
        while "pagination_key" in ret.json():
            params["pagination_key"] = ret.json()["pagination_key"]
            ret = self._get(url, params)
            data += ret.json()["breakdown"]

        df = pd.DataFrame.from_dict(data)
        cols = [
            "AnnouncementDate",
            "AnnouncementTime",
            "Code",
            "ReferenceNumber",
            "StatusCode",
            "BoardMeetingDate",
            "InterimFinalCode",
            "ForecastResultCode",
            "InterimFinalTerm",
            "GrossDividendRate",
            "RecordDate",
            "ExDate",
            "ActualRecordDate",
            "PayableDate",
            "CAReferenceNumber",
            "DistributionAmount",
            "RetainedEarnings",
            "DeemedDividend",
            "DeemedCapitalGains",
            "NetAssetDecreaseRatio",
            "CommemorativeSpecialCode",
            "CommemorativeDividendRate",
            "SpecialDividendRate",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "AnnouncementDate"] = pd.to_datetime(
            df["AnnouncementDate"], format="%Y-%m-%d"
        )
        df = df.sort_values(["AnnouncementDate", "Code"]).reset_index(drop=True)
        return df[cols]

    def get_dividend_range(
        self,
        start_dt: datetime = datetime(2008, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        配当金情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 配当金情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_dividend(date_yyyymmdd=s.strftime("%Y%m%d"))
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        if len(buff) == 0:
            return None
        if len(buff) == 1:
            df_return = buff[0]
            if len(df_return) == 0:
                return None
            return df_return
        return pd.concat(buff).reset_index(drop=True)
