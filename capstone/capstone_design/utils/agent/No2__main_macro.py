import os
import logging
import requests
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from scipy.stats import pearsonr
from pandas_datareader import data as web
from pykrx import stock

# 환경변수에서 API 키 로드
from dotenv import load_dotenv
load_dotenv()
ECOS_KEY = os.getenv("ECOS_API_KEY")

# 한 번만 불러두고 재사용
try:
    _KOSPI_TICKERS = set(stock.get_market_ticker_list("KOSPI"))
except Exception:
    _KOSPI_TICKERS = set()

# ── 공통 헬퍼 ─────────────────────────────────────────────────────────────────

def get_log_return(
    df: pd.DataFrame,
    value_col: str,
    date_col:  str = 'Date',
    return_col:str = 'Log_Return'
) -> pd.DataFrame:
    """지정한 value_col의 로그 수익률을 계산해서 return_col에 담아 리턴"""
    df = df[[date_col, value_col]].dropna().copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df[df[value_col] > 0]
    df = df.sort_values(date_col)
    df[return_col] = np.log(df[value_col] / df[value_col].shift(1))
    return df[[date_col, return_col]].dropna()

def calculate_correlation(
    df1: pd.DataFrame, col1: str,
    df2: pd.DataFrame, col2: str,
    min_obs: int = 10
) -> tuple[float, float, bool]:
    """
    두 시리즈의 피어슨 상관·p-value를 구하고,
    유의미성(p<0.05) 여부를 반환
    """
    merged = pd.merge(df1, df2, on='Date').dropna()
    if len(merged) < min_obs:
        return None, None, False
    r, p = pearsonr(merged[col1], merged[col2])
    return round(r,4), round(p,4), (p < 0.05)

# ── 지표별 데이터 수집 ───────────────────────────────────────────────────────

def _fetch_fred(series: str, start: datetime, end: datetime, col: str) -> pd.DataFrame:
    """FRED API로부터 시계열을 가져와 Date, col 컬럼을 가진 df로 반환"""
    df = (web.DataReader(series, "fred", start, end)
            .dropna()
            .reset_index()
            .rename(columns={"DATE":"Date", series:col}))
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def _fetch_ecos(
    stat_code: str,
    start: datetime,
    end:   datetime,
    col:   str,
    mkt_code: str = ""
) -> pd.DataFrame:
    """ECOS API로부터 시계열을 가져와 Date, col 컬럼을 가진 df로 반환"""
    s, e = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    url = (
      f"http://ecos.bok.or.kr/api/StatisticSearch/{ECOS_KEY}/json/kr/"
      f"1/10000/{stat_code}/D/{s}/{e}/{mkt_code}"
    )
    data = requests.get(url).json().get("StatisticSearch", {})
    rows = data.get("row", [])
    df = pd.DataFrame(rows)[["TIME","DATA_VALUE"]]
    df.columns = ["Date", col]
    df["Date"] = pd.to_datetime(df["Date"])
    df[col]   = pd.to_numeric(df[col], errors="coerce")
    return df

def _fetch_index(code: str, start_s: str, end_s: str) -> pd.DataFrame:
    """KOSPI/KOSDAQ 지수 price를 Date, Index 컬럼으로 반환"""
    idx = "1001" if code in _KOSPI_TICKERS else "2001"
    df = (stock.get_index_ohlcv_by_date(start_s, end_s, idx)
            .reset_index()[["날짜","종가"]]
            .rename(columns={"날짜":"Date","종가":"Index"}))
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# ── 메인 리팩토링 함수 ───────────────────────────────────────────────────────

def get_macro_series(
    code: str,
    start_date: datetime,
    end_date:   datetime,
    p_threshold: float = 0.05
) -> pd.DataFrame:
    """
    종목 code의 주가(log-return)와
    여러 거시지표(log-return) 간의 상관관계 DataFrame을 반환.

    결과 컬럼: indicator, correlation, p_value, significance(bool)
    """
    # 1) 입력 검증
    if not isinstance(code, str):
        raise TypeError("code는 문자열이어야 합니다.")
    if not all(isinstance(d, datetime) for d in (start_date, end_date)):
        raise TypeError("start_date, end_date는 datetime이어야 합니다.")

    # 2) 주가 수익률
    s_fmt, e_fmt = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    df_price = (stock.get_market_ohlcv_by_date(s_fmt, e_fmt, code)
                  .reset_index()[["날짜","종가"]]
                  .rename(columns={"날짜":"Date","종가":"Price"}))
    df_price["Date"] = pd.to_datetime(df_price["Date"])
    stock_ret = get_log_return(df_price, "Price", "Date", "Stock_Return")

    # 3) 지표 정의 (이름, 수집 함수)
    indicators = [
        ("WTI유가",    lambda: _fetch_fred("DCOILWTICO", start_date, end_date, "WTI")),
        ("KOSPI/KOSDAQ", lambda: _fetch_index(code, s_fmt, e_fmt)),
        ("국고채10년", lambda: _fetch_ecos("817Y002", start_date, end_date, "Bond")),
        ("USD/KRW",    lambda: _fetch_ecos("731Y001", start_date, end_date, "USD")),
        ("외국인매매", lambda: _fetch_ecos(
                            "802Y001",
                            start_date,
                            end_date,
                            "Foreign",
                            mkt_code="0030000" if code in _KOSPI_TICKERS else "0113000"))
    ]

    results = []
    for name, fetch_fn in indicators:
        try:
            df = fetch_fn()
            # 마지막 컬럼 이름을 자동으로 찾아서 로그 수익률 계산
            val_col = df.columns.difference(["Date"])[0]
            ret_df = get_log_return(df, val_col, "Date", "X_Return")
            r, p, _sig = calculate_correlation(ret_df, "X_Return", stock_ret, "Stock_Return")
            results.append({
                "indicator":   name,
                "correlation": r,
                "p_value":     p,
                "significance": bool(p is not None and p < p_threshold)
            })
        except Exception as e:
            logging.warning(f"[get_macro_series] '{name}' 수집 실패: {e}")

    return pd.DataFrame(
        results,
        columns=["indicator","correlation","p_value","significance"]
    )


if __name__ == "__main__":
    # ——— 기본 실행 파라미터 ———
    code = "329180"
    # 오늘 기준 180일 전부터 오늘까지
    end_date   = datetime(2024,5,31)
    start_date = datetime(2024,1,1)

    # ——— 실행 ———
    df_macro = get_macro_series(code, start_date, end_date)
    print(f"=== 거시지표 상관관계 ({code}) ===")
    print(df_macro.to_string(index=False))

