import os
import pandas as pd
import glob
from datetime import datetime
from pykrx import stock
from industry_map import industry_map

start_date = "2024-01-01"
end_date   = "2024-05-31"

# ✅ 재무제표 연도 자동 생성 유틸 함수
def compute_years(start_date: str, num_years: int = 3):
    base_year = datetime.strptime(start_date, "%Y-%m-%d").year
    return [str(base_year - i) for i in reversed(range(num_years))]

# ✅ 주가 수집
def fetch_and_save_stock_price(name: str, code: str, start_date: str, end_date: str) -> None:
    print(f"[주가 수집] {name} ({code}) 기간: {start_date}~{end_date}")
    df = stock.get_market_ohlcv_by_date(start_date, end_date, code).reset_index()
    df['등락'] = df['종가'].diff()
    df['5일_이동평균'] = df['종가'].rolling(5).mean()
    df['20일_이동평균'] = df['종가'].rolling(20).mean()
    df['60일_이동평균'] = df['종가'].rolling(60).mean()
    df['120일_이동평균'] = df['종가'].rolling(120).mean()

# ✅ 대표기업 펀더멘탈 시계열 수집
def fetch_fundamental_timeseries(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    print(f"[대표기업 펀더멘탈]  ({code}) 기간: {start_date}~{end_date}")
    df = stock.get_market_fundamental(start_date, end_date, code, freq="m")
    if df.empty:
        print(f"[{code}] 기간 내 펀더멘탈 데이터 없음")
        return pd.DataFrame()
    df = df[['BPS','PER','PBR','EPS','DIV','DPS']].copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    return df

# ✅ 관련기업 펀더멘탈 시계열 수집
def fetch_related_fundamental_timeseries(maincorp_name: str, start_date: str, end_date: str):
    
    if maincorp_name not in industry_map:
        raise ValueError(f"[ERROR] '{maincorp_name}' not in industry_map")

    related = industry_map[maincorp_name].get("관련기업", [])
    results = {}
    for name, code in related:
        print(f"[펀더멘탈 수집] {name} 기간: {start_date}~{end_date}")
        df = stock.get_market_fundamental(start_date, end_date, code, freq="m")
        if df.empty:
            print(f"[SKIP] {name} ({code}) - No data")
            continue
        df = df[['BPS','PER','PBR','EPS','DIV','DPS']].copy()
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        results[name] = df
    return results

# ✅ 시가총액 / 수급
def fetch_market_cap(code: str, start_date: str, end_date: str, freq: str = "m") -> pd.DataFrame:
    df = stock.get_market_cap(start_date, end_date, code, freq=freq)
    df.index = pd.to_datetime(df.index)
    return df[['시가총액', '거래량', '거래대금', '상장주식수']]

def fetch_investor_trading(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = stock.get_market_trading_volume_by_investor(start_date, end_date, code)
    return df

# ✅ 재무제표 기반 심화 재무비율 분석 (CSV 기반)
def analyze_financial_ratios_csv(stock_code, corp_name, base_dir, years):
    base = os.path.join(base_dir, corp_name)
    print(f"[DEBUG] looking in {base}")
    if not os.path.isdir(base):
        print(f"[ERROR] 폴더가 없습니다: {base}")
        return pd.DataFrame()

    bs_path = os.path.join(base, f"{corp_name}_재무상태표.csv")
    pl_path1 = os.path.join(base, f"{corp_name}_포괄손익계산서.csv")
    pl_path2 = os.path.join(base, f"{corp_name}_손익계산서.csv")
    pl_path = pl_path1 if os.path.exists(pl_path1) else pl_path2 if os.path.exists(pl_path2) else None
    cf_path = glob.glob(os.path.join(base, "*현금흐름표.csv"))[0] if glob.glob(os.path.join(base, "*현금흐름표.csv")) else None

    if not (pl_path and cf_path):
        print("[ERROR] 일부 파일 누락")
        return pd.DataFrame()

    bs_df = pd.read_csv(bs_path, index_col=0)
    pl_df = pd.read_csv(pl_path, index_col=0)
    cf_df = pd.read_csv(cf_path, index_col=0)

    def gv(df, key, y):
        try:
            return float(str(df.at[key, y]).replace(",", "").strip())
        except:
            return None

    def safe_div(a, b): return round(a/b, 4) if a and b else None
    def safe_add(a, b): return a+b if a is not None and b is not None else None

    result = {}
    for year in years:
        end = f"{year}1231"
        pl_y = f"{year}0101-{year}1231"
        try:
            net_income = gv(pl_df, "ifrs-full_ProfitLoss", pl_y)
            자산 = gv(bs_df, "ifrs-full_Assets", end)
            자본 = gv(bs_df, "ifrs-full_Equity", end)
            부채 = gv(bs_df, "ifrs-full_Liabilities", end)
            유동자산 = gv(bs_df, "ifrs-full_CurrentAssets", end)
            유동부채 = gv(bs_df, "ifrs-full_CurrentLiabilities", end)
            매출 = gv(pl_df, "ifrs-full_Revenue", pl_y)
            영업이익 = gv(pl_df, "dart_OperatingIncomeLoss", pl_y)
            연구개발비 = gv(pl_df, "ifrs-full_ResearchAndDevelopmentExpense", pl_y)
            감가상각 = gv(cf_df, "ifrs-full_AdjustmentsForDepreciationExpense", pl_y)
            영업활동현금흐름 = gv(cf_df, "ifrs-full_CashFlowsFromUsedInOperatingActivities", pl_y)
            투자활동현금흐름 = gv(cf_df, "ifrs-full_CashFlowsFromUsedInInvestingActivities", pl_y)
            재무활동현금흐름 = gv(cf_df, "ifrs-full_CashFlowsFromUsedInFinancingActivities", pl_y)
            총현금흐름 = safe_add(safe_add(영업활동현금흐름, 투자활동현금흐름), 재무활동현금흐름)

            result[year] = {
                "자기자본비율": safe_div(자본, 자산),
                "부채비율": safe_div(부채, 자본),
                "유동비율": safe_div(유동자산, 유동부채),
                "ROE": safe_div(net_income, 자본),
                "ROA": safe_div(net_income, 자산),
                "영업이익률": safe_div(영업이익, 매출),
                "EBITDA": safe_add(영업이익, 감가상각),
                "연구개발비비중": safe_div(연구개발비, 매출),
                "FCF": safe_add(영업활동현금흐름, -투자활동현금흐름),
                "투자활동현금흐름": 투자활동현금흐름,
                "재무활동현금흐름": 재무활동현금흐름,
                "총현금흐름": 총현금흐름
            }

        except Exception as e:
            print(f"[{corp_name}] {year} 계산 오류: {e}")

    df = pd.DataFrame(result).T
    print(f"[CSV 기반 재무비율 계산 완료] {corp_name}")
    return df

# ✅ 전체 통합 함수
def build_integrated_dataframe(stock_code, corp_name, base_dir, start_date, end_date):
    years = compute_years(start_date)
    
    df_fin = analyze_financial_ratios_csv(stock_code, corp_name, base_dir, years)
    df_fund = fetch_fundamental_timeseries(stock_code, start_date, end_date)
    df_cap = fetch_market_cap(stock_code, start_date, end_date)
    df_invest = fetch_investor_trading(stock_code, start_date, end_date)

    return {
        "재무제표": df_fin,
        "펀더멘탈": df_fund,
        "시총": df_cap,
        "수급": df_invest
    }

# ✅ 테스트용
if __name__ == "__main__":
    base_dir = r"C:\Users\윤지언\Desktop\PKNU\대학교 4학년\캡디workspace\리포팅 신뢰성 검증\재무제표"
    
    print(build_integrated_dataframe("329180", "HD현대중공업", base_dir, start_date, end_date))
