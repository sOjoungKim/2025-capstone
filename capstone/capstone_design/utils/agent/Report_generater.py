import os
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.chains.llm import LLMChain
from pathlib import Path

from No1_Basic_company_info import get_basic_info
from No2__main_macro import get_macro_series
from No4_Sentimental_Analysis import apply_sentiment_to_articles
from industry_map import industry_map
from No3__main_financial import (
    fetch_and_save_stock_price, analyze_financial_ratios_csv,
    fetch_fundamental_timeseries, fetch_market_cap,
    fetch_investor_trading, fetch_related_fundamental_timeseries
)

# 환경변수 로드
load_dotenv()

# 전역 파라미터
start_date = "2025-01-01"
end_date   = "2025-05-31"
DEFAULT_PRICE_START = "20250101"
DEFAULT_PRICE_END   = "20250531"
FUND_LOOKBACK_DAYS  = 180
MACRO_LOOKBACK_DAYS = 180

base_year = datetime.strptime(start_date, "%Y-%m-%d").year
YEARS = [str(base_year - i) for i in reversed(range(1,4))]  # ['2023', '2024', '2025']

# 공통 경로
BASE_DIR = Path(__file__).resolve().parent.parent

XBRL_DIR    = BASE_DIR / "agent" / "재무재표"
JSON_DIR    = BASE_DIR / "agent" / "json"
OUTPUT_ROOT = BASE_DIR / "agent" / "md"

# 뉴스 요약 체인 --------------
extract_llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0.2)
news_extraction_prompt = PromptTemplate(
    input_variables=["raw_news"],
    template="""
다음은 특정 산업 관련 뉴스 원문입니다. 여기서 투자 결정의견에 사용할 수 있는 핵심 내용을 아래 조건에 맞게 추출하세요:
- 7~8문장으로 요약
- 구체적인 주장과 이유를 포함
- 수치나 고유명사는 가능한 유지
- 논리적 구조 (무엇이, 왜, 어떻게)를 명확히 표현
뉴스 원문:
{raw_news}
핵심 요약:
"""
)
extract_news_chain = LLMChain(llm=extract_llm, prompt=news_extraction_prompt)
from tiktoken import get_encoding

def summarize_news_from_json(json_news_path: str, max_tokens: int = 8000) -> str:
    import unicodedata
    import json

    enc = get_encoding("cl100k_base")  # gpt-3.5-turbo 기준 tokenizer

    with open(json_news_path, encoding="utf-8") as f:
        data = json.load(f)

    news_list = data.get("뉴스공시", [])
    
    selected_news = []
    token_count = 0

    for item in news_list:
        content = item.get("본문", "")
        tokens = enc.encode(content)
        if token_count + len(tokens) > max_tokens:
            break
        selected_news.append(content)
        token_count += len(tokens)

    joined_news = "\n".join(selected_news)
    summary = extract_news_chain.run({"raw_news": joined_news})
    return summary.strip()

# 뉴스 요약 체인 끝 -------------

# 관련기업 펀더멘탈 요약용 ---------------------------------------------
def summarize_related_fundamentals(df_rel_dict):
    dfs = [df for firm, df in df_rel_dict.items()]
    if dfs:
        df_all = pd.concat(dfs)
        return df_all.describe().to_dict()
    else:
        return {}

# 데이터 통합 수집 파이프라인
def generate_data_summary(
    stock_code: str, 
    xbrl_base_dir: str, 
    json_news_path: str,
    start_date: str,
    end_date: str,
    fund_lookback_days: int = 180,
    macro_lookback_days: int = 180
) -> dict:

    # 연도 리스트 생성
    base_year = datetime.strptime(start_date, "%Y-%m-%d").year
    years = [str(base_year - i) for i in reversed(range(3))]

    info = get_basic_info(stock_code)
    corp_name = info["기업명"]
    related = industry_map[corp_name].get("관련기업", [])

    fetch_and_save_stock_price(corp_name, stock_code, start_date, end_date)
    for rel_name, rel_code in related:
        fetch_and_save_stock_price(rel_name, rel_code, start_date, end_date)

    df_fin = analyze_financial_ratios_csv(stock_code, corp_name, xbrl_base_dir, years)
    df_fund = fetch_fundamental_timeseries(stock_code, start_date, end_date)
    df_rel  = fetch_related_fundamental_timeseries(corp_name, start_date, end_date)
    df_cap  = fetch_market_cap(stock_code, start_date, end_date)
    df_invest = fetch_investor_trading(stock_code, start_date, end_date)

    with open(json_news_path, encoding="utf-8") as fp:
        raw = json.load(fp)

    sentiments = {}
    for key, field in [("뉴스공시", "본문"), ("업종뉴스", "본문")]:
        articles = raw.get(key, [])
        items = apply_sentiment_to_articles(articles, field, key)
        sentiments[key] = {"items": items}

    end_macro_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_macro_dt = end_macro_dt - timedelta(days=macro_lookback_days)
    df_macro = get_macro_series(stock_code, start_macro_dt, end_macro_dt)

    return {
        "기업기본정보": info,
        "재무제표": df_fin,
        "펀더멘탈": df_fund,
        "관련기업펀더멘탈": df_rel,
        "시총": df_cap,
        "수급": df_invest,
        "감성": sentiments,
        "거시": df_macro
    }

# LLM 입력 최적화 파이프라인
def build_llm_input(summary: dict) -> dict:
    corp_name = summary["기업기본정보"]["기업명"]

    financial_str = summary["재무제표"].to_string()
    fund_str = summary["펀더멘탈"].to_string()
    cap_str = summary["시총"].to_string()
    net_buy = summary["수급"]["순매수"].sum()

    # 관련기업 펀더멘탈 요약
    df_rel_list = [df for firm, df in summary["관련기업펀더멘탈"].items()]
    if df_rel_list:
        df_rel_concat = pd.concat(df_rel_list)
        industry_str = df_rel_concat.to_string()
    else:
        industry_str = "관련기업 데이터 없음"

    # 감성분석 분리
    industry_sentiment = summarize_sentiment(summary["감성"]["업종뉴스"]["items"])
    company_sentiment  = summarize_sentiment(summary["감성"]["뉴스공시"]["items"])

    return {
        "corp_name": corp_name,
        "macro": summary["거시"].to_string(),
        "industry": industry_str,
        "financial": financial_str,
        "fundamental": fund_str,
        "marketcap": cap_str,
        "investor": f"최근 순매수: {net_buy:,}주",
        "industry_sentiment": str(industry_sentiment),
        "company_sentiment": str(company_sentiment)
    }

# 감성요약 보조함수 유지 ---------------------------------------------
def summarize_sentiment(items):
    pos = sum(1 for r in items if r["감정"] == "긍정")
    neg = sum(1 for r in items if r["감정"] == "부정")
    neu = len(items) - pos - neg
    return {"긍정": pos, "부정": neg, "중립": neu, "총건수": len(items)}


# LLM 판단 파트 ---------------------------------------------
def generate_llm_report(summary: dict, json_dir: str) -> str:
    llm = ChatOpenAI(temperature=0, model="gpt-4o")
    news_argument = summarize_news_from_json(json_dir)

    print("\n====== 뉴스 요약 (news_argument) ======")
    print(news_argument)
    print("======================================\n")

    prompt = PromptTemplate(
        input_variables=["corp_name", "macro", "industry", "financial", "fundamental", "marketcap", "investor", "industry_sentiment", "company_sentiment","news_argument"],
        template="""
당신은 퀀트 애널리스트입니다. 아래 기업 정보를 바탕으로 다음 형식의 마크다운 문법으로 증권 리포트를 작성해주세요.

## 주의사항 (작성 규칙)
- 반드시 수치 기반 해석 → 논리 분석 → 최종 의견 도출 순서로 작성하십시오.
- 모든 분석에서 풍부하게 상세히 작성하십시오.
- 제공된 수치에 대해 다양한 가능성을 검토하며 신중하게 해석하십시오.
- 과거의 흐름과 최근 변화를 함께 고려하여 시계열적 해석을 수행하십시오.
- 경제분석, 산업분석, 기업분석 각각의 말미에 반드시 [긍정/부정/중립]으로 표시하십시오.
- 최종 종합 판단에서는 [적극매수/매수/중립/매도/매우위험] 중 하나로 최종 투자 의견을 표기하십시오.
- 최종 투자 의견에 대한 비율 추정치도 매수/보유/매도 퍼센트로 함께 제시하십시오.
- 증권사 애널리스트 관점에서 전문적이고 근거 중심으로 판단하십시오.
- 모든 판단의 근거는 풍부히 서술하십시오.
- 기업 최근 뉴스:
    "{news_argument}"
    위 내용을 참고하고, 반드시 내용을 인용하여 서술할 것

## 매도판단 보조 기준 (적극 반영)
- ROE 최근 3년 평균 5% 미만일 경우 부정적으로 평가하십시오.
- 부채비율이 200%를 초과하는 경우 재무위험 신호로 간주하십시오.
- 유동비율이 80% 미만인 경우 단기 지급능력 위험으로 간주하십시오.
- 영업이익률이 5% 미만인 경우 수익성 약화로 간주하십시오.
- FCF가 최근 3년간 지속 적자인 경우 현금흐름 위험으로 간주하십시오.
- PER이 업종평균 대비 50% 초과하거나 적자인 경우 고평가 위험으로 간주하십시오.
- 감성분석에서 부정 비율이 50%를 초과할 경우 부정적 시장심리로 간주하십시오.
- 최근 순매수가 기관·외국인 순매도일 경우 수급 부담 요인으로 고려하십시오.
- 보유 의견은 긍정과 부정 신호가 혼재할 경우에만 내십시오.
- 뉴스 데이터는 서술에 적극 활용하되, 의견에는 비판적으로 수용하십시오.

# {corp_name} 리포트
## 창의적인 소제목(리포트 요약)

## 1. 거시경제 분석:
{macro}

## 2. 산업분석:
{industry}
산업 감성 요약: {industry_sentiment}

## 3. 기업분석:
재무: {financial}
펀더멘탈: {fundamental}
시가총액 및 거래대금: {marketcap}
수급: {investor}
기업 감성 요약: {company_sentiment}

## 4. 최종 종합 판단


"""
    )

    llm_input = build_llm_input(summary)
    llm_input["news_argument"] = news_argument
    result = LLMChain(prompt=prompt, llm=llm).run(llm_input)
    return result


# 전체 파이프라인 --------------------------------------------
def full_run(corp_name: str, xbrl_path: str, json_dir: str, start_date: str, end_date: str):
    stock_code = industry_map[corp_name]["종목코드"]
    
    json_filename = f"{corp_name}.json"
    json_path = os.path.join(json_dir, json_filename)
    
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"{corp_name}에 대한 json 뉴스파일이 존재하지 않습니다: {json_path}")
    
    summary = generate_data_summary(stock_code, xbrl_path, json_path, start_date, end_date)
    report = generate_llm_report(summary, json_path)
    return report

# ------------------------------------------------------------------------------------------

# 분석 대상 기업 리스트
targets = [
    # "성우하이텍",
    # "고려제강",
    # "셀바스헬스케어",
    # "메카로",
    # "한선엔지니어링",
    # "데브시스터즈",
    # "동성화인텍",
    "HD현대중공업",
    "한화에어로스페이스",
    "한미반도체",
    "현대자동차",
    "에코프로"
]

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

for corp_name in targets:
    try:
        print(f"▶ {corp_name} LLM 리포트 생성 시작")

        # 날짜 파라미터를 추가로 넘김
        report = full_run(corp_name, XBRL_DIR, JSON_DIR, start_date, end_date)

        output_path = OUTPUT_ROOT / f"{corp_name}_리포트.md"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)

        print(f"✔ {corp_name} 리포트 완료 → {output_path}\n")
    except Exception as e:
        print(f"✖ {corp_name} 리포트 생성 오류: {e}\n")

