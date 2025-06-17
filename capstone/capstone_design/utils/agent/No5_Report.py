from datetime import datetime, timedelta
import pandas as pd
from pykrx import stock
import json
import os
from typing import Dict
from dataclasses import dataclass

from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.chains.llm import LLMChain

from No1_Basic_company_info import get_basic_info
from No2__main_macro import get_macro_series
from No3__main_financial import (
    analyze_financial_ratios_csv,
    fetch_and_save_stock_price,
    get_latest_trading_day,
    get_latest_valid_fundamental
)
from No4_Sentimental_Analysis import apply_sentiment_to_articles
from industry_map import industry_map
from dotenv import load_dotenv

load_dotenv()

# 기본 파라미터
start_date = "2025-01-01"
end_date = "2025-05-31"
DEFAULT_PRICE_START = "20250101"
DEFAULT_PRICE_END   = "20250531"
FUND_LOOKBACK_DAYS  = 180
MACRO_LOOKBACK_DAYS = 180

# 데이터 수집 및 요약 -------------------------------------------------------------------

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

def extract_macro_summary(df_macro):
    return {row["indicator"]: round(row["correlation"], 3)
            for _, row in df_macro.iterrows() if row["significance"]}

def extract_industry_summary(df_related):
    summary = {"count": int(len(df_related))}
    for col in ["PER", "PBR", "EPS", "BPS", "DIV", "DPS"]:
        vals = df_related[col].dropna()
        summary[f"{col}_avg"] = float(vals.mean()) if not vals.empty else None
        summary[f"{col}_min"] = float(vals.min())  if not vals.empty else None
        summary[f"{col}_max"] = float(vals.max())  if not vals.empty else None
    return summary

def extract_metrics_summary(df_fin):
    latest = df_fin.iloc[-1]
    return {
        "ROE": float(latest["ROE"]),
        "ROA": float(latest["ROA"]),
        "영업이익률": float(latest["영업이익률"]),
        "PER": float(latest["PER"]),
        "EPS": float(latest["EPS"]),
        "PBR": float(latest["PBR"]),
        "부채비율": float(latest["부채비율"]),
        "유동비율": float(latest["유동비율"]),
    }

def extract_sentiment_summary(sentiments):
    def summarize(items):
        pos = sum(1 for r in items if r["감정"] == "긍정")
        neg = sum(1 for r in items if r["감정"] == "부정")
        neu = len(items) - pos - neg
        return {"총건수": len(items), "긍정": pos, "부정": neg, "중립": neu}

    return {
        "뉴스": summarize(sentiments["뉴스공시"]["items"]),
        "업종뉴스": summarize(sentiments["업종뉴스"]["items"])
    }

def generate_data_summary(stock_code: str, xbrl_base_dir: str, json_news_path: str) -> dict:
    info = get_basic_info(stock_code)
    corp_name = info["기업명"]
    related = industry_map[corp_name].get("관련기업", [])

    fetch_and_save_stock_price(corp_name, stock_code, DEFAULT_PRICE_START, DEFAULT_PRICE_END)
    for rel_name, rel_code in related:
        fetch_and_save_stock_price(rel_name, rel_code, DEFAULT_PRICE_START, DEFAULT_PRICE_END)

    df_fin = analyze_financial_ratios_csv(stock_code, corp_name, xbrl_base_dir)

    ref_date = get_latest_trading_day()
    rel_vals = {name: get_latest_valid_fundamental(code, ref_date) for name, code in related}
    df_rel = pd.DataFrame({n: v for n, v in rel_vals.items() if v}).T

    with open(json_news_path, encoding="utf-8") as fp:
        raw = json.load(fp)

    sentiments = {}
    for key, field in [("뉴스공시", "본문"), ("업종뉴스", "본문")]:
        articles = raw.get(key, [])
        items = apply_sentiment_to_articles(articles, field, key)
        sentiments[key] = {"items": items}

    end_macro = datetime(2025,5,31)
    start_macro = end_macro - timedelta(days=MACRO_LOOKBACK_DAYS)
    df_macro = get_macro_series(stock_code, start_macro, end_macro)

    return {
        "기업기본정보": info,
        "재무분석": extract_metrics_summary(df_fin),
        "관련기업": extract_industry_summary(df_rel),
        "감성": extract_sentiment_summary(sentiments),
        "거시": extract_macro_summary(df_macro)
    }

# LLM 판단 -------------------------------------------------------------------

def generate_llm_report(summary: dict) -> str:
    llm = ChatOpenAI(temperature=0.2, model="gpt-4o")

    prompt = PromptTemplate(
        input_variables=["corp_name", "macro", "industry", "financial", "sentiment"],
        template="""
당신은 퀀트 애널리스트입니다. 아래 데이터를 종합적으로 판단하고 최종 투자 의견을 도출하십시오.

# {corp_name} 리포트
## 창의적인 소제목(리포트 요약)

## 경제분석:
{macro}

## 산업분석:
{industry}

## 기업분석:
{financial}

## 감성분석 요약:
{sentiment}

## 최종 요약:

### 작성 지침:
- 반드시 수치 기반 해석 → 논리 → 최종 의견 순으로 작성
- 기업 최근 뉴스:
    "{news_argument}"
    위 내용을 참고하고, 충분히 인용할 것
- 경제분석/산업분석/기업분석 후 반드시 마지막에 [긍정/부정/중립] 중 하나 표시 할 것
- 수치 해석의 근거를 충분히 제시하고, 각 분석 섹터 별로 근거를 풍부하게 작성할 것
- 분석에 있어 제공된 수치들에 대한 가능성을 잘 표현할 것. 
- 최종 요약에서 투자 의견은 [매수 / 보유 / 매도] 중 하나로 표기할 것.
- 최종 요약에는 매수/매도/중립의 비율을 마지막에 따로 기술할 것. 
- 투자 관점에서 냉정하게 판단할 것.
"""
    )

    input_data = {
        "corp_name": summary["기업기본정보"]["기업명"],
        "macro": str(summary["거시"]),
        "industry": str(summary["관련기업"]),
        "financial": str(summary["재무분석"]),
        "sentiment": str(summary["감성"]),
        "news_argument": news_argument
    }

    result = LLMChain(prompt=prompt, llm=llm).run(input_data)
    return result

# 전체 파이프라인 실행 ------------------------------------------------------

def full_run(corp_name: str, xbrl_path: str, json_dir: str):
    stock_code = industry_map[corp_name]["종목코드"]
    files = [f for f in os.listdir(json_dir) if corp_name in f and f.endswith(".json")]
    json_path = os.path.join(json_dir, sorted(files, reverse=True)[0])

    summary = generate_data_summary(stock_code, xbrl_path, json_path)
    report = generate_llm_report(summary)
    # print(report)
    return report


from pathlib import Path

# 공통 경로
XBRL_DIR    = r"./agent/재무재표"
JSON_DIR    = r"./agent/json"
OUTPUT_ROOT = Path(r"./agent/md")

# 분석 대상 기업 리스트
targets = [
    "성우하이텍",
    "고려제강",
    "셀바스헬스케어",
    "메카로",
    "한선엔지니어링",
    "데브시스터즈",
    "동성화인텍"
]

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

for corp_name in targets:
    try:
        print(f"▶ {corp_name} LLM 리포트 생성 시작")
        report = full_run(corp_name, XBRL_DIR, JSON_DIR)

        output_path = OUTPUT_ROOT / f"{corp_name}_리포트_LLM.md"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)

        print(f"✔ {corp_name} 리포트 완료 → {output_path}\n")
    except Exception as e:
        print(f"✖ {corp_name} 리포트 생성 오류: {e}\n")
