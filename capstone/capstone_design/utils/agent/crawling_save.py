# ✅ 뉴스/토론 캐시 저장 모듈 (날짜 자동)
import os
import json
import datetime
import pandas as pd
from No4_news_crawling import (
    crawl_naver_news_by_company,
    crawl_news_by_industry_keywords_weighted,
    extract_article_body,
    clean_article_body,
    industry_map
)
from No4_debating_crawling import crawl_naver_board_optimized


def get_full_article(df: pd.DataFrame) -> list:
    results = []
    for _, row in df.iterrows():
        try:
            raw = extract_article_body(row["url"])
            body = clean_article_body(raw)
            results.append({
                "제목": row["title"],
                "날짜": row["date"],
                "본문": body,
                "링크": row["url"]
            })
        except Exception as e:
            print(f"❌ 본문 오류: {e}")
    return results


def save_news_json(company_name: str, path="news_tmp.json", per_keyword=80):
    info = industry_map.get(company_name)
    if not info:
        raise ValueError(f"❌ '{company_name}' is not in industry_map")

    code = info["종목코드"]
    keywords = info["산업명"]
    if isinstance(keywords, str):
        keywords = [keywords]

    df_news = crawl_naver_news_by_company(company_name, max_limit=1000)
    kw_limits = {kw: per_keyword for kw in keywords}
    df_industry = crawl_news_by_industry_keywords_weighted(kw_limits)

    news_json = {
        "기업명": company_name,
        "종목코드": code,
        "뉴스공시": get_full_article(df_news),
        "업종뉴스": get_full_article(df_industry),
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(news_json, f, ensure_ascii=False, indent=2)


def save_board_json(code: str, path="board_tmp.json"):
    df = crawl_naver_board_optimized(code=code, max_page=5)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"종목토론실": df.to_dict(orient="records")}, f, ensure_ascii=False, indent=2)


def merge_json(news_path: str, board_path: str, output_path: str):
    with open(news_path, encoding="utf-8") as f1, open(board_path, encoding="utf-8") as f2:
        news = json.load(f1)
        board = json.load(f2)
    news["종목토론실"] = board.get("종목토론실", [])
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2)


# ✅ 실행 예시
if __name__ == "__main__":
    cname = "성우하이텍"
    today = datetime.datetime.now().strftime("%Y%m%d")

    news_path = f"news_tmp.json"
    board_path = f"board_tmp.json"
    final_path = f"{today}_{cname}.json"

    save_news_json(cname, news_path, per_keyword=80)
    save_board_json(industry_map[cname]["종목코드"], board_path)
    merge_json(news_path, board_path, final_path)
