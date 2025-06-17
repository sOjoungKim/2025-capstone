# bert_sentiment.py
import json
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from collections import Counter
import pandas as pd

# ✅ 감성 분석 모델 경로
NEWS_MODEL = "snunlp/KR-FinBert-SC"

# ✅ 토크나이저 및 모델 로딩
news_tokenizer = AutoTokenizer.from_pretrained(NEWS_MODEL)
news_model = AutoModelForSequenceClassification.from_pretrained(NEWS_MODEL)
news_model.eval()

# ✅ 감정 라벨 (뉴스: 0-부정, 1-중립, 2-긍정)
news_label_map = {0: "부정", 1: "중립", 2: "긍정"}

# ✅ 분석 함수
def analyze_sentiment(text: str, tokenizer, model, label_map):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    with torch.no_grad():
        logits = model(**inputs).logits[0]
    probs = softmax(logits.numpy())
    label_id = int(probs.argmax())
    return label_map[label_id], float(probs[label_id])

# ✅ 기사/토론 리스트 감성 분석 적용
def apply_sentiment_to_articles(articles: list[dict], text_key: str, content_type: str) -> list[dict]:
    results = []
    for article in articles:
        text = article.get(text_key, "").strip()
        if not text:
            continue
        label, score = analyze_sentiment(text, news_tokenizer, news_model, news_label_map)
        article["감정"] = label
        article["점수"] = round(score, 4)
        results.append(article)
    return results

# ✅ 감정 건수 카운팅
def count_sentiments(articles: list[dict]) -> Counter:
    return Counter([a["감정"] for a in articles])

# ✅ 실행 테스트
if __name__ == "__main__":

    # JSON 파일 로드
    with open("JSON/동성화인텍.json", encoding="utf-8") as f:
        data = json.load(f)

    def print_sentiment_list(items, label):
        # 요약 집계
        counts = count_sentiments(items)
        print(f"\n📌 [{label}] 감성 분석 결과 ({len(items)}건)")
        print(f"  긍정: {counts.get('긍정',0)}건, 부정: {counts.get('부정',0)}건, 중립: {counts.get('중립',0)}건\n")
        # 상세 출력
        for i, art in enumerate(items, start=1):
            title = art.get("제목", "(제목 없음)")
            body  = art.get("본문", "").replace("\n"," ")
            snippet = body[:80] + ("..." if len(body) > 80 else "")
            print(f"{i}. 제목: {title}")
            print(f"   본문: {snippet}")
            print(f"   감정: {art['감정']} (점수: {art['점수']:.2f})\n")

    # 기업 뉴스공시
    news = apply_sentiment_to_articles(data.get("뉴스공시", []), text_key="본문", content_type="뉴스공시")
    print_sentiment_list(news, "뉴스공시")

    # 업종 뉴스
    industry = apply_sentiment_to_articles(data.get("업종뉴스", []), text_key="본문", content_type="업종뉴스")
    print_sentiment_list(industry, "업종뉴스")
