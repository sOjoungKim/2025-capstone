from pykrx import stock
import pandas as pd
import requests
import xml.etree.ElementTree as ET

def get_basic_info(stock_code: str):
    # KRX 기업리스트 (kind.krx.co.kr)
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    df = pd.read_html(requests.get(url).text, encoding='euc-kr')[0]
    df['종목코드'] = df['종목코드'].map('{:06d}'.format)
    info = df[df['종목코드'] == stock_code].iloc[0]
    
    return {
        "기업명": info["회사명"],
        "종목코드": stock_code,
        "업종": info["업종"],
        "상장일": info["상장일"],
        "본사 위치": info["지역"],
        "대표자명": info["대표자명"],
        "홈페이지": info["홈페이지"]
    }

# 사용자 입력 기반
# print(get_basic_info(input("Enter the stock code (6 digits): ")))


