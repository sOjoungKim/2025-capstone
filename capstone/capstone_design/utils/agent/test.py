import os
import unicodedata

corp_name = "데브시스터즈"
json_dir = "/Users/icebear/Desktop/workspace/capstone/No5_Report_Generator"
normalized_name = unicodedata.normalize("NFC", corp_name)

print("corp_name 포함 json 파일 (정규화 비교):")
print([
    f for f in os.listdir(json_dir)
    if normalized_name in unicodedata.normalize("NFC", f) and f.endswith(".json")
])
