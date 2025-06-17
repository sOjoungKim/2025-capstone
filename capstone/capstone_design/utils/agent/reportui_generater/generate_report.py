import re
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# 1. 기업 리스트
companies = ['현대자동차',"셀바스헬스케어", "성우하이텍", "데브시스터즈", '고려제강', '메카로', '에코프로', '한미반도체', '한선엔지니어링', '한화에어로스페이스','HD현대중공업']

BASE_DIR = Path(__file__).resolve().parent
MD_DIR = BASE_DIR / "reports"
IMG_DIR = BASE_DIR / "img"
OUTPUT_DIR = BASE_DIR / "output"
TEMPLATES_DIR = BASE_DIR / "templates"

# 2. 마크다운 파일에서 최종 판단과 본문 추출
def parse_md_file(file_path):
    with open(file_path, encoding="utf-8") as f:
        lines = f.readlines()

    last_line = lines[-1].strip()
    body_md = "".join(lines)

    match = re.search(r"최종[\s]*판단[:：]?[\s]*(\w+)", last_line)
    opinion = match.group(1) if match else "의견 없음"

    return {
        "opinion": opinion,
        "markdown": body_md
    }

def build_image_groups(company):
    categories = ["성장성", "수익성", "안정성", "시장가치", "활동성", "거시경제지표"]
    group_dict = {}

    for cat in categories:
        group_dir = IMG_DIR / company / cat
        if group_dir.exists():
            # output 폴더에서 상대경로 사용
            images = [f"../img/{company}/{cat}/{p.name}" for p in group_dir.glob("*.png")]
            if images:
                group_dict[cat] = images

    return group_dict

# 4. HTML 렌더링 및 저장
def generate_report(md_path, company: str):
    parsed = parse_md_file(md_path)
    image_groups = build_image_groups(company)
    base_dir = Path(__file__).resolve().parent  # report/ 디렉토리
    templates_path = base_dir / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_path)))

    template = env.get_template("markdown_viewer.html")

    output_html = template.render(
        company=company,
        opinion=parsed["opinion"],
        markdown=parsed["markdown"],
        image_groups=image_groups
    )

    output_path = base_dir / "output" / f"{company}.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output_html)


# 5. 실행 로직
def main():
    for company in companies:
        md_file = MD_DIR / f"{company}_리포트.md"
        if md_file.exists():
            generate_report(md_file, company)
        else:
            print(f"[!] 마크다운 파일 없음: {company}")


if __name__ == "__main__":
    main()
