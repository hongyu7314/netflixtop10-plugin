"""
Netflix Tudum Top 10 数据抓取与解析层（纯标准库，便于独立测试）。

数据来源为 Netflix 官方公开的周榜 TSV 文件（每周更新，通常为周二发布）：
- all-weeks-global.tsv    全球榜（含周观看时长/观看次数/片长）
- all-weeks-countries.tsv 国家/地区榜（约 90+ 市场，仅排名与上榜周数）

两个文件的表头（以实际文件为准，按列名解析，不依赖列顺序）：
- 全球：week, category, weekly_rank, show_title, season_title,
        weekly_hours_viewed, runtime, weekly_views, cumulative_weeks_in_top_10
- 国家：week, country_name, country_iso2, category, weekly_rank, show_title,
        season_title, cumulative_weeks_in_top_10

category 取值：Films (English) / Films (Non-English) / TV (English) / TV (Non-English)
"""

import re
from typing import Any, Dict, List, Optional, Tuple

# ─── 分类映射 ───

# 插件内部分类 key -> TSV category 文本
CATEGORY_MAP: Dict[str, str] = {
    "tv_en": "TV (English)",
    "tv_nonen": "TV (Non-English)",
    "film_en": "Films (English)",
    "film_nonen": "Films (Non-English)",
}
# TSV category 文本 -> 插件内部分类 key
CATEGORY_MAP_INV: Dict[str, str] = {v: k for k, v in CATEGORY_MAP.items()}
# 分类中文显示名
CATEGORY_ZH: Dict[str, str] = {
    "tv_en": "剧集（英语）",
    "tv_nonen": "剧集（非英语）",
    "film_en": "电影（英语）",
    "film_nonen": "电影（非英语）",
}
# 展示顺序
CATEGORY_ORDER: List[str] = ["tv_en", "tv_nonen", "film_en", "film_nonen"]

# ─── 常用国家（Netflix 主要市场，TSV country_name 为英文原名）───

COMMON_COUNTRIES: List[str] = [
    "United States", "United Kingdom", "Canada", "Australia", "New Zealand",
    "Japan", "South Korea", "Taiwan", "Hong Kong", "Singapore", "Malaysia",
    "Thailand", "Philippines", "Vietnam", "Indonesia", "India", "France",
    "Germany", "Spain", "Italy", "Netherlands", "Poland", "Turkey", "Brazil",
    "Mexico", "Argentina", "Colombia", "Chile", "Saudi Arabia",
    "United Arab Emirates", "Egypt", "Nigeria", "Kenya", "South Africa",
    "Israel", "Sweden", "Norway", "Denmark", "Finland", "Ireland", "Belgium",
    "Switzerland", "Austria", "Portugal", "Greece", "Czech Republic",
    "Romania", "Hungary", "Ukraine", "Russia",
]

# 常用国家中文名（仅用于界面展示，value 始终为 TSV 英文原名）
COUNTRY_ZH: Dict[str, str] = {
    "United States": "美国",
    "United Kingdom": "英国",
    "Canada": "加拿大",
    "Australia": "澳大利亚",
    "New Zealand": "新西兰",
    "Japan": "日本",
    "South Korea": "韩国",
    "Taiwan": "中国台湾",
    "Hong Kong": "中国香港",
    "Singapore": "新加坡",
    "Malaysia": "马来西亚",
    "Thailand": "泰国",
    "Philippines": "菲律宾",
    "Vietnam": "越南",
    "Indonesia": "印度尼西亚",
    "India": "印度",
    "France": "法国",
    "Germany": "德国",
    "Spain": "西班牙",
    "Italy": "意大利",
    "Netherlands": "荷兰",
    "Poland": "波兰",
    "Turkey": "土耳其",
    "Brazil": "巴西",
    "Mexico": "墨西哥",
    "Argentina": "阿根廷",
    "Colombia": "哥伦比亚",
    "Chile": "智利",
    "Saudi Arabia": "沙特阿拉伯",
    "United Arab Emirates": "阿联酋",
    "Egypt": "埃及",
    "Nigeria": "尼日利亚",
    "Kenya": "肯尼亚",
    "South Africa": "南非",
    "Israel": "以色列",
    "Sweden": "瑞典",
    "Norway": "挪威",
    "Denmark": "丹麦",
    "Finland": "芬兰",
    "Ireland": "爱尔兰",
    "Belgium": "比利时",
    "Switzerland": "瑞士",
    "Austria": "奥地利",
    "Portugal": "葡萄牙",
    "Greece": "希腊",
    "Czech Republic": "捷克",
    "Romania": "罗马尼亚",
    "Hungary": "匈牙利",
    "Ukraine": "乌克兰",
    "Russia": "俄罗斯",
}


def to_int(value: Any) -> int:
    """将 TSV 中的数值文本安全转为 int（兼容 '37,700,000'、'37700000' 等格式）。"""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else 0


def format_count(n: Optional[int]) -> str:
    """按中文习惯格式化观看次数/时长：1.2亿 / 3456万 / 1234。"""
    if not n or n <= 0:
        return ""
    if n >= 100_000_000:
        return f"{n / 100_000_000:.1f}亿"
    if n >= 10_000:
        return f"{n / 10_000:.0f}万"
    return str(n)


def normalize_season(season: Any) -> str:
    """标准化季信息：'Season 1' -> '第1季'；'N/A'/空 -> ''。"""
    s = str(season or "").strip()
    if not s or s.upper() == "N/A":
        return ""
    m = re.match(r"^Season\s*(\d+)$", s, re.IGNORECASE)
    if m:
        return f"第{m.group(1)}季"
    m = re.match(r"^Season\s*(\d+)\s*-\s*Season\s*(\d+)$", s, re.IGNORECASE)
    if m:
        return f"第{m.group(1)}-{m.group(2)}季"
    return s


def country_match(row_country: str, want: str) -> bool:
    """国家名匹配：精确 -> 忽略大小写 -> 子串（双向），兼容 'Hong Kong'/'Hong Kong SAR' 等拼写差异。"""
    rc = str(row_country or "").strip()
    w = str(want or "").strip()
    if not w:
        return True
    if not rc:
        return False
    if rc == w:
        return True
    rc_l, w_l = rc.lower(), w.lower()
    if rc_l == w_l:
        return True
    return w_l in rc_l or rc_l in w_l


def parse_tsv(text: str) -> Tuple[List[Dict[str, str]], List[str]]:
    """解析 TSV 文本为行 dict 列表（按表头名索引）。

    :return: (rows, countries) rows 为原始行（未做周/国家过滤），countries 为文件中出现的全部国家名。
    """
    rows: List[Dict[str, str]] = []
    countries: List[str] = []
    if not text:
        return rows, countries
    lines = text.splitlines()
    if len(lines) < 2:
        return rows, countries
    header = [h.strip() for h in lines[0].split("\t")]
    if "show_title" not in header or "weekly_rank" not in header:
        raise ValueError(f"TSV 表头异常，缺少必需列：{header}")
    country_seen = set()
    ncols = len(header)
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < ncols:
            parts += [""] * (ncols - len(parts))
        row = {header[i]: parts[i].strip() for i in range(ncols)}
        if not row.get("show_title"):
            continue
        cn = row.get("country_name", "")
        if cn and cn not in country_seen:
            country_seen.add(cn)
            countries.append(cn)
        rows.append(row)
    countries.sort()
    return rows, countries


def extract_latest(
    text: str,
    country: Optional[str] = None,
    categories: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """解析 TSV 并提取每个分类最新一周的标准化条目。

    :param text: TSV 文本
    :param country: None=全球榜（不过滤）；否则按国家名过滤（country_match 模糊匹配）
    :param categories: 需要保留的内部分类 key 列表；None=全部四个分类
    :return: (items, countries) items 已按分类+排名排序，countries 为文件中全部国家名
    """
    rows, countries = parse_tsv(text)
    if not rows:
        return [], countries

    wanted_cats = set(categories or CATEGORY_ORDER)

    # 单次遍历：国家过滤 + 分类过滤，并记录每个分类的最新周
    filtered: List[Dict[str, str]] = []
    latest_week: Dict[str, str] = {}
    for row in rows:
        cat_key = CATEGORY_MAP_INV.get(row.get("category", ""))
        if not cat_key or cat_key not in wanted_cats:
            continue
        if country is not None and not country_match(row.get("country_name", ""), country):
            continue
        week = row.get("week", "")
        if week > latest_week.get(cat_key, ""):
            latest_week[cat_key] = week
        filtered.append(row)

    # 取最新周
    items: List[Dict[str, Any]] = []
    for row in filtered:
        cat_key = CATEGORY_MAP_INV.get(row.get("category", ""))
        if not cat_key:
            continue
        if row.get("week", "") != latest_week.get(cat_key, ""):
            continue
        items.append(_normalize_row(row, cat_key))

    items.sort(key=lambda x: (CATEGORY_ORDER.index(x["category"]), x["rank"]))
    return items, countries


def _normalize_row(row: Dict[str, str], cat_key: str) -> Dict[str, Any]:
    """将一行 TSV 数据标准化为插件条目。"""
    return {
        "rank": to_int(row.get("weekly_rank")) or 0,
        "name": row.get("show_title", "").strip(),
        "season": normalize_season(row.get("season_title", "")),
        "category": cat_key,
        "category_name": CATEGORY_MAP.get(cat_key, row.get("category", "")),
        "category_zh": CATEGORY_ZH.get(cat_key, cat_key),
        "mtype": "MOVIE" if cat_key.startswith("film") else "TV",
        "week": row.get("week", ""),
        "views": to_int(row.get("weekly_views")),
        "hours_viewed": to_int(row.get("weekly_hours_viewed")),
        "weeks_in_top10": to_int(row.get("cumulative_weeks_in_top_10")) or 0,
        # 以下字段由插件主类在 TMDB 识别后填充
        "tmdbid": 0,
        "poster": "",
        "year": "",
        "zh_name": "",
        "status": "",
    }


def country_display_name(country: str) -> str:
    """国家名界面展示：常用国家附中文，其他原样。"""
    zh = COUNTRY_ZH.get(country)
    return f"{zh}（{country}）" if zh else country
