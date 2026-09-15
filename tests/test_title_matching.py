"""离线单测：抽出插件里的标题匹配 helper，用固定样本验证优先级。"""
import re
import sys
from datetime import datetime
from enum import Enum

SRC = r"C:/Users/73142/WorkBuddy/2026-09-12-22-06-21/netflix-plugin-staging/plugins.v3/netflixtop10/__init__.py"
text = open(SRC, encoding="utf-8").read()

start = text.index("_NETFLIX_SUFFIX_RE = re.compile(")
end = text.index("class TmdbHelper:")
block = text[start:end]


class MediaType(Enum):
    MOVIE = "MOVIE"
    TV = "TV"


ns = {"re": re, "datetime": datetime, "MediaType": MediaType}
exec("from typing import Any, Dict, List, Optional\n" + block, ns)

clean = ns["_clean_netflix_title"]
norm = ns["_normalize_title_for_compare"]
pick = ns["_pick_best_tmdb_match"]

failures = []


def check(label, got, want):
    ok = got == want
    print(f"{'OK  ' if ok else 'FAIL'} {label}: got={got} want={want}")
    if not ok:
        failures.append(label)


# --- _clean_netflix_title ---
check("clean('Wednesday: Season 2')", clean("Wednesday: Season 2"), "Wednesday")
check("clean('Squid Game: Season 3')", clean("Squid Game: Season 3"), "Squid Game")
check("clean('Stranger Things 5')", clean("Stranger Things 5"), "Stranger Things 5")
check("clean('The Witcher (2019)')", clean("The Witcher (2019)"), "The Witcher")
check("clean('KPop Demon Hunters')", clean("KPop Demon Hunters"), "KPop Demon Hunters")

# --- strict 命中 ---
movies = [
    {"id": 18387, "title": "伟大的星期三", "original_title": "Big Wednesday", "poster_path": "/x.jpg"},
    {"id": 808, "title": "怪物史瑞克", "original_title": "Shrek", "poster_path": "/s.jpg"},
    {"id": 252634, "title": "Shrek Stories", "original_title": "Shrek Stories", "poster_path": "/t.jpg"},
]
check("strict movie 'Shrek'", pick("Shrek", movies, MediaType.MOVIE, strict=True), 808)

tv = [
    {"id": 88119, "name": "The Gentlemen's Hentai Club", "original_name": "The Gentlemen's Hentai Club", "poster_path": "/a.jpg"},
    {"id": 236235, "name": "绅士们", "original_name": "The Gentlemen", "poster_path": "/b.jpg"},
]
check("strict tv 'The Gentlemen'", pick("The Gentlemen", tv, MediaType.TV, strict=True), 236235)

tv2 = [
    {"id": 331466, "name": "Outer Banks: The Official Podcast", "original_name": "Outer Banks: The Official Podcast", "poster_path": "/c.jpg"},
    {"id": 100757, "name": "外滩探秘", "original_name": "Outer Banks", "poster_path": "/d.jpg"},
]
check("strict tv 'Outer Banks'", pick("Outer Banks", tv2, MediaType.TV, strict=True), 100757)

# 只有不相干结果时 strict 应该放弃
junk = [{"id": 88119, "name": "The Gentlemen's Hentai Club", "original_name": "The Gentlemen's Hentai Club", "poster_path": "/a.jpg"}]
check("strict 放弃无关候选(忽略子串)", pick("完全无关的标题Q", junk, MediaType.TV, strict=True), 0)

# --- 子串匹配取长度最接近 ---
sub = [
    {"id": 46791, "name": "Graveyard Carz", "original_name": "Graveyard Carz", "poster_path": "/g.jpg"},
    {"id": 99999, "name": "Graveyard", "original_name": "Graveyard", "poster_path": "/h.jpg"},
]
check("子串取长度最接近", pick("Graveyard", sub, MediaType.TV, strict=True), 99999)

# --- 宽松兜底：更近期且更热门的候选应顶替相关性第一条 ---
loose = [
    {"id": 600251, "title": "Shakespeare's Mother", "original_title": "Shakespeare's Mother",
     "release_date": "2015-02-12", "popularity": 1.203, "poster_path": "/old.jpg"},
    {"id": 1631807, "title": "她的秘密人生", "original_title": "Den hemmelige kvinde",
     "release_date": "2026-08-28", "popularity": 34.68, "poster_path": "/new.jpg"},
]
check("loose 近期高热顶替", pick("The Secret Woman", loose, MediaType.MOVIE), 1631807)

# 老片应当保留相关性第一条（近期候选热度更低时不替换）
keep = [
    {"id": 5, "title": "老片", "original_title": "老片", "release_date": "2004-05-19",
     "popularity": 40.0, "poster_path": "/o.jpg"},
    {"id": 6, "title": "新片", "original_title": "新片", "release_date": "2026-01-01",
     "popularity": 3.0, "poster_path": "/n.jpg"},
]
check("loose 不替换更冷门的近期候选", pick("老片", keep, MediaType.MOVIE), 5)

# 无海报时宽松兜底退回原列表
noposter = [{"id": 7, "title": "A", "original_title": "A", "poster_path": ""}]
check("loose 无海报回退", pick("题外", noposter, MediaType.MOVIE), 7)

check("空列表", pick("任何", [], MediaType.MOVIE), 0)

# --- 静态检查：模块顶层用到的标准库必须已 import ---
# 之前 _NETFLIX_SUFFIX_RE = re.compile(...) 用了 re 却没 import re，
# 插件在容器里 import 直接 NameError（run-once 返回 404）。这里做个兜底检查。
REQUIRED_IMPORTS = ["re", "hashlib", "random", "threading", "time"]
for mod in REQUIRED_IMPORTS:
    used = re.search(rf"(?<![\w.]){mod}\.", text) is not None
    imported = re.search(rf"^import {mod}\b|^import .*\b{mod}\b", text, re.M) is not None
    if used and not imported:
        failures.append(f"缺少 import {mod}")
        print(f"FAIL 模块顶层使用了 {mod}. 但没有 import {mod}")
    else:
        print(f"OK   导入检查 {mod}: used={used} imported={imported}")

print()
if failures:
    print(f"{len(failures)} 个用例失败：{failures}")
    sys.exit(1)
print("全部用例通过")
