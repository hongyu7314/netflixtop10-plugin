"""离线单测：海报卡片结构 + 版本号一致性。

背景（v1.1.3 修的问题）：
    MoviePilot 插件页由前端 PageRender 统一绑定 events.click，其 commonAction
    不调用 preventDefault / stopPropagation。所以只要「订阅」按钮位于卡片的
    <a href="#/media?..."> 内部，点订阅就会冒泡激活链接 → 同时跳转/弹出系统详情页，
    用户既看不到订阅结果，又以为"点订阅弹出详情页"。配置层无法阻止冒泡，
    唯一可靠做法是把按钮移出 <a>。这里把该结构约束固化成用例。
"""
import json
import re
import sys
import textwrap
from enum import Enum
from urllib.parse import quote

ROOT = "C:/Users/73142/WorkBuddy/2026-09-12-22-06-21/netflix-plugin-staging"
SRC = f"{ROOT}/plugins.v3/netflixtop10/__init__.py"
text = open(SRC, encoding="utf-8").read()

failures = []


def check(label, got, want):
    ok = got == want
    print(f"{'OK  ' if ok else 'FAIL'} {label}: got={got!r} want={want!r}")
    if not ok:
        failures.append(label)


def ok(label, cond, extra=""):
    print(f"{'OK  ' if cond else 'FAIL'} {label}{(' — ' + extra) if extra else ''}")
    if not cond:
        failures.append(label)


# ─── 抽出 _render_poster_card 方法体，用桩对象离线调用 ───
start = text.index("    def _render_poster_card(self, item: dict) -> dict:")
end = text.index("    def _render_item_row(self, item: dict) -> dict:")
method_src = textwrap.dedent(text[start:end])


class MediaType(Enum):
    MOVIE = "MOVIE"
    TV = "TV"


class _Settings:
    """桩：_render_poster_card 会把 settings.API_TOKEN 塞进按钮事件参数。"""
    API_TOKEN = "TEST_TOKEN"


ns = {
    "quote": quote,
    "format_count": lambda n: f"{n:,}",
    "MediaType": MediaType,
    "settings": _Settings(),
    "Any": object,
    "Dict": dict,
    "List": list,
    "Optional": None,
}
exec(method_src, ns)
render_poster_card = ns["_render_poster_card"]


class FakeSelf:
    """桩：只提供 _render_poster_card 依赖的 _check_media_status。"""

    def __init__(self, status):
        self._status = status

    def _check_media_status(self, tmdbid, name="", mtype="TV"):
        return self._status


def walk(node):
    if not isinstance(node, dict):
        return
    yield node
    for child in node.get("content") or []:
        yield from walk(child)


def anchors(root):
    return [n for n in walk(root) if n.get("component") == "a"]


def api_buttons(root):
    return [
        n for n in walk(root)
        if n.get("component") == "VBtn"
        and ((n.get("events") or {}).get("click") or {}).get("api")
    ]


def button_by_text(root, label):
    for n in walk(root):
        if n.get("component") == "VBtn" and n.get("props", {}).get("text") == label:
            return n
    return None


BASE_ITEM = {
    "rank": 3,
    "name": "Outer Banks",
    "zh_name": "外滩探秘",
    "mtype": "TV",
    "tmdbid": 100757,
    "poster": "/api/v1/system/img/1?imgurl=https://image.tmdb.org/t/p/w500/x.jpg",
    "year": "2020",
    "season": "Outer Banks: Season 5",
    "weeks_in_top10": 4,
    "views": 13900000,
}

# ─── 1) 未订阅：按钮可点，且在 <a> 之外 ───
card = render_poster_card(FakeSelf("未添加订阅"), dict(BASE_ITEM))
sub = button_by_text(card, "订阅")
ok("未订阅时渲染出「订阅」按钮", sub is not None)
ok("「订阅」按钮带 subscribe 接口事件",
   sub is not None and sub["events"]["click"]["api"] == "plugin/NetflixTop10/subscribe")
ok("「订阅」按钮参数含 tmdbid/mtype/apikey",
   sub is not None
   and sub["events"]["click"]["params"].get("tmdbid") == 100757
   and sub["events"]["click"]["params"].get("mtype") == "TV"
   and "apikey" in sub["events"]["click"]["params"])

anchors_found = anchors(card)
check("卡片内有 1 个详情页链接", len(anchors_found), 1)
if anchors_found:
    expected_href = (
        "#/media?media_source=themoviedb&media_id=100757"
        "&title=%E5%A4%96%E6%BB%A9%E6%8E%A2%E7%A7%98&year=2020&type=%E7%94%B5%E8%A7%86%E5%89%A7"
    )
    check("详情页链接 query 与 MP v3 前端一致（media_source/media_id/title/year/type）",
          anchors_found[0]["props"]["href"], expected_href)
    # 核心断言：任何可点击的接口按钮都不得位于 <a> 内部（否则点击会冒泡触发跳转）
    for idx, a in enumerate(anchors_found):
        leaked = api_buttons(a)
        ok(f"第 {idx + 1} 个 <a> 内不含带接口事件的按钮", not leaked,
           f"发现 {[b['props'].get('text') for b in leaked]}")
    ok("「订阅」按钮位于 <a> 之外",
       sub is not None and not any(sub is b for a in anchors_found for b in walk(a)))

# ─── 2) 已订阅 / 已入库：按钮禁用且有明确文字（订阅结果的可见反馈） ───
for status, label, color in (("订阅已添加", "订阅已添加", "info"), ("影片已入库", "影片已入库", "success")):
    c = render_poster_card(FakeSelf(status), dict(BASE_ITEM))
    b = button_by_text(c, label)
    ok(f"{status} → 按钮文案为「{label}」", b is not None)
    ok(f"{status} → 按钮 disabled、无接口事件",
       b is not None and b["props"].get("disabled") is True and not b.get("events"))
    ok(f"{status} → 按钮配色为 {color}",
       b is not None and b["props"].get("color") == color)

# ─── 3) 未识别到 TMDB：不给可点按钮，也不给详情链接 ───
no_tmdb = dict(BASE_ITEM, tmdbid=0)
c3 = render_poster_card(FakeSelf("未添加订阅"), no_tmdb)
ok("未识别 TMDB 时提示「未识别 TMDB」", button_by_text(c3, "未识别 TMDB") is not None)
ok("未识别 TMDB 时无详情页链接", not anchors(c3))
ok("未识别 TMDB 时无接口按钮", not api_buttons(c3))

# ─── 4) 三处版本号必须一致（不一致会被 MP 静默回滚容器内文件） ───
ver_code = re.search(r'plugin_version\s*=\s*"([^"]+)"', text).group(1)
ver_pkg = json.load(open(f"{ROOT}/plugins.v3/netflixtop10/package.v3.json", encoding="utf-8"))["Version"]
ver_idx = json.load(open(f"{ROOT}/package.v3.json", encoding="utf-8"))["NetflixTop10"]["version"]
check("代码 plugin_version", ver_code, ver_pkg)
check("根 index version", ver_idx, ver_pkg)
ok("根 index 有该版本的 history 记录",
   f"v{ver_pkg}" in json.load(open(f"{ROOT}/package.v3.json", encoding="utf-8"))["NetflixTop10"]["history"])

print()
if failures:
    print(f"{len(failures)} 个用例失败：{failures}")
    sys.exit(1)
print("全部用例通过")
