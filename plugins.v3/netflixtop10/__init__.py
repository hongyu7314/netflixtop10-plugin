"""
网飞排行榜 - MoviePilot V3 插件
基于 Netflix 官方 Tudum Top 10 周榜数据（全球/国家维度、剧集/电影、英语/非英语分类），
TMDB 识别媒体信息，支持一键订阅、新上榜提醒与自动订阅。

数据来源（Netflix 官方公开 TSV，每周更新）：
- https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv
- https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv
"""

import hashlib
import random
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from apscheduler.triggers.interval import IntervalTrigger
from fastapi import Body

from app.chain.subscribe import SubscribeChain
from app.db.oper.mediaserver import MediaServerOper
from app.db.oper.subscribe import SubscribeOper
from app.db.oper.transferhistory import TransferHistoryOper
from app.modules.themoviedb.tmdbapi import TmdbApi
from app.plugins import _PluginBase
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.network import RequestUtils
from app.schemas.types import EventType, MediaType

from .scraper import (
    CATEGORY_MAP,
    CATEGORY_ORDER,
    CATEGORY_ZH,
    COMMON_COUNTRIES,
    country_display_name,
    extract_latest,
    format_count,
)

try:
    from app.schemas.types import MessageType
except ImportError:  # 兼容旧版本
    from app.schemas import NotificationType as MessageType  # type: ignore


def _create_meta_info(title: str, mtype: MediaType):
    """创建 MoviePilot MetaInfo，兼容 V2/V3。

    V3 同时存在两个 MetaInfo：
      - app.schemas.MetaInfo: pydantic model，字段名 name/type，全部默认 None，可无参构造
      - app.sdk.media.MetaInfo: 函数，必传位置参数 title，字段名 title/mtype（用于解析文件名/种子名）
    优先使用 pydantic 版（与 MaoyanDianYing 等参考插件一致），失败兜底用函数版。
    """
    meta = None
    last_err: Exception | None = None
    # 优先 V3 pydantic 版（字段名 name= / type= 兼容老代码）
    try:
        from app.schemas import MetaInfo as _MetaInfo
        meta = _MetaInfo(name=title, type=mtype)
    except Exception as e:
        last_err = e
        # 兜底 V3 函数版（必传 title=，字段 mtype=）
        try:
            from app.sdk.media import MetaInfo as _MetaInfo
            meta = _MetaInfo(title=title, mtype=mtype)
        except Exception as e2:
            last_err = e2
            logger.warning("创建 MoviePilot MetaInfo 失败: %s", last_err)
            return None
    # 兜底 setattr，确保 name/title/type 都设上（不同版本字段名不同）
    for key, value in {
        "name": title,
        "title": title,
        "original_name": title,
        "type": mtype,
    }.items():
        try:
            setattr(meta, key, value)
        except Exception:
            try:
                object.__setattr__(meta, key, value)
            except Exception:
                pass
    return meta


# Netflix 榜单名里常见的副标题后缀：Season X / Part X / Chapter X / Vol X / Book X / (YYYY)
_NETFLIX_SUFFIX_RE = re.compile(
    r"\s*[,:]\s*(?:Season|Part|Chapter|Vol\.?|Volume|Book|Series|Film|Movie|Collection)\s+\d+\b.*$",
    re.IGNORECASE,
)
_NETFLIX_YEAR_RE = re.compile(r"\s*\(\d{4}\)\s*$")


def _clean_netflix_title(title: str) -> str:
    """剥离 Netflix 榜单标题中的季/部/年份后缀，得到更利于 TMDB 搜索的干净名字。"""
    if not title:
        return ""
    cleaned = _NETFLIX_SUFFIX_RE.sub("", title)
    cleaned = _NETFLIX_YEAR_RE.sub("", cleaned)
    return cleaned.strip()


def _normalize_title_for_compare(t: str) -> str:
    """忽略大小写/空白/常见标点，用于"完全一致"匹配。"""
    if not t:
        return ""
    # 去掉所有非字母数字字符，转小写；中文等非拉丁字符保留
    return re.sub(r"[\s\-_:'\",.!?()\[\]{}&/\\|+@#$%^*~`]", "", t).lower()


_TMDB_LOOSE_RECENT_DAYS = 730  # 宽松兜底时判定"近期发行"的窗口：Netflix 周榜以新片为主


def _as_float(value: Any) -> float:
    """把 TMDB 返回的数值字段安全转成 float。"""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _item_release_date(item: dict) -> Optional[datetime]:
    """取出 TMDB 搜索结果的上映/首播日期，缺失或非法时返回 None。"""
    raw = str(item.get("release_date") or item.get("first_air_date") or "")[:10]
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d")
    except ValueError:
        return None


def _pick_best_tmdb_match(
    title: str, items: List[dict], media_type: MediaType, strict: bool = False
) -> int:
    """从 TMDB 搜索结果里挑最可能对应 Netflix 条目的那一个 id。

    优先级：
      1. original_title / original_name 与输入完全一致（忽略大小写/标点）
      2. title / name 完全一致
      3. original_title 与输入互为子串（取长度最接近的一条）
      4. title / name 与输入互为子串（取长度最接近的一条）
      5. 宽松兜底：优先 TMDB 相关性第一条；若存在"近期发行且热度更高"的候选则替换

    strict=True 时只做 1~4 的"标题确实对得上"判断，不做第 5 步兜底，
    避免把毫不相关的热门结果当成目标（例如 "The Gentlemen" 命中 "The Gentlemen's Hentai Club"）。

    第 5 步的"近期 + 更热门"替换规则，针对的是这类真实错误：
      "The Secret Woman" 的相关性第一条是 2015 年的 Shakespeare's Mother，
      而正确的《她的秘密人生》(2026-08-28) 排在第二条且热度高出一个量级。
    """
    if not items:
        return 0
    title_field = "title" if media_type == MediaType.MOVIE else "name"
    original_field = "original_title" if media_type == MediaType.MOVIE else "original_name"
    needle = _normalize_title_for_compare(title)
    if not needle:
        return 0

    # 1) original 完全匹配
    for it in items:
        if _normalize_title_for_compare(it.get(original_field) or "") == needle:
            return int(it.get("id") or 0)
    # 2) 本地化 title 完全匹配
    for it in items:
        if _normalize_title_for_compare(it.get(title_field) or "") == needle:
            return int(it.get("id") or 0)

    def substring_hit(field: str) -> int:
        """互为子串的候选里取长度最接近的，避免 "Graveyard" 被 "Graveyard Carz" 抢先命中。"""
        best_diff: Optional[int] = None
        best_id = 0
        for it in items:
            cand = _normalize_title_for_compare(it.get(field) or "")
            if not cand or not (needle in cand or cand in needle):
                continue
            diff = abs(len(cand) - len(needle))
            if best_diff is None or diff < best_diff:
                best_diff, best_id = diff, int(it.get("id") or 0)
        return best_id

    # 3) original 互为子串（处理 Netflix 加了 ": Season 2" 等副标题）
    hit = substring_hit(original_field)
    if hit:
        return hit
    # 4) 本地化 title 互为子串
    hit = substring_hit(title_field)
    if hit:
        return hit
    if strict:
        return 0

    # 5) 宽松兜底：TMDB 相关性第一条为基准，只被"更近期发行且更热门"的候选顶替
    pool = [it for it in items if it.get("poster_path")] or list(items)
    best = pool[0]
    best_pop = _as_float(best.get("popularity"))
    now = datetime.now()
    for it in pool[1:]:
        released = _item_release_date(it)
        if not released or (now - released).days > _TMDB_LOOSE_RECENT_DAYS:
            continue
        pop = _as_float(it.get("popularity"))
        if pop > best_pop:
            best, best_pop = it, pop
    return int(best.get("id") or 0)


class TmdbHelper:
    """TMDB 数据辅助器（使用 MoviePilot 内置 TMDB API）。"""

    @staticmethod
    def api() -> TmdbApi:
        """构造 TmdbApi，兼容不同版本的构造签名。"""
        try:
            return TmdbApi(language="zh")
        except TypeError:
            return TmdbApi()

    @staticmethod
    def get_poster_url(poster_path: str) -> str:
        """将 TMDB 海报相对路径转换为 MP 代理 URL；空路径返回空字符串。"""
        if not poster_path:
            return ""
        if poster_path.startswith("http"):
            if "image.tmdb.org" in poster_path:
                return f"/api/v1/system/img/1?imgurl={poster_path}"
            return poster_path
        return f"/api/v1/system/img/1?imgurl=https://image.tmdb.org/t/p/w500{poster_path}"


class NetflixTop10(_PluginBase):
    """网飞排行榜插件主类：抓取 Netflix 官方周榜 + TMDB 识别 + 一键订阅 + 新上榜提醒。"""

    plugin_name = "网飞排行榜"
    plugin_desc = (
        "Netflix（Tudum Top 10）官方周榜订阅：全球/中国香港/美国/韩国/日本五榜一键切换，"
        "剧集/电影四分类，TMDB 识别，海报墙一键订阅，新上榜提醒，支持自动订阅。"
    )
    plugin_icon = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/08/Netflix_2015_logo.svg/512px-Netflix_2015_logo.svg.png"
    plugin_version = "1.1.1"
    plugin_author = "hongyu7314"
    author_url = "https://github.com/hongyu7314"
    plugin_config_prefix = "netflixtop10_"
    plugin_order = 51
    auth_level = 1

    _enabled = False
    _cache_key = "netflixtop10_rank"
    _countries_key = "netflixtop10_countries"
    _prev_key = "netflixtop10_prev"
    _notified_key = "netflixtop10_notified"
    _refresh_interval = 6
    _fetch_lock = threading.Lock()
    # 当前页面展示的地区 scope（独立于 plugin_config["rank_scope"]，
    # 便于详情页快速切换而无需写入表单配置；详见 switch_region 接口）。
    _view_scope = "global"

    # ─── 快捷地区预设（详情页顶部一排按钮）───
    # value 为插件内统一 scope 标识：
    #   "global"   = 全球榜（使用 all-weeks-global.tsv）
    #   其余字符串 = 国家/地区榜（使用 all-weeks-countries.tsv，按 country_match 模糊匹配）
    PRESET_REGIONS: List[Dict[str, str]] = [
        {"value": "global",        "label": "全球",   "icon": "mdi-earth"},
        {"value": "Hong Kong",     "label": "中国香港", "icon": "mdi-flag-variant"},
        {"value": "United States", "label": "美国",   "icon": "mdi-flag-variant"},
        {"value": "South Korea",   "label": "韩国",   "icon": "mdi-flag-variant"},
        {"value": "Japan",         "label": "日本",   "icon": "mdi-flag-variant"},
    ]

    _tmdb_cache_ttl = 7 * 86400       # TMDB 搜索缓存 7 天
    _detail_cache_ttl = 7 * 86400     # TMDB 详情缓存 7 天
    _status_cache_ttl = 1800          # 订阅/入库状态缓存 30 分钟
    _countries_cache_ttl = 7 * 86400  # 国家列表缓存 7 天

    # 数据源候选（按优先级依次尝试）
    GLOBAL_URLS = [
        "https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv",
        "https://www.netflix.com/tudum/top10/all-weeks-global.tsv",
        "https://top10.netflix.com/data/all-weeks-global.tsv",
    ]
    COUNTRIES_URLS = [
        "https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv",
        "https://www.netflix.com/tudum/top10/all-weeks-countries.tsv",
        "https://top10.netflix.com/data/all-weeks-countries.tsv",
    ]
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/plain,text/csv,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.netflix.com/tudum/top10",
    }

    # ─── 生命周期 ───

    def init_plugin(self, config: dict | None = None) -> None:
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._rank_scope = str(config.get("rank_scope", "global") or "global").strip() or "global"
        # 恢复详情页上次切换的地区 scope（独立于表单配置）
        self._view_scope = self._rank_scope
        try:
            cached_view = self.get_data("netflixtop10_view_scope")
            if isinstance(cached_view, dict) and cached_view.get("scope"):
                self._view_scope = str(cached_view["scope"]).strip() or self._rank_scope
        except Exception:
            pass
        cats = config.get("media_categories")
        self._media_categories = [c for c in (cats or []) if c in CATEGORY_MAP] or list(CATEGORY_ORDER)
        self._refresh_interval = int(config.get("refresh_interval", 6) or 6)
        self._reminder_enabled = bool(config.get("reminder_enabled", False))
        self._reminder_msgtype = str(config.get("reminder_msgtype", "Plugin") or "Plugin")
        self._auto_subscribe = bool(config.get("auto_subscribe", False))
        self._subscribe_oper = SubscribeOper()
        self._media_oper = MediaServerOper()
        self._transfer_oper = TransferHistoryOper()
        logger.info(
            "【网飞排行】插件初始化：enabled=%s, scope=%s, view_scope=%s, categories=%s, interval=%sh",
            self._enabled, self._rank_scope, self._view_scope,
            self._media_categories, self._refresh_interval,
        )

        if config.get("run_once_flag"):
            threading.Thread(
                target=self._run_refresh_safe, name="NetflixTop10.RunOnce", daemon=True
            ).start()
            self.update_config({**config, "run_once_flag": False})

        if self._enabled:
            cached = self.get_data(self._cache_key)
            # 如果上次详情页选定的地区跟缓存 scope 不一致（容器重启后常见），
            # 启动时主动抓一次 view_scope 的数据
            target_scope = self._view_scope
            scope_mismatch = (not cached or not isinstance(cached, dict)
                              or not cached.get("rows")
                              or cached.get("scope") != target_scope)
            if scope_mismatch:
                threading.Thread(
                    target=self._run_refresh_safe,
                    kwargs={"notify": False, "scope": target_scope},
                    name="NetflixTop10.InitialRefresh",
                    daemon=True,
                ).start()
            # 后台刷新国家列表（低频，7 天一次）
            threading.Thread(
                target=self._refresh_country_list, name="NetflixTop10.CountryList", daemon=True
            ).start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> list[dict[str, Any]]:
        return []

    def get_service(self) -> list[dict]:
        """插件启用时注册周期刷新任务。"""
        if not self.get_state():
            return []
        return [
            {
                "id": "NetflixTop10.AutoRefresh",
                "name": "网飞排行榜自动刷新",
                "trigger": IntervalTrigger(hours=self._refresh_interval),
                "func": self._run_refresh_safe,
                "kwargs": {},
            }
        ]

    def stop_service(self) -> None:
        self._enabled = False
        logger.info("【网飞排行】插件已停止")

    # ─── API 注册 ───

    def get_api(self) -> list[dict[str, Any]]:
        return [
            {
                "path": "/subscribe",
                "endpoint": self.add_subscribe,
                "methods": ["GET"],
                "summary": "添加订阅",
            },
            {
                "path": "/run-once",
                "endpoint": self.run_once,
                "methods": ["GET"],
                "summary": "立即抓取一次",
            },
            {
                "path": "/get-cache",
                "endpoint": self.get_cache,
                "methods": ["GET"],
                "summary": "获取缓存数据",
            },
            {
                "path": "/clear-cache",
                "endpoint": self.clear_cache,
                "methods": ["GET"],
                "summary": "清理插件缓存",
            },
            {
                "path": "/switch-region",
                "endpoint": self.switch_region,
                "methods": ["GET"],
                "summary": "切换详情页展示的地区榜单（全球/中国香港/美国/韩国/日本）",
            },
        ]

    # ─── 配置表单 ───

    def get_form(self) -> tuple[list[dict], dict[str, Any]]:
        # 榜单范围：全球 + 已缓存的国家列表（首次启用后自动获取）
        scope_items = [{"title": "全球", "value": "global"}]
        try:
            cached = self.get_data(self._countries_key)
            if cached and isinstance(cached, dict) and cached.get("countries"):
                for c in cached["countries"]:
                    scope_items.append({"title": country_display_name(c), "value": c})
        except Exception:
            pass

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSwitch", "props": {"model": "reminder_enabled", "label": "新上榜提醒"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSwitch", "props": {"model": "auto_subscribe", "label": "自动订阅新上榜"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "rank_scope",
                                    "label": "榜单范围",
                                    "variant": "outlined", "density": "compact",
                                    "items": scope_items,
                                }}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "refresh_interval",
                                    "label": "自动刷新间隔（小时）",
                                    "variant": "outlined", "density": "compact",
                                    "items": [
                                        {"title": "1小时", "value": 1},
                                        {"title": "2小时", "value": 2},
                                        {"title": "3小时", "value": 3},
                                        {"title": "6小时", "value": 6},
                                        {"title": "12小时", "value": 12},
                                        {"title": "24小时", "value": 24},
                                    ],
                                }}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "reminder_msgtype",
                                    "label": "消息类型",
                                    "variant": "outlined", "density": "compact",
                                    "items": [{"title": item.value, "value": item.name} for item in MessageType],
                                }}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "media_categories",
                                    "label": "榜单分类",
                                    "multiple": True, "chips": True,
                                    "variant": "outlined", "density": "compact",
                                    "items": [
                                        {"title": CATEGORY_ZH["tv_en"], "value": "tv_en"},
                                        {"title": CATEGORY_ZH["tv_nonen"], "value": "tv_nonen"},
                                        {"title": CATEGORY_ZH["film_en"], "value": "film_en"},
                                        {"title": CATEGORY_ZH["film_nonen"], "value": "film_nonen"},
                                    ],
                                }}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "run_once_flag", "label": "保存后立即运行一次"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VAlert", "props": {
                                    "type": "info", "variant": "tonal", "density": "compact", "class": "mt-2",
                                }, "text": (
                                    "数据来自 Netflix 官方 Tudum Top 10 周榜（每周二更新）；开启「新上榜提醒」后，"
                                    "每周榜单更新时推送新上榜片目并标注订阅状态；开启「自动订阅新上榜」会对新上榜且未订阅/未入库的片目自动添加订阅。"
                                    "「榜单范围」仅决定默认抓取的地区；详情页顶部支持 5 个快捷地区一键切换（全球/中国香港/美国/韩国/日本），"
                                    "切换后立即触发后台抓取（约 1-2 分钟）。"
                                    "国家榜单需下载较大数据文件，如获取失败请检查网络（可能需要代理）。"
                                )}]},
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "rank_scope": "global",
            "media_categories": list(CATEGORY_ORDER),
            "refresh_interval": 6,
            "reminder_enabled": False,
            "reminder_msgtype": "Plugin",
            "auto_subscribe": False,
            "run_once_flag": False,
        }

    # ─── 详情页 ───

    def get_page(self) -> list[dict]:
        cached = self.get_data(self._cache_key)
        if not self.get_state():
            return [self._alert("插件未启用，请先在设置中启用。", "info")]
        if not cached or not isinstance(cached, dict) or not cached.get("rows"):
            return [self._alert("暂无数据，首次启用会自动抓取，请稍后刷新页面；也可在设置中打开「保存后立即运行一次」。", "info")]

        # ─── 当前页面 scope 与缓存 scope 解耦 ───
        # 详情页支持 5 个快捷地区（全球/中国香港/美国/韩国/日本）。
        # 缓存里的 scope 可能还是旧地区：若 view_scope 与 cache.scope 不同，
        # 给出"切换中"提示并自动后台抓取新地区；不等抓取完成，先渲染旧缓存避免页面空白。
        view_scope = getattr(self, "_view_scope", "global") or "global"
        cache_scope = cached.get("scope") or "global"
        cache_mismatched = view_scope != cache_scope

        rows = cached.get("rows", [])
        scope_text = "全球" if cache_scope == "global" else country_display_name(cache_scope)
        view_text = "全球" if view_scope == "global" else country_display_name(view_scope)
        header_text = (
            f"榜单范围：{scope_text} ｜ 数据周期：{cached.get('week', '未知')} ｜ "
            f"共 {len(rows)} 条 ｜ 最后刷新：{cached.get('update_time', '未知')}"
        )

        contents: list[dict] = []

        # ① 快捷地区切换条（5 个 toggle 按钮）
        contents.append(self._render_region_switcher(view_scope))

        # ② 顶部信息条 + 操作按钮
        contents.append({
            "component": "VRow",
            "props": {"class": "align-center mb-2", "no-gutters": True},
            "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                    {"component": "VAlert",
                     "props": {"type": "info" if cache_mismatched else "success",
                               "variant": "tonal", "density": "compact"},
                     "text": (
                         header_text if not cache_mismatched else
                         f"已切换至「{view_text}」榜，正在后台抓取（约 1-2 分钟），"
                         f"抓取完成前展示上次缓存：{scope_text} {cached.get('week', '')}"
                     )}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "div", "props": {"class": "d-flex justify-end ga-2"},
                     "content": [
                         {"component": "VBtn",
                          "props": {"size": "small", "color": "primary", "variant": "tonal",
                                    "text": "立即刷新"},
                          "events": {"click": {"api": "plugin/NetflixTop10/run-once",
                                               "method": "get",
                                               "params": {"apikey": settings.API_TOKEN}}}},
                         {"component": "VBtn",
                          "props": {"size": "small", "color": "warning", "variant": "tonal",
                                    "text": "清理缓存"},
                          "events": {"click": {"api": "plugin/NetflixTop10/clear-cache",
                                               "method": "get",
                                               "params": {"apikey": settings.API_TOKEN}}}},
                     ]}]},
            ],
        })

        # ③ 每个分类一张海报墙
        for cat in CATEGORY_ORDER:
            cat_rows = [r for r in rows if r.get("category") == cat]
            if not cat_rows:
                continue
            contents.append(self._render_category_section(cat, cat_rows, cached.get("week", "")))

        return contents

    def _render_region_switcher(self, current: str) -> dict:
        """渲染详情页顶部 5 个快捷地区按钮。"""
        btn_items: list[dict] = []
        for region in self.PRESET_REGIONS:
            value = region["value"]
            is_active = (value == current)
            btn_items.append({
                "component": "VBtn",
                "props": {
                    "size": "small",
                    "variant": "elevated" if is_active else "tonal",
                    "color": "primary" if is_active else "default",
                    "class": "ma-1",
                    "prepend-icon": region["icon"],
                    "text": region["label"],
                },
                "events": ({"click": {
                    "api": "plugin/NetflixTop10/switch-region",
                    "method": "get",
                    "params": {"scope": value, "apikey": settings.API_TOKEN},
                }} if not is_active else {}),
            })
        return {
            "component": "VCard",
            "props": {"class": "mb-3", "variant": "outlined"},
            "content": [
                {"component": "VCardText", "props": {"class": "pa-3"}, "content": [
                    {"component": "div", "props": {"class": "d-flex align-center flex-wrap ga-1"},
                     "content": [
                         {"component": "span",
                          "props": {"class": "text-subtitle-2 mr-3 font-weight-bold"},
                          "text": "切换榜单地区："},
                         *btn_items,
                     ]},
                ]},
            ],
        }

    def _render_category_section(self, cat: str, cat_rows: list, week: str) -> dict:
        """渲染单个分类的海报墙（响应式 6/4/3/2 列网格）。"""
        cards: list[dict] = []
        for item in cat_rows:
            cards.append({
                "component": "VCol",
                "props": {"cols": 6, "sm": 4, "md": 3, "lg": 2},
                "content": [self._render_poster_card(item)],
            })

        return {
            "component": "VCard",
            "props": {"class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "d-flex align-center text-subtitle-1 pb-2"},
                 "content": [
                     {"component": "VIcon", "props": {"size": "small", "class": "mr-2"},
                      "text": "mdi-fire"},
                     {"component": "span", "text": f"{CATEGORY_ZH.get(cat, cat)} · {week}"},
                     {"component": "VSpacer"},
                     {"component": "VChip", "props": {"size": "x-small", "color": "primary",
                                                      "variant": "tonal"},
                      "text": f"TOP {len(cat_rows)}"},
                 ]},
                {"component": "VDivider"},
                {"component": "VCardText", "props": {"class": "pa-3"}, "content": [
                    {"component": "VRow", "props": {"no-gutters": True, "dense": True},
                     "content": cards},
                ]},
            ],
        }

    def _render_poster_card(self, item: dict) -> dict:
        """渲染单个海报卡片：海报 + 状态徽章 + 标题 + 订阅按钮。

        卡片整体是 <a href="#/media?...">，点击即在 SPA 内跳转到 MoviePilot 系统详情页；
        下方订阅按钮 stop-propagation 防止点击穿透。
        """
        tmdbid = item.get("tmdbid") or 0
        name = item.get("name", "")
        zh_name = item.get("zh_name") or ""
        display_name = zh_name or name
        mtype = item.get("mtype", "TV")
        year = item.get("year", "")
        season = item.get("season", "")

        # 计算订阅/入库状态（缓存 30 分钟）
        status = item.get("status") or self._check_media_status(tmdbid, name, mtype)
        item["status"] = status

        poster = item.get("poster") or ""
        rank = item.get("rank", 0)
        weeks = item.get("weeks_in_top10", 0)
        views = item.get("views") or 0
        hours = item.get("hours_viewed") or 0

        # 状态颜色 + 文字
        status_color_map = {
            "影片已入库": "success",
            "订阅已添加": "info",
            "未添加订阅": "default",
        }
        status_color = status_color_map.get(status, "default")

        # 副标题：原名（如果有中文）/ 年份 / 季 / 上榜周数
        sub_parts: list[str] = []
        if zh_name and name and zh_name != name:
            sub_parts.append(name)
        if year:
            sub_parts.append(str(year))
        if season:
            sub_parts.append(season)
        if weeks:
            sub_parts.append(f"上榜{weeks}周")
        sub_line = " · ".join(sub_parts)

        # 观看数据
        if views:
            views_line = f"周观看 {format_count(views)} 次"
        elif hours:
            views_line = f"周观看 {format_count(hours)} 小时"
        else:
            views_line = ""

        # 系统详情页 URL（仅当有 TMDB ID 时可点击）
        media_url = ""
        if tmdbid:
            media_url = (
                f"#/media?media_source=themoviedb&media_id={tmdbid}"
                f"&title={quote(display_name)}"
                f"&year={year or ''}"
                f"&type={'电影' if mtype == 'MOVIE' else '电视剧'}"
            )

        # 海报区（160x240，与猫眼海报墙一致）
        poster_inner: list[dict] = []
        if poster:
            poster_inner.append({
                "component": "VImg",
                "props": {
                    "src": poster,
                    "max-width": "160",
                    "max-height": "240",
                    "aspect-ratio": "2/3",
                    "cover": True,
                    "class": "rounded netflix-poster-img",
                    "style": "height:auto; width:100%;",
                },
            })
        else:
            poster_inner.append({
                "component": "div",
                "props": {
                    "class": "rounded bg-grey-lighten-2 d-flex align-center justify-center",
                    "style": "aspect-ratio:2/3; width:100%;",
                },
                "content": [
                    {"component": "VIcon", "props": {"size": "large", "color": "grey"},
                     "text": "mdi-image-off-outline"},
                ],
            })
        # 排名标签（左上角）
        poster_inner.insert(0, {
            "component": "VChip",
            "props": {
                "size": "x-small",
                "color": "primary",
                "variant": "elevated",
                "class": "netflix-rank-chip",
                "style": "position:absolute; top:6px; left:6px; z-index:2;",
            },
            "text": f"No.{rank}",
        })
        # 状态徽章（底部）
        poster_inner.append({
            "component": "div",
            "props": {
                "class": f"netflix-status-bar text-white text-center",
                "style": (
                    "position:absolute; bottom:0; left:0; right:0;"
                    f" background:var(--v-theme-{status_color}, #555);"
                    " font-size:10px; padding:2px 0;"
                    " border-bottom-left-radius:4px; border-bottom-right-radius:4px;"
                ),
            },
            "text": status,
        })

        # 卡片信息区：标题 + 副标题 + 数据 + 订阅按钮
        info_content: list[dict] = [
            {"component": "div",
             "props": {"class": "text-body-2 font-weight-bold mt-2 text-truncate",
                       "style": "line-height:1.3;"},
             "text": display_name or name},
        ]
        if sub_line:
            info_content.append({
                "component": "div",
                "props": {"class": "text-caption text-medium-emphasis text-truncate"},
                "text": sub_line,
            })
        if views_line:
            info_content.append({
                "component": "div",
                "props": {"class": "text-caption text-medium-emphasis"},
                "text": views_line,
            })

        # 订阅按钮：未订阅且有 TMDB ID 时可点击
        can_sub = (status == "未添加订阅" and tmdbid)
        sub_btn = {
            "component": "VBtn",
            "props": {
                "size": "x-small",
                "color": "primary" if can_sub else "default",
                "variant": "elevated" if can_sub else "tonal",
                "block": True,
                "class": "mt-2",
                "text": "订阅" if can_sub else status,
                "disabled": not can_sub,
            },
        }
        if can_sub:
            sub_btn["events"] = {"click": {
                "api": "plugin/NetflixTop10/subscribe",
                "method": "get",
                "params": {
                    "tmdbid": tmdbid,
                    "name": name,
                    "mtype": mtype,
                    "apikey": settings.API_TOKEN,
                },
            }}
        info_content.append(sub_btn)

        # 整个卡片包成 <a> 点击进系统详情（如果有 TMDB ID），否则普通 VCard
        inner = {
            "component": "VCard",
            "props": {
                "variant": "outlined",
                "rounded": "lg",
                "class": "netflix-poster-card h-100",
                "style": "cursor:pointer; transition: transform .15s, box-shadow .15s;",
            },
            "content": [
                # 海报区域（用 div 包住以便 absolute 定位徽章）
                {"component": "div",
                 "props": {"style": "position:relative;", "class": "netflix-poster-wrap"},
                 "content": poster_inner},
                # 文字 + 按钮区
                {"component": "VCardText",
                 "props": {"class": "pa-2 pt-1"},
                 "content": info_content},
            ],
        }
        if media_url:
            # 用 <a> 包整个卡片 → SPA 内跳系统详情
            return {
                "component": "a",
                "props": {
                    "href": media_url,
                    "rel": "noopener noreferrer",
                    "class": "d-block text-decoration-none text-high-emphasis netflix-poster-link",
                    "style": "color:inherit;",
                },
                "content": [inner],
            }
        return inner

    def _render_item_row(self, item: dict) -> dict:
        """渲染单个榜单条目行（保留旧版紧凑行模式，供历史兼容/未来调试使用）。"""
        status = item.get("status") or self._check_media_status(
            item.get("tmdbid", 0), item.get("name", ""), item.get("mtype", "TV")
        )
        item["status"] = status

        name = item.get("name", "")
        display_name = item.get("zh_name") or name
        sub_parts = []
        if item.get("zh_name") and item["zh_name"] != name:
            sub_parts.append(name)
        if item.get("year"):
            sub_parts.append(str(item["year"]))
        if item.get("season"):
            sub_parts.append(item["season"])
        if item.get("weeks_in_top10"):
            sub_parts.append(f"上榜{item['weeks_in_top10']}周")
        sub_line = " ｜ ".join(str(p) for p in sub_parts if p)

        views_text = ""
        if item.get("views"):
            views_text = f"观看\n{format_count(item['views'])}"
        elif item.get("hours_viewed"):
            views_text = f"时长\n{format_count(item['hours_viewed'])}小时"

        poster = item.get("poster") or ""
        poster_col = {"component": "VCol", "props": {"cols": "auto"}, "content": [
            {"component": "VImg", "props": {
                "src": poster, "width": 40, "height": 60, "cover": True,
                "class": "rounded",
            }} if poster else {"component": "div", "props": {
                "style": "width:40px;height:60px;", "class": "rounded bg-grey-lighten-2"}}]}

        status_color = {"影片已入库": "success", "订阅已添加": "info"}.get(status, "default")
        status_chip = {"component": "VChip", "props": {
            "size": "x-small", "variant": "tonal", "color": status_color,
        }, "text": status}

        can_sub = status == "未添加订阅" and item.get("tmdbid")
        sub_btn = {"component": "VBtn", "props": {
            "size": "x-small", "color": "primary", "variant": "tonal",
            "text": "订阅", "disabled": not can_sub,
        }}
        if can_sub:
            sub_btn["events"] = {"click": {
                "api": "plugin/NetflixTop10/subscribe",
                "method": "get",
                "params": {
                    "tmdbid": item.get("tmdbid"),
                    "name": name,
                    "mtype": item.get("mtype", "TV"),
                    "apikey": settings.API_TOKEN,
                },
            }}

        return {
            "component": "VRow",
            "props": {"class": "align-center py-1 border-b", "no-gutters": True},
            "content": [
                {"component": "VCol", "props": {"cols": "auto"}, "content": [
                    {"component": "div", "text": str(item.get("rank", "")),
                     "props": {"class": "text-h6 text-medium-emphasis px-2",
                               "style": "min-width:32px;text-align:center;"}}]},
                poster_col,
                {"component": "VCol", "props": {"cols": True, "class": "px-2"}, "content": [
                    {"component": "div", "text": display_name,
                     "props": {"class": "font-weight-medium text-truncate"}},
                    {"component": "div", "text": sub_line,
                     "props": {"class": "text-caption text-medium-emphasis text-truncate"}},
                ]},
                {"component": "VCol", "props": {"cols": "auto"}, "content": [
                    {"component": "div", "text": views_text.replace("\n", " "),
                     "props": {"class": "text-caption text-center px-2", "style": "min-width:86px;"}}]},
                {"component": "VCol", "props": {"cols": "auto", "class": "px-2"}, "content": [
                    {"component": "div", "props": {"class": "d-flex align-center ga-1"},
                     "content": [status_chip, sub_btn]}]},
            ],
        }

    @staticmethod
    def _alert(text: str, type_: str = "info") -> dict:
        return {
            "component": "VRow",
            "content": [{"component": "VCol", "props": {"cols": 12}, "content": [
                {"component": "VAlert", "props": {"type": type_, "variant": "tonal", "density": "compact"}, "text": text}]}],
        }

    # ─── 数据抓取 ───

    def _fetch_tsv(self, urls: List[str]) -> str:
        """依次尝试候选 URL（先直连后代理）抓取 TSV 文本。"""
        proxies = self._get_proxies()
        attempts: List[Optional[dict]] = [None]
        if proxies:
            attempts.append(proxies)
        last_err = ""
        for proxy in attempts:
            for url in urls:
                try:
                    resp = RequestUtils(headers=self.HEADERS, timeout=60, proxies=proxy).get_res(url)
                    if resp is not None and resp.ok:
                        text = (resp.text or "").strip()
                        if text and not text.lstrip()[:1] == "<" and "show_title" in text[:2000]:
                            return text
                        last_err = f"{url} 返回内容异常"
                    elif resp is not None:
                        last_err = f"{url} HTTP {resp.status_code}"
                    else:
                        last_err = f"{url} 请求失败"
                except Exception as e:
                    last_err = f"{url} {e}"
                logger.debug("【网飞排行】数据源尝试失败：%s", last_err)
        raise ConnectionError(
            f"Netflix 官方数据源请求失败（{last_err}）。"
            "Netflix 可能限制了当前出口 IP，家庭宽带网络通常可正常访问；如在受限网络环境请为 MoviePilot 配置代理后重试。"
        )

    @staticmethod
    def _get_proxies() -> Optional[dict]:
        """读取宿主代理配置（兼容 str/dict）。"""
        try:
            proxy = getattr(settings, "PROXY", None)
            if not proxy:
                return None
            if isinstance(proxy, dict):
                return proxy
            if isinstance(proxy, str):
                return {"http": proxy, "https": proxy}
        except Exception:
            pass
        return None

    def _run_refresh_safe(self, notify: bool = True, scope: Optional[str] = None):
        """带锁的刷新入口（线程安全）。scope=None 用 self._rank_scope。"""
        with self._fetch_lock:
            try:
                self._auto_refresh(notify=notify, scope=scope)
            except Exception as e:
                logger.error("【网飞排行】刷新失败: %s", e)

    def _auto_refresh(self, notify: bool = True, scope: Optional[str] = None):
        """核心刷新：抓取榜单 -> TMDB 识别 -> 更新缓存 -> 新上榜通知。

        :param scope: 临时覆盖榜单范围（None 表示用 self._rank_scope）。
        """
        scope = (scope or self._rank_scope or "global").strip() or "global"
        logger.info("【网飞排行】开始刷新，scope=%s", scope)
        country = None if scope == "global" else scope
        urls = self.GLOBAL_URLS if country is None else self.COUNTRIES_URLS

        text = self._fetch_tsv(urls)
        items, countries = extract_latest(text, country=country, categories=self._media_categories)
        if not items:
            raise ValueError("未解析到榜单数据（请检查榜单范围/分类配置，或数据源格式变化）")

        # 缓存国家列表（供配置表单使用）
        if countries:
            self.save_data(self._countries_key, {"countries": countries, "ts": time.time()})

        # 与旧缓存合并：已识别的 TMDB 信息直接复用（仅当 scope 一致时）
        cached = self.get_data(self._cache_key)
        existing: Dict[str, dict] = {}
        if cached and isinstance(cached, dict) and cached.get("scope") == scope:
            for old in cached.get("rows", []):
                key = f"{old.get('category', '')}::{old.get('name', '')}"
                if old.get("tmdbid"):
                    existing[key] = old

        enriched = []
        new_tmdb_count = 0
        for item in items:
            key = f"{item['category']}::{item['name']}"
            old = existing.get(key)
            if old:
                for field in ("tmdbid", "poster", "year", "zh_name"):
                    if old.get(field):
                        item[field] = old[field]
            if not item.get("tmdbid"):
                tmdb_info = self._search_tmdb_with_cache(item["name"], item["mtype"])
                if tmdb_info:
                    item["tmdbid"] = tmdb_info.get("id") or 0
                    item["poster"] = TmdbHelper.get_poster_url(tmdb_info.get("poster_path") or "")
                    item["zh_name"] = tmdb_info.get("zh_name") or ""
                    item["year"] = tmdb_info.get("year") or ""
                new_tmdb_count += 1
                time.sleep(0.15)
            enriched.append(item)

        week = enriched[0].get("week", "") if enriched else ""
        result = {
            "scope": scope,
            "week": week,
            "rows": enriched,
            "total": len(enriched),
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": time.time(),
        }
        self.save_data(self._cache_key, result)
        logger.info("【网飞排行】刷新完成，scope=%s 周期 %s 共 %d 条（新识别 TMDB %d 条）",
                    scope, week, len(enriched), new_tmdb_count)

        # 刷新成功后同步更新 view_scope，使详情页立刻与缓存一致
        self._view_scope = scope
        self.save_data("netflixtop10_view_scope", {"scope": scope, "ts": time.time()})

        if notify:
            self._notify_new_entries(enriched, scope, week)

    def _refresh_country_list(self):
        """后台刷新国家列表（7 天一次）。"""
        try:
            cached = self.get_data(self._countries_key)
            if cached and isinstance(cached, dict) and cached.get("countries"):
                if time.time() - (cached.get("ts") or 0) < self._countries_cache_ttl:
                    return
            text = self._fetch_tsv(self.COUNTRIES_URLS)
            _, countries = extract_latest(text, country=None, categories=list(CATEGORY_ORDER))
            if countries:
                self.save_data(self._countries_key, {"countries": countries, "ts": time.time()})
                logger.info("【网飞排行】国家列表已更新，共 %d 个市场", len(countries))
        except Exception as e:
            logger.warning("【网飞排行】国家列表刷新失败：%s", e)

    # ─── TMDB 识别 ───

    @staticmethod
    def _tmdb_cache_key(title: str, mtype: str) -> str:
        # 命名空间带版本号：识别策略变更时 bump，可一次性作废历史错误缓存
        md5 = hashlib.md5(f"{mtype}::{title}".encode("utf-8")).hexdigest()[:12]
        return f"netflixtop10_tmdb_v2_{md5}"

    def _get_cached(self, key: str, ttl: int) -> Optional[Any]:
        try:
            cached = self.get_data(key)
            if cached and isinstance(cached, dict):
                if time.time() - (cached.get("ts") or 0) < ttl:
                    return cached.get("data")
        except Exception:
            pass
        return None

    def _save_cached(self, key: str, data: Any) -> None:
        try:
            self.save_data(key, {"data": data, "ts": time.time()})
        except Exception:
            pass

    def _search_tmdb_with_cache(self, title: str, mtype: str) -> Optional[dict]:
        """带 7 天缓存的 TMDB 识别：优先严格标题搜索，回退宿主识别链，最后宽松兜底。"""
        if not title:
            return None
        cache_key = self._tmdb_cache_key(title, mtype)
        cached = self._get_cached(cache_key, self._tmdb_cache_ttl)
        if cached:
            return cached

        media_type = MediaType.MOVIE if mtype == "MOVIE" else MediaType.TV
        tmdb_id = 0

        candidates: List[str] = [title]
        cleaned = _clean_netflix_title(title)
        if cleaned and cleaned != title:
            candidates.append(cleaned)

        # 1. 底层 Search 客户端直接调用 + 严格标题匹配。
        #    刻意绕开 TmdbApi._project_search_results：它用 `查询词 in item["name"]` 过滤，
        #    而 language=zh 时 item["name"] 是中文名（"绅士们"/"怪物史瑞克"），
        #    英文榜单标题永远不可能是其子串 —— 结果会被整批过滤掉（曾导致 40 条只认出 11 条）。
        probed: List[tuple] = []
        for cand_title in candidates:
            try:
                api = TmdbHelper.api()
                if media_type == MediaType.MOVIE:
                    raw_results = list(api.search.movies(term=cand_title) or [])
                else:
                    raw_results = list(api.search.tv_shows(term=cand_title) or [])
            except Exception as e:
                logger.debug("【网飞排行】TMDB 底层搜索 '%s' 失败：%s", cand_title, e)
                raw_results = []
            probed.append((cand_title, raw_results))
            if not raw_results:
                continue
            matched = _pick_best_tmdb_match(cand_title, raw_results, media_type, strict=True)
            if matched:
                tmdb_id = matched
                if cand_title != title:
                    logger.info(
                        "【网飞排行】'%s' 未直接命中，已用清理后标题 '%s' 匹配到 TMDB %s",
                        title, cand_title, tmdb_id,
                    )
                break

        # 2. 回退：宿主识别链（利用宿主 TMDB 缓存与中文别名匹配）
        if not tmdb_id:
            try:
                meta = _create_meta_info(title, media_type)
                if meta:
                    media_info = self.chain.recognize_media(meta=meta, cache=True)
                    if media_info and getattr(media_info, "tmdb_id", None):
                        source = str(getattr(media_info, "media_source", "") or "").lower()
                        if not source or source in ("themoviedb", "tmdb"):
                            tmdb_id = int(getattr(media_info, "tmdb_id"))
            except Exception as e:
                logger.debug("【网飞排行】识别链识别 '%s' 失败：%s", title, e)

        # 3. 最后兜底：宽松取第一个带海报的候选（复用第 1 步已抓到的结果，不重复请求）
        if not tmdb_id:
            for cand_title, raw_results in probed:
                if not raw_results:
                    continue
                tmdb_id = _pick_best_tmdb_match(cand_title, raw_results, media_type)
                if tmdb_id:
                    logger.info("【网飞排行】'%s' 无严格命中，宽松匹配到 TMDB %s", title, tmdb_id)
                    break

        if not tmdb_id:
            logger.warning("【网飞排行】TMDB 未匹配到 '%s'（%s）", title, mtype)
            return None

        # 4. 详情（海报/日期/中文名），7 天缓存
        detail_key = f"netflixtop10_detail_{mtype}_{tmdb_id}"
        detail = self._get_cached(detail_key, self._detail_cache_ttl)
        if not detail:
            try:
                api = TmdbHelper.api()
                if media_type == MediaType.MOVIE:
                    detail = api.movie.details(tmdb_id)
                else:
                    detail = api.tv.details(tmdb_id)
                if detail:
                    self._save_cached(detail_key, detail)
            except Exception as e:
                logger.debug("【网飞排行】TMDB 详情 %s 失败：%s", tmdb_id, e)
                detail = None

        result = {
            "id": tmdb_id,
            "zh_name": (detail or {}).get("name") or (detail or {}).get("title") or "",
            "poster_path": (detail or {}).get("poster_path") or "",
            "year": str((detail or {}).get("first_air_date") or (detail or {}).get("release_date") or "")[:4],
            "media_type": mtype,
        }
        self._save_cached(cache_key, result)
        return result

    # ─── 媒体状态 ───

    def _check_media_status(self, tmdbid: int, name: str = "", mtype: str = "TV") -> str:
        """按 TMDB 媒体身份返回 影片已入库 / 订阅已添加 / 未添加订阅。"""
        if not tmdbid:
            return "未添加订阅"
        media_type = MediaType.MOVIE if mtype == "MOVIE" else MediaType.TV

        status_key = f"netflixtop10_status_{mtype}_{tmdbid}"
        cached = self._get_cached(status_key, self._status_cache_ttl)
        if cached:
            return cached

        media_source = "themoviedb"
        media_id = str(tmdbid)
        status = "未添加订阅"
        try:
            # 1. 媒体库
            item = self._media_oper.exists(
                media_source=media_source, media_id=media_id, mtype=media_type.value
            )
            if item:
                status = "影片已入库"
        except Exception as e:
            logger.debug("【网飞排行】媒体库查询异常：%s", e)

        if status == "未添加订阅" and name:
            # 2. 按标题兜底
            try:
                item = self._media_oper.exists(title=name, mtype=media_type.value)
                if item:
                    status = "影片已入库"
            except Exception as e:
                logger.debug("【网飞排行】按标题查询媒体库异常：%s", e)

        if status == "未添加订阅":
            # 3. 整理记录（兼容无媒体服务器同步协议的环境）
            try:
                records = self._transfer_oper.get_by(
                    media_source=media_source, media_id=media_id, mtype=media_type.value
                )
                if any(getattr(r, "status", False) for r in records or []):
                    status = "影片已入库"
            except Exception as e:
                logger.debug("【网飞排行】整理记录查询异常：%s", e)

        if status == "未添加订阅":
            # 4. 订阅表
            try:
                subs = self._subscribe_oper.list_by_media_identity(
                    media_source=media_source, media_id=media_id
                )
                if subs:
                    status = "订阅已添加"
            except Exception as e:
                logger.debug("【网飞排行】订阅查询异常：%s", e)

        self._save_cached(status_key, status)
        return status

    # ─── 订阅 ───

    def _add_subscription(self, tmdbid: int, name: str, mtype: str, year: str = "") -> tuple:
        """调用订阅链添加订阅，返回 (success, message)。"""
        media_type = MediaType.MOVIE if mtype == "MOVIE" else MediaType.TV
        try:
            sub_id, msg = SubscribeChain().add(
                title=name,
                year=year or "",
                mtype=media_type,
                media_source="themoviedb",
                media_id=str(tmdbid),
                username="网飞排行",
            )
            if sub_id:
                # 失效状态缓存
                self.del_data(f"netflixtop10_status_{mtype}_{tmdbid}")
                return True, f"订阅已添加：{name}"
            return False, str(msg or "添加订阅失败")
        except Exception as e:
            logger.error("【网飞排行】添加订阅异常：%s", e)
            return False, str(e)

    def add_subscribe(self, tmdbid: int = None, name: str = "", mtype: str = "TV", apikey: str = None):
        """API：为指定片目添加订阅。"""
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        name = str(name or "").strip()
        logger.info("【网飞排行】添加订阅请求：%s (TMDB=%s, 类型=%s)", name, tmdbid, mtype)

        if not tmdbid and name:
            tmdb_info = self._search_tmdb_with_cache(name, mtype)
            if tmdb_info:
                tmdbid = tmdb_info.get("id")
                self._update_cached_tmdbid(name, mtype, tmdbid)
        if not tmdbid:
            return {"success": False, "message": f"未能识别《{name or '未知'}》的 TMDB 信息，请先刷新数据"}

        tmdbid = int(tmdbid)
        status = self._check_media_status(tmdbid, name, mtype)
        if status == "影片已入库":
            return {"success": False, "message": "影片已入库，无需订阅"}
        if status == "订阅已添加":
            return {"success": False, "message": "已订阅，无需重复订阅"}

        ok, msg = self._add_subscription(tmdbid, name, mtype)
        return {"success": ok, "message": msg, "data": {"tmdbid": tmdbid}}

    def _update_cached_tmdbid(self, name: str, mtype: str, tmdbid: int) -> None:
        """将即时识别出的 TMDB ID 回写主缓存。"""
        if not name or not tmdbid:
            return
        try:
            cached = self.get_data(self._cache_key)
            if not isinstance(cached, dict):
                return
            for item in cached.get("rows", []):
                if item.get("name") == name and item.get("mtype") == mtype:
                    item["tmdbid"] = int(tmdbid)
                    break
            self.save_data(self._cache_key, cached)
        except Exception as e:
            logger.debug("【网飞排行】回写缓存失败：%s", e)

    # ─── 新上榜通知 ───

    def _notify_new_entries(self, rows: List[dict], scope: str, week: str):
        """新上榜提醒：对比上次榜单快照，推送新上榜片目（按周+片目去重）。"""
        try:
            if not self.get_state() or not self._reminder_enabled:
                return

            prev = self.get_data(self._prev_key)
            prev_titles: Dict[str, set] = {}
            if prev and isinstance(prev, dict) and prev.get("scope") == scope:
                for cat, names in (prev.get("titles") or {}).items():
                    prev_titles[cat] = set(names or [])

            # 找出新上榜：当前在榜但上次不在榜
            new_items = []
            for item in rows:
                cat, name = item.get("category", ""), item.get("name", "")
                if name and name not in prev_titles.get(cat, set()):
                    new_items.append(item)

            # 去重记录（同周同片目只推送一次）
            record = self.get_data(self._notified_key) or {}
            sent: set = set()
            if isinstance(record, dict) and record.get("week") == week and record.get("scope") == scope:
                sent = set(record.get("items") or [])
            pending = [i for i in new_items if f"{i.get('category')}::{i.get('name')}" not in sent]

            # 更新快照（无论是否推送，快照都以本次为准）
            self.save_data(self._prev_key, {
                "scope": scope,
                "week": week,
                "titles": {cat: sorted({i["name"] for i in rows if i.get("category") == cat})
                           for cat in CATEGORY_ORDER},
                "ts": time.time(),
            })

            if not pending:
                logger.info("【网飞排行】无待推送的新上榜片目（新上榜 %d 条均已推送过）", len(new_items))
                return

            mtype_enum = MessageType
            try:
                mtype = mtype_enum[str(self._reminder_msgtype or "Plugin")]
            except (KeyError, TypeError):
                mtype = mtype_enum.Manual

            scope_text = "全球" if scope == "global" else country_display_name(scope)
            lines = [f"Netflix Top 10 新上榜（{scope_text} · {week}）"]
            images = []
            for item in pending:
                line = f"🆕 {item['rank']}. 《{item.get('zh_name') or item.get('name', '')}》"
                line += f"（{item.get('category_zh', '')}）"
                if item.get("views"):
                    line += f"周观看{format_count(item['views'])}"
                # 自动订阅（可选）
                tag = ""
                if (self._auto_subscribe and item.get("tmdbid")
                        and self._check_media_status(item["tmdbid"], item["name"], item["mtype"]) == "未添加订阅"):
                    ok, _ = self._add_subscription(
                        item["tmdbid"], item["name"], item["mtype"], item.get("year", "")
                    )
                    tag = "【已自动订阅】" if ok else "【未订阅】"
                if not tag:
                    status = self._check_media_status(item.get("tmdbid", 0), item.get("name", ""), item.get("mtype", "TV"))
                    tag = "【未订阅】" if status == "未添加订阅" else "【已订阅】"
                lines.append(line + tag)
                # 海报
                poster = item.get("poster") or ""
                if poster.startswith("/api/v1/system/img/1?imgurl="):
                    images.append(poster.split("imgurl=", 1)[1])
                elif poster.startswith("http"):
                    images.append(poster)
                # 每 8 条一批推送
                if len(lines) >= 9:
                    self.post_message(
                        mtype=mtype, title="网飞排行榜新上榜", text="\n".join(lines),
                        image=random.choice(images) if images else None,
                    )
                    lines = [f"Netflix Top 10 新上榜（{scope_text} · {week}）续"]
                    images = []
            if len(lines) > 1:
                self.post_message(
                    mtype=mtype, title="网飞排行榜新上榜", text="\n".join(lines),
                    image=random.choice(images) if images else None,
                )

            sent.update(f"{i.get('category')}::{i.get('name')}" for i in pending)
            self.save_data(self._notified_key, {
                "scope": scope, "week": week,
                "items": sorted(sent)[-200:],
                "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            logger.info("【网飞排行】新上榜推送完成，共 %d 条", len(pending))
        except Exception as e:
            logger.error("【网飞排行】新上榜提醒失败：%s", e)

    # ─── 订阅变更事件（失效状态短缓存）───

    @eventmanager.register(
        [
            EventType.SubscribeDeleted,
            EventType.SubscribeModified,
            EventType.SubscribeAdded,
        ]
    )
    def on_subscribe_changed(self, event: Event) -> None:
        if not getattr(self, "_enabled", False):
            return
        try:
            data = event.event_data if isinstance(event.event_data, dict) else {}
            snap = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
            media = data.get("mediainfo") if isinstance(data.get("mediainfo"), dict) else {}
            info = snap or media
            source = str(info.get("media_source") or "").strip().lower()
            if source and source != "themoviedb":
                return
            raw_id = info.get("media_id") or info.get("tmdb_id") or info.get("tmdbid")
            if raw_id is None or str(raw_id).strip() == "":
                return
            tmdbid = int(str(raw_id).strip())
            self.del_data(f"netflixtop10_status_TV_{tmdbid}")
            self.del_data(f"netflixtop10_status_MOVIE_{tmdbid}")
            logger.info("【网飞排行】订阅变更，已清除 tmdbid=%s 状态缓存", tmdbid)
        except Exception as e:
            logger.debug("【网飞排行】订阅事件处理失败：%s", e)

    # ─── API 端点 ───

    def run_once(self, apikey: str = None):
        """API：立即抓取一次（同步执行，返回结果并更新缓存）。"""
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        start = time.time()
        try:
            self._auto_refresh(notify=True, scope=self._view_scope)
            cached = self.get_data(self._cache_key) or {}
            elapsed = round(time.time() - start, 1)
            return {
                "success": True,
                "message": f"刷新完成，耗时 {elapsed}s",
                "data": {"total": cached.get("total", 0), "week": cached.get("week", "")},
            }
        except Exception as e:
            logger.error("【网飞排行】立即运行失败：%s", e)
            return {"success": False, "message": str(e)}

    def switch_region(self, scope: str = "", apikey: str = None):
        """API：切换详情页展示的地区榜单。

        立即更新 view_scope 并启动后台线程抓取新地区数据；
        当前缓存（旧地区）保留供过渡展示，前端刷新页面即可看到新地区。
        """
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        scope = (scope or "").strip() or "global"
        # 合法性校验：必须是 5 个预设之一，否则用 global
        valid_values = {r["value"] for r in self.PRESET_REGIONS}
        if scope not in valid_values:
            scope = "global"
        label = next((r["label"] for r in self.PRESET_REGIONS if r["value"] == scope), scope)
        cached = self.get_data(self._cache_key)
        already = (cached and isinstance(cached, dict)
                   and cached.get("scope") == scope
                   and cached.get("rows"))
        # 先更新 view_scope + 持久化，让 get_page 立刻反映新选择
        self._view_scope = scope
        self.save_data("netflixtop10_view_scope", {"scope": scope, "ts": time.time()})
        if already:
            logger.info("【网飞排行】切换地区 %s：缓存已存在，跳过抓取", label)
            return {
                "success": True,
                "message": f"已切换至「{label}」榜，缓存有效（周期 {cached.get('week', '未知')}）",
                "data": {"scope": scope, "week": cached.get("week", ""), "from_cache": True},
            }
        # 后台抓取新地区
        logger.info("【网飞排行】切换地区 %s：启动后台抓取", label)
        threading.Thread(
            target=self._run_refresh_safe,
            kwargs={"notify": False, "scope": scope},
            name=f"NetflixTop10.SwitchRegion.{scope}",
            daemon=True,
        ).start()
        return {
            "success": True,
            "message": f"已切换至「{label}」榜，正在后台抓取（约 1-2 分钟），请稍后刷新页面",
            "data": {"scope": scope, "week": "", "from_cache": False},
        }

    def get_cache(self, apikey: str = None):
        """API：获取缓存数据（每次重算订阅状态）。"""
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        cached = self.get_data(self._cache_key)
        if cached and isinstance(cached, dict) and cached.get("rows"):
            for item in cached.get("rows", []):
                item["status"] = self._check_media_status(
                    item.get("tmdbid", 0), item.get("name", ""), item.get("mtype", "TV")
                )
            return {"success": True, "data": cached}
        return {"success": True, "data": {"rows": [], "total": 0}}

    def clear_cache(self, apikey: str = None):
        """API：清理全部插件缓存并静默重建。"""
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        try:
            all_items = self.get_data() or []
            removed = 0
            for item in all_items:
                key = getattr(item, "key", "")
                if key.startswith("netflixtop10_"):
                    self.del_data(key)
                    removed += 1
            logger.info("【网飞排行】已清理 %d 个缓存项，开始重新抓取", removed)
            try:
                self._auto_refresh(notify=False)
                msg = f"已清理 {removed} 个缓存项，并重新抓取最新数据"
            except Exception as e:
                msg = f"已清理 {removed} 个缓存项，但重新抓取失败：{e}"
            return {"success": True, "message": msg, "data": {"count": removed}}
        except Exception as e:
            return {"success": False, "message": str(e)}
