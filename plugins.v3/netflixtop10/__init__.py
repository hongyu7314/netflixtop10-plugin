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
    """创建 MoviePilot MetaInfo，兼容 TMDB chain/cache。"""
    try:
        try:
            from app.sdk.media import MetaInfo
        except ImportError:
            from app.schemas import MetaInfo
        try:
            meta = MetaInfo(name=title, type=mtype)
        except Exception:
            meta = MetaInfo()
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
    except Exception as e:
        logger.warning("创建 MoviePilot MetaInfo 失败: %s", e)
        return None


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
        "Netflix（Tudum Top 10）官方周榜订阅：全球/国家维度、剧集/电影四分类，"
        "TMDB 识别，一键订阅，新上榜提醒，支持自动订阅。"
    )
    plugin_icon = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/08/Netflix_2015_logo.svg/512px-Netflix_2015_logo.svg.png"
    plugin_version = "1.0.0"
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
            "【网飞排行】插件初始化：enabled=%s, scope=%s, categories=%s, interval=%sh",
            self._enabled, self._rank_scope, self._media_categories, self._refresh_interval,
        )

        if config.get("run_once_flag"):
            threading.Thread(
                target=self._run_refresh_safe, name="NetflixTop10.RunOnce", daemon=True
            ).start()
            self.update_config({**config, "run_once_flag": False})

        if self._enabled:
            cached = self.get_data(self._cache_key)
            scope_changed = not (cached and isinstance(cached, dict)
                                 and cached.get("scope") == self._rank_scope)
            if not cached or not isinstance(cached, dict) or not cached.get("rows") or scope_changed:
                threading.Thread(
                    target=self._run_refresh_safe, name="NetflixTop10.InitialRefresh", daemon=True
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

        rows = cached.get("rows", [])
        scope_text = "全球" if cached.get("scope") == "global" else country_display_name(cached.get("scope", ""))
        header_text = (
            f"榜单范围：{scope_text} ｜ 数据周期：{cached.get('week', '未知')} ｜ "
            f"共 {len(rows)} 条 ｜ 最后刷新：{cached.get('update_time', '未知')}"
        )
        contents: list[dict] = [
            {
                "component": "VRow",
                "props": {"class": "align-center mb-2", "no-gutters": True},
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                        {"component": "VAlert", "props": {"type": "success", "variant": "tonal", "density": "compact"}, "text": header_text}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "div", "props": {"class": "d-flex justify-end ga-2"},
                         "content": [
                             {"component": "VBtn", "props": {"size": "small", "color": "primary", "variant": "tonal", "text": "立即刷新"},
                              "events": {"click": {"api": "plugin/NetflixTop10/run-once", "method": "get", "params": {"apikey": settings.API_TOKEN}}}},
                             {"component": "VBtn", "props": {"size": "small", "color": "warning", "variant": "tonal", "text": "清理缓存"},
                              "events": {"click": {"api": "plugin/NetflixTop10/clear-cache", "method": "get", "params": {"apikey": settings.API_TOKEN}}}},
                         ]}]},
                ],
            },
        ]

        # 按分类分组渲染
        for cat in CATEGORY_ORDER:
            cat_rows = [r for r in rows if r.get("category") == cat]
            if not cat_rows:
                continue
            card_content: list[dict] = [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": f"{CATEGORY_ZH.get(cat, cat)} · {cached.get('week', '')}"},
                {"component": "VDivider"},
                {"component": "VCardText", "props": {"class": "pa-2"}, "content": []},
            ]
            card_rows = card_content[2]["content"]
            for item in cat_rows:
                card_rows.append(self._render_item_row(item))
            contents.append({"component": "VCard", "props": {"class": "mb-3"}, "content": card_content})
        return contents

    def _render_item_row(self, item: dict) -> dict:
        """渲染单个榜单条目行。"""
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

        # 状态徽章
        status_color = {"影片已入库": "success", "订阅已添加": "info"}.get(status, "default")
        status_chip = {"component": "VChip", "props": {
            "size": "x-small", "variant": "tonal", "color": status_color,
        }, "text": status}

        # 订阅按钮：未订阅且有 TMDB ID 时可点击
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

    def _run_refresh_safe(self, notify: bool = True):
        """带锁的刷新入口（线程安全）。"""
        with self._fetch_lock:
            try:
                self._auto_refresh(notify=notify)
            except Exception as e:
                logger.error("【网飞排行】刷新失败: %s", e)

    def _auto_refresh(self, notify: bool = True):
        """核心刷新：抓取榜单 -> TMDB 识别 -> 更新缓存 -> 新上榜通知。"""
        logger.info("【网飞排行】开始刷新，scope=%s", self._rank_scope)
        scope = self._rank_scope
        country = None if scope == "global" else scope
        urls = self.GLOBAL_URLS if country is None else self.COUNTRIES_URLS

        text = self._fetch_tsv(urls)
        items, countries = extract_latest(text, country=country, categories=self._media_categories)
        if not items:
            raise ValueError("未解析到榜单数据（请检查榜单范围/分类配置，或数据源格式变化）")

        # 缓存国家列表（供配置表单使用）
        if countries:
            self.save_data(self._countries_key, {"countries": countries, "ts": time.time()})

        # 与旧缓存合并：已识别的 TMDB 信息直接复用
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
        logger.info("【网飞排行】刷新完成，周期 %s 共 %d 条（新识别 TMDB %d 条）", week, len(enriched), new_tmdb_count)

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
        md5 = hashlib.md5(f"{mtype}::{title}".encode("utf-8")).hexdigest()[:12]
        return f"netflixtop10_tmdb_{md5}"

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
        """带 7 天缓存的 TMDB 识别：优先宿主识别链，回退 TmdbApi 直接搜索。"""
        if not title:
            return None
        cache_key = self._tmdb_cache_key(title, mtype)
        cached = self._get_cached(cache_key, self._tmdb_cache_ttl)
        if cached:
            return cached

        media_type = MediaType.MOVIE if mtype == "MOVIE" else MediaType.TV
        tmdb_id = 0
        zh_name = ""
        # 1. 宿主识别链（利用宿主 TMDB 缓存与别名匹配）
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

        # 2. 回退 TmdbApi 搜索
        if not tmdb_id:
            try:
                api = TmdbHelper.api()
                if media_type == MediaType.MOVIE:
                    result = api.search_movies(title, "")
                else:
                    result = api.search_tvs(title, "")
                if result:
                    tmdb_id = int(result[0].get("id") or 0)
            except Exception as e:
                logger.debug("【网飞排行】TMDB 搜索 '%s' 失败：%s", title, e)

        if not tmdb_id:
            return None

        # 3. 详情（海报/日期/中文名），7 天缓存
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
            self._auto_refresh(notify=True)
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
