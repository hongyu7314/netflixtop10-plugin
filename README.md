# 网飞排行榜 (NetflixTop10)

MoviePilot V3 插件。基于 Netflix 官方公开的 [Tudum Top 10](https://www.netflix.com/tudum/top10) 周榜数据（全球 + 90+ 国家/地区），覆盖 **剧集（英语/非英语）+ 电影（英语/非英语）** 共四个榜单分类。每周自动刷新，TMDB 识别媒体信息后可在 MoviePilot 内：

- 浏览全部榜单项（海报、片名、季数、观看时长/次数、上榜周数）
- 一键添加订阅
- 接收新上榜/排名变化通知
- 自动订阅新入榜作品（可选）

## 插件目录

| 路径 | 说明 |
| --- | --- |
| [`plugins.v3/netflixtop10/`](plugins.v3/netflixtop10/) | 插件源码（`__init__.py` + `scraper.py` + `package.v3.json` + `README.md`） |
| [`plugins.v3/netflixtop10/README.md`](plugins.v3/netflixtop10/README.md) | 安装、配置、API 详细文档 |

## 安装

### MoviePilot 插件市场 → Git 安装

在 MoviePilot 后台 → **插件** → **安装插件** → **Git 安装**，粘贴：

```
https://github.com/hongyu7314/netflixtop10-plugin
```

### 手动安装

将 `plugins.v3/netflixtop10/` 整个目录放到 MoviePilot 的 `plugins.v3/` 目录下，重启 MoviePilot，启用插件即可。

## 数据来源

Netflix 官方公开 TSV 文件，每周一更新（北京时间周二发布）：

| 文件 | 维度 | 字段 |
| --- | --- | --- |
| `all-weeks-global.tsv` | 全球榜（4 个分类） | 周观看时长、观看次数、片长、上榜累计周数 |
| `all-weeks-countries.tsv` | 90+ 国家/地区（4 个分类） | 仅排名与上榜累计周数 |

> 数据抓取通过 `requests` 自带代理设置，遵循 MoviePilot 全局代理配置。极端情况下（Netflix 拒绝请求）会自动重试 2 次。

## 致谢

本插件参考 [irab-liu/MoviePilot-Plugins](https://github.com/irab-liu/MoviePilot-Plugins) 中 **MaoyanDianYing**（猫眼热度榜）的抓取 → TMDB 识别 → 一键订阅 → 通知提醒四段式架构。

## 许可

[MIT](LICENSE)