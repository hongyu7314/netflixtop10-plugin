# 网飞排行榜（NetflixTop10）

MoviePilot V3 插件。基于 Netflix 官方公开的 [Tudum Top 10](https://www.netflix.com/tudum/top10) 周榜数据（全球 + 90+ 个国家/地区），覆盖 **剧集（英语/非英语）+ 电影（英语/非英语）** 共四个榜单分类。每周自动刷新，TMDB 识别媒体信息后可在 MoviePilot 内：

- 浏览全部榜单项（海报、片名、季数、观看时长/次数、上榜周数）
- 一键添加订阅
- 接收新上榜/排名变化通知
- 自动订阅新入榜作品（可选）

## 数据来源

Netflix 官方公开的 TSV 文件，每周一更新（北京时间周二发布）：

| 文件 | 维度 | 字段 |
| --- | --- | --- |
| `all-weeks-global.tsv` | 全球榜（4 个分类） | 周观看时长、观看次数、片长、上榜累计周数 |
| `all-weeks-countries.tsv` | 90+ 国家/地区（4 个分类） | 仅排名与上榜累计周数 |

> 数据抓取通过 `requests` 自带代理设置，遵循 MoviePilot 全局代理配置。极端情况下（Netflix 拒绝请求）会自动重试 2 次。

## 功能特性

| 模块 | 说明 |
| --- | --- |
| 📊 **榜单浏览** | 详情页用 Vuetify 卡片流呈现，按分类分组；包含海报、片名（中/英）、季数、上榜周数、观看量（按国家/全球维度显示） |
| 🔔 **新上榜提醒** | 新一周发布后，对比上周榜，新上榜 / 排名跃升 TOP3 / 新入榜首的剧集电影触发通知 |
| ⚡ **一键订阅** | 卡片按钮调用插件 `/subscribe` API，TMDB 识别后写入 MoviePilot 订阅库；已订阅/已入库的项目自动标识 |
| 🤖 **自动订阅** | 可选对新上榜 + 未订阅的作品自动发起订阅（受 TV 季/电影上映日期检查保护） |
| 🌍 **多国家切换** | 配置页可切换「全球榜」或选择任意 Netflix 上线国家/地区（界面附带常用国家下拉） |

## 安装

### 方式一：市场安装（推荐）

MoviePilot V3 插件市场（待仓库发布后即可搜索 **网飞排行榜** 一键安装）。

### 方式二：手动安装

将 `plugins.v3/netflixtop10/` 目录（含 `__init__.py`、`scraper.py`、`package.v3.json`）放入 MoviePilot `plugins.v3/` 目录下，重启 MoviePilot，插件管理中启用。

### 方式三：Git 安装

在 MoviePilot 插件管理 → 安装插件 → Git 安装，填入本仓库地址即可。

## 配置项

启用插件后，在 **设置 → 插件 → 网飞排行榜** 中可调整：

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| 启用推送通知 | ✅ | 新上榜/排名变化时发送通知（按 MoviePilot 消息渠道） |
| 自动订阅新上榜作品 | ❌ | 对新上榜 + 尚未订阅的剧集电影自动发起订阅 |
| 启用国家榜 | ✅ | 关闭后仅展示全球榜（仅依赖 `all-weeks-global.tsv`，节省请求） |
| 国家榜地区 | 全球 | 下拉选择，附常用 50+ 国家/地区 |
| 定时刷新（小时） | 12 | Netflix 周榜每周二更新，建议 6–24 小时 |

> 国家/地区字段使用 Netflix 英文原名（如 `Hong Kong SAR`），界面显示中文别名（"中国香港"）。

## 榜单示例

插件详情页（`get_page`）按分类分组展示：

```
🎬 剧集（英语）  ━━━━ 第 39 周 (2026-09-06)
  #1  鱿鱼游戏 / Squid Game  第3季  ⭐ 上榜 5 周   3.0亿次观看
  #2  Wednesday                第2季  ⭐ 上榜 3 周   9700万次观看

🎬 剧集（非英语）
  #1  The Trunk                第1季  ⭐ 上榜 1 周

🎬 电影（英语）
  #1  KPop Demon Hunters                ⭐ 上榜 2 周   3870万次观看

🎬 电影（非英语）
  #1  Swapped                            ⭐ 上榜 1 周   2700万次观看
```

每条卡片右下角操作按钮：

- **订阅**（未订阅且未入库时显示） → 调用 `/subscribe` API
- **已订阅**（订阅表中存在）→ 灰色提示
- **已入库**（媒体库/历史中存在）→ 绿色勾选

## API 路径

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/plugin/NetflixTop10/refresh` | 强制刷新缓存（最近一周拉取） |
| `POST` | `/plugin/NetflixTop10/subscribe` | 一键订阅（body: `name`, `mtype`, `category`, `week`） |

鉴权使用 MoviePilot 默认 `apikey`（由前端 `events` 自动附带）。

## 异常与边界

- **TMDB 识别失败** → 卡片显示「TMDB 未匹配」，订阅按钮隐藏（不影响其他条目）
- **网络被 Netflix 拒绝** → 自动重试 2 次（指数退避），失败后保留旧数据 + 错误日志
- **TSV 格式变化** → `scraper.parse_tsv` 会因缺列抛出 `ValueError`，插件捕获后告警但不崩溃
- **同标题跨周重复订阅** → 在历史记录 `subscribed_keys` 中持久化 MD5 指纹，避免重复

## 开发与测试

```bash
# 运行解析器单元测试（纯标准库，无需 MoviePilot 环境）
python tests/test_scraper.py
```

测试覆盖：全球榜最新周提取、国家榜过滤（精确/模糊/大小写）、分类过滤、排序、工具函数、异常输入。

## 致谢

本插件参考 [irab-liu/MoviePilot-Plugins](https://github.com/irab-liu/MoviePilot-Plugins) 中 **MaoyanDianYing**（猫眼热度榜）的抓取 → TMDB 识别 → 一键订阅 → 通知提醒四段式架构。

数据源：[Netflix Tudum Top 10](https://www.netflix.com/tudum/top10)（公开周榜）。

## 许可

MIT