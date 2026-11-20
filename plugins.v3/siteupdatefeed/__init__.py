# -*- coding: utf-8 -*-
"""
站点更新播报 SiteUpdateFeed
------------------------------------------------------------
把「各 PT 站最近发布了什么」变成一条看得完的更新流：

  1. 数据源用站点自带的 RSS（torrentrss.php + passkey）与首页浏览，
     走 MP 内置的 TorrentsChain，不额外发明爬虫；
  2. 与上次快照做差集，只播「新增」，不重复刷旧种子；
  3. 默认排除本地已下载/已做种的同名种子，并可按标签排除刷流种，
     避免播报被刷流噪音淹没；
  4. 每天汇总成一条钉钉消息（按站点分组 + 限量），面板常驻显示
     每日新增台账（按天 + 累计 + 最近 7 天）。

只通知、不下载：不会自动添加任何下载任务。
"""
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.chain.torrents import TorrentsChain
from app.db.oper.site import SiteOper
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.sdk.logging import logger
from app.sdk.services import DownloaderHelper


# ----------------------------------------------------------------------
# 纯函数：便于离线用真实 RSS/首页样本做回归测试
# ----------------------------------------------------------------------
def normalize_title(name: Any) -> str:
    """把种子名归一化，用于与本地下载器里的同名种子比对"""
    if not name:
        return ""
    text = str(name)
    text = re.sub(r"\[[^\]]{0,60}\]", " ", text)            # 去掉 [中字][双语] 等标签
    text = re.sub(r"-\s*[A-Za-z0-9@._\-]{1,28}\s*$", " ", text)  # 去掉 -GROUP 后缀
    text = re.sub(r"[^\w一-鿿]+", "", text)                  # 只保留字母数字与汉字
    return text.lower()


def title_key(title: Any) -> str:
    """用于差集去重的稳定键"""
    return re.sub(r"\s+", " ", str(title or "")).strip().lower()


def match_local(norm: str, local: Dict[str, str], min_len: int = 14) -> Optional[str]:
    """
    判断归一化后的种子名是否命中本地已有种子。
    命中返回该本地种子的标签（逗号分隔），未命中返回 None。
    """
    if not norm:
        return None
    if norm in local:
        return local[norm]
    for name, tags in local.items():
        if len(norm) < min_len:
            continue
        if name.startswith(norm) or norm.startswith(name):
            return tags
    return None


def format_time(value: Any) -> str:
    """把 pubdate（字符串或 datetime）转成 HH:MM"""
    if not value:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%m-%d %H:%M")
    text = str(value)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})", text)
    if m:
        return f"{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}"
    m = re.search(r"(\d{2})-(\d{2})[ T](\d{2}):(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)} {m.group(3)}:{m.group(4)}"
    return text[:16]


def build_message(by_site: List[Dict[str, Any]], total: int, skipped_local: int,
                  skipped_tag: int, stats: Dict[str, Any]) -> str:
    """生成钉钉 markdown 正文"""
    lines = ["### 📡 PT 站更新播报", ""]
    for group in by_site:
        lines.append(f"**{group['site']}** 新增 {group['count']} 个")
        for item in group["items"]:
            lines.append(f"- {item['title']} · {item['time']}")
        if group.get("more"):
            lines.append(f"- …… 另有 {group['more']} 个未列出")
        lines.append("")
    if not by_site:
        lines.append("本轮没有新的种子（已过滤本地已有与刷流种）。")
        lines.append("")
    lines.append(
        f"> 新增 {total} 个 · 累计 {stats.get('total', 0)} 个 · "
        f"过滤本地已有 {skipped_local} 个 / 刷流种 {skipped_tag} 个 · "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} · 只通知不下载"
    )
    return "\n".join(lines)


# ----------------------------------------------------------------------
# 插件主体
# ----------------------------------------------------------------------
class SiteLike:
    """统一的站点视图，屏蔽 MP 不同版本的 SiteOper 返回类型差异"""

    def __init__(self, sid: Any, name: Any, domain: Any, rss: Any):
        self.id = sid
        self.name = name or domain
        self.domain = domain or ""
        self.rss = rss or ""

    def __repr__(self):
        return f"<SiteLike {self.name} {self.domain}>"


class SiteUpdateFeed(_PluginBase):
    plugin_name = "站点更新播报"
    plugin_desc = "汇总各 PT 站最近发布的种子，每天一条；只通知不下载，面板带每日新增台账。"
    plugin_icon = "https://raw.githubusercontent.com/147392552/MoviePilot-Plugins/main/icons/siteupdatefeed.png"
    plugin_version = "1.0.2"
    plugin_author = "WorkBuddy"
    author_url = "https://github.com/147392552"
    plugin_config_prefix = "siteupdatefeed_"
    plugin_order = 34
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _cron = "25 8 * * *"
    _interval_minutes = 0
    _site_ids: List[Any] = []
    _rows = 50
    _max_per_site = 8
    _max_total = 30
    _skip_local = True
    _exclude_tags = "刷流"
    _first_notify = True
    _notify = True
    _browse_supplement = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: dict = None) -> None:
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "25 8 * * *").strip()
        try:
            self._interval_minutes = int(config.get("interval_minutes") or 0)
        except Exception:
            self._interval_minutes = 0
        self._site_ids = config.get("site_ids") or []
        try:
            self._rows = max(10, min(100, int(config.get("rows") or 50)))
        except Exception:
            self._rows = 50
        try:
            self._max_per_site = max(1, min(50, int(config.get("max_per_site") or 8)))
        except Exception:
            self._max_per_site = 8
        try:
            self._max_total = max(1, min(200, int(config.get("max_total") or 30)))
        except Exception:
            self._max_total = 30
        self._skip_local = bool(config.get("skip_local", True))
        self._exclude_tags = str(config.get("exclude_tags") or "刷流")
        self._first_notify = bool(config.get("first_notify", True))
        self._notify = bool(config.get("notify", True))
        self._browse_supplement = bool(config.get("browse_supplement", False))
        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self.__snapshot())
            self.__run(manual=True)
        logger.info(
            f"站点更新播报初始化：启用={self._enabled}，站点={len(self.__target_sites())}，"
            f"每站上限={self._max_per_site}，本地过滤={self._skip_local}"
        )

    def __snapshot(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "onlyonce": False,
            "cron": self._cron,
            "interval_minutes": self._interval_minutes,
            "site_ids": self._site_ids,
            "rows": self._rows,
            "max_per_site": self._max_per_site,
            "max_total": self._max_total,
            "skip_local": self._skip_local,
            "exclude_tags": self._exclude_tags,
            "first_notify": self._first_notify,
            "notify": self._notify,
            "browse_supplement": self._browse_supplement,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    # ------------------------------------------------------------------
    # 数据读取
    # ------------------------------------------------------------------
    def __target_sites(self) -> List[Any]:
        """按配置筛选要播报的站点（兼容不同 MP 版本的 SiteOper 取法）"""
        sites = []
        try:
            oper = SiteOper()
            all_sites = []
            for method in ("get_services", "get_all", "get_sites", "get_enabled_sites"):
                func = getattr(oper, method, None)
                if not callable(func):
                    continue
                try:
                    candidate = func()
                except Exception:
                    continue
                if isinstance(candidate, list) and candidate:
                    all_sites = candidate
                    break
            if not all_sites:
                # 兜底：遍历所有 get_* 方法，找第一个返回站点列表的
                for name in dir(oper):
                    if not name.startswith("get"):
                        continue
                    func = getattr(oper, name, None)
                    if not callable(func):
                        continue
                    try:
                        candidate = func()
                    except Exception:
                        continue
                    if not isinstance(candidate, list) or not candidate:
                        continue
                    first = candidate[0]
                    domain = first.get("domain") if isinstance(first, dict) else getattr(first, "domain", None)
                    if domain:
                        all_sites = candidate
                        break
            for site in all_sites or []:
                sid = getattr(site, "id", None) if not isinstance(site, dict) else site.get("id")
                domain = getattr(site, "domain", None) if not isinstance(site, dict) else site.get("domain")
                name = getattr(site, "name", None) if not isinstance(site, dict) else site.get("name")
                rss = getattr(site, "rss", None) if not isinstance(site, dict) else site.get("rss")
                if self._site_ids and sid not in [int(x) for x in self._site_ids]:
                    continue
                if not domain or not name:
                    continue
                sites.append(SiteLike(sid, name, domain, rss))
        except Exception as err:
            logger.warning(f"站点更新播报：读取站点列表失败：{str(err)[:150]}")
        return sites

    @staticmethod
    def _field(torrent: Any, *names: str, default: Any = "") -> Any:
        for name in names:
            if isinstance(torrent, dict):
                if torrent.get(name) not in (None, ""):
                    return torrent.get(name)
            else:
                value = getattr(torrent, name, None)
                if value not in (None, ""):
                    return value
        return default

    def __fetch(self, site: Any) -> List[Dict[str, Any]]:
        """抓取一个站点的更新条目（RSS 为主，首页浏览可选补充）"""
        domain = getattr(site, "domain", "") or ""
        rows: List[Dict[str, Any]] = []
        chain = TorrentsChain()
        try:
            torrents = chain.rss(domain=domain) or []
        except Exception as err:
            logger.warning(f"站点更新播报：{getattr(site, 'name', domain)} RSS 拉取失败：{str(err)[:120]}")
            torrents = []
        for item in torrents:
            rows.append(self.__to_row(item, site))
        if self._browse_supplement:
            try:
                browsed = chain.browse(domain=domain) or []
            except Exception as err:
                logger.warning(f"站点更新播报：{getattr(site, 'name', domain)} 首页浏览失败：{str(err)[:120]}")
                browsed = []
            for item in browsed:
                rows.append(self.__to_row(item, site, from_browse=True))
        return rows

    def __to_row(self, item: Any, site: Any, from_browse: bool = False) -> Dict[str, Any]:
        title = self._field(item, "title", "torrent_name", "name", default="")
        tid = self._field(item, "torrent_id", "id", "torrent_hash", "hash", default="")
        if not tid:
            tid = title_key(title)
        return {
            "id": str(tid),
            "title": str(title).strip(),
            "time": format_time(self._field(item, "pubdate", "publish_date", "addtime", default="")),
            "tags": self._field(item, "tags", "labels", default=""),
            "from_browse": from_browse,
            "site": getattr(site, "name", "") or "",
        }

    def __local_index(self) -> Dict[str, str]:
        """本地下载器已有种子：归一化名 -> 标签串"""
        index: Dict[str, str] = {}
        try:
            helper = DownloaderHelper()
            for name in helper.get_configs().keys():
                service = helper.get_service(name=name)
                if not service or not getattr(service, "instance", None):
                    continue
                torrents, error = service.instance.get_torrents()
                if error:
                    continue
                for torrent in torrents or []:
                    raw_name = self._field(torrent, "name", "torrent_name", "title", default="")
                    norm = normalize_title(raw_name)
                    if not norm:
                        continue
                    tags = self._field(torrent, "tags", default="")
                    if isinstance(tags, (list, tuple)):
                        tags = ",".join(str(x) for x in tags)
                    index[norm] = str(tags or "")
        except Exception as err:
            logger.warning(f"站点更新播报：读取本地种子失败：{str(err)[:120]}")
        return index

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def __run(self, manual: bool = False) -> Dict[str, Any]:
        today = datetime.now().strftime("%Y-%m-%d")
        sites = self.__target_sites()
        if not sites:
            return {"success": False, "message": "没有可用站点"}

        seen_data = self.get_data("feed_seen") or {}
        if not isinstance(seen_data, dict):
            seen_data = {}
        local_index = self.__local_index() if self._skip_local else {}
        exclude_tags = [t.strip() for t in self._exclude_tags.replace("，", ",").split(",") if t.strip()]

        by_site: List[Dict[str, Any]] = []
        total = 0
        skipped_local = 0
        skipped_tag = 0
        day_stat: Dict[str, int] = {}
        recent: List[Dict[str, str]] = list(self.get_data("feed_recent") or [])

        for site in sites:
            sid = str(getattr(site, "id", ""))
            rows = self.__fetch(site)
            if not rows:
                continue
            known = set(seen_data.get(sid) or [])
            first_run = not known
            fresh = [r for r in rows if r["id"] not in known]
            seen_data[sid] = list({r["id"] for r in rows} | known)[:800]

            candidates: List[Dict[str, Any]] = []
            for row in fresh:
                norm = normalize_title(row["title"])
                tags = ""
                if norm:
                    hit = match_local(norm, local_index)
                    if hit is not None:
                        tags = hit
                        if any(t and t in tags for t in exclude_tags):
                            skipped_tag += 1
                            continue
                        if self._skip_local:
                            skipped_local += 1
                            continue
                candidates.append(row)

            if first_run and not self._first_notify:
                candidates = []
            elif first_run:
                # 首次运行只播最近 3 条/站当样例，其余进基线，避免一次刷屏
                candidates = candidates[:3]
            if not candidates:
                continue

            candidates.sort(key=lambda x: x.get("time") or "", reverse=True)
            show = candidates[: self._max_per_site]
            day_stat[sid] = len(candidates)
            total += len(candidates)
            by_site.append({
                "site": getattr(site, "name", sid),
                "count": len(candidates),
                "items": show,
                "more": max(0, len(candidates) - len(show)),
            })
            for row in show:
                recent.insert(0, {"site": getattr(site, "name", sid), "title": row["title"], "time": row["time"]})

        # 限量：总条数超限就截断，避免刷屏
        if total > self._max_total:
            overflow = total - self._max_total
            total = self._max_total
            for group in reversed(by_site):
                if overflow <= 0:
                    break
                trim = min(overflow, len(group["items"]))
                if trim:
                    del group["items"][len(group["items"]) - trim:]
                    group["count"] = max(0, group["count"] - trim)
                    group["more"] = group.get("more", 0) + trim
                    overflow -= trim

        stats = self.__record_stats(today, day_stat)
        self.save_data("feed_seen", seen_data)
        self.save_data("feed_recent", recent[:20])
        self.save_data("feed_last_run", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if by_site and self._notify:
            message = build_message(by_site, total, skipped_local, skipped_tag, stats)
            try:
                self.post_message(mtype=NotificationType.SiteMessage,
                                  title="【PT 站更新播报】", text=message)
            except Exception as err:
                logger.warning(f"站点更新播报：推送失败：{str(err)[:120]}")

        logger.info(
            f"站点更新播报完成：新增 {total} 个，站点 {len(by_site)} 个，"
            f"过滤本地 {skipped_local} / 刷流 {skipped_tag}"
        )
        return {
            "success": True,
            "date": today,
            "total": total,
            "sites": len(by_site),
            "skipped_local": skipped_local,
            "skipped_tag": skipped_tag,
            "manual": manual,
        }

    def __record_stats(self, today: str, day_stat: Dict[str, int]) -> Dict[str, Any]:
        data = self.get_data("feed_stats") or {}
        if not isinstance(data, dict):
            data = {}
        days = data.get("days") if isinstance(data.get("days"), dict) else {}
        entry = days.get(today) or {}
        for sid, count in day_stat.items():
            entry[sid] = int(entry.get(sid) or 0) + int(count)
        days[today] = entry
        if len(days) > 400:
            days = {k: days[k] for k in sorted(days.keys())[-400:]}
        data["days"] = days
        total = sum(sum(v.values()) for v in days.values())
        data["total"] = total
        self.save_data("feed_stats", data)
        return {"total": total, "days": len(days)}

    # ------------------------------------------------------------------
    # 服务与接口
    # ------------------------------------------------------------------
    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        services = [{
            "id": "SiteUpdateFeed_Collect",
            "name": "站点更新播报-采集推送",
            "func": self.__run_safe,
            "kwargs": {},
        }]
        if self._interval_minutes > 0:
            services[0]["trigger"] = IntervalTrigger(minutes=self._interval_minutes)
        else:
            try:
                services[0]["trigger"] = CronTrigger.from_crontab(self._cron)
            except Exception:
                services[0]["trigger"] = IntervalTrigger(hours=12)
        return services

    def __run_safe(self):
        try:
            self.__run()
        except Exception as err:
            logger.error(f"站点更新播报运行异常：{str(err)[:200]}")

    def stop_service(self) -> None:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/stats",
                "endpoint": self.__stats_api,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "更新播报统计台账（今日/累计/最近7天）",
            },
            {
                "path": "/run",
                "endpoint": self.__run_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即采集一轮并推送",
            },
            {
                "path": "/diagnose",
                "endpoint": self.__diagnose_api,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "站点列表读取诊断",
            },
        ]

    def __diagnose_api(self, api_result=None):
        """列出 SiteOper 各取法返回的条数，便于确认站点来源是否正确"""
        info = []
        try:
            oper = SiteOper()
            for name in dir(oper):
                if not name.startswith("get"):
                    continue
                func = getattr(oper, name, None)
                if not callable(func):
                    continue
                try:
                    candidate = func()
                except Exception:
                    continue
                if not isinstance(candidate, list) or not candidate:
                    continue
                first = candidate[0]
                keys = list(first.keys())[:8] if isinstance(first, dict) else [a for a in dir(first) if not a.startswith("_")][:8]
                info.append({"method": name, "count": len(candidate), "sample_keys": keys})
        except Exception as err:
            return {"success": False, "message": str(err)[:150]}
        return {"success": True, "data": {"methods": info, "target_sites": len(self.__target_sites())}}

    def __stats_api(self, api_result=None):
        return api_result or {"success": True, "data": self.__stats_summary()}

    def __run_api(self, api_result=None):
        return api_result or {"success": True, "data": self.__run(manual=True)}

    def __stats_summary(self) -> Dict[str, Any]:
        data = self.get_data("feed_stats") or {}
        days = data.get("days") if isinstance(data.get("days"), dict) else {}
        ordered = sorted(days.items())
        today = datetime.now().strftime("%Y-%m-%d")
        site_names = {}
        try:
            for site in self.__target_sites():
                site_names[str(getattr(site, "id", ""))] = getattr(site, "name", "")
        except Exception:
            pass
        cur = days.get(today) or {}
        return {
            "today": today,
            "today_total": sum(int(v or 0) for v in cur.values()),
            "today_by_site": {site_names.get(k, k): int(v or 0) for k, v in cur.items()},
            "days": len(days),
            "total": int(data.get("total") or 0),
            "last_run": self.get_data("feed_last_run") or "",
            "recent": [
                {"day": d, "total": sum(int(v or 0) for v in v0.values()),
                 "by_site": {site_names.get(k, k): int(v or 0) for k, v in v0.items()}}
                for d, v0 in ordered[-7:]
            ],
        }

    # ------------------------------------------------------------------
    # 表单与面板
    # ------------------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "notify", "label": "推送钉钉汇总"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {"model": "cron", "label": "执行周期(cron)",
                                                                 "hint": "默认每天 08:25；留空则配合「间隔分钟」使用"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {"model": "interval_minutes", "label": "间隔分钟(0=用cron)",
                                                                 "type": "number", "min": 0}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {"model": "max_per_site", "label": "每站最多列出(条)",
                                                                 "type": "number", "min": 1, "max": 50}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {"model": "max_total", "label": "单次最多播报(条)",
                                                                 "type": "number", "min": 1, "max": 200}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VSwitch", "props": {"model": "skip_local", "label": "排除本地已有种子",
                                                                 "hint": "本地已下载/已做种的同名资源不再播报"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {"model": "exclude_tags", "label": "排除标签(逗号分隔)",
                                                                 "hint": "默认「刷流」，避免刷流种淹没播报"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VSwitch", "props": {"model": "first_notify", "label": "首次运行也播报",
                                                                 "hint": "关闭则首次只建立基线，不推送"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VSwitch", "props": {"model": "browse_supplement", "label": "同时抓首页位",
                                                                 "hint": "补充首页置顶/老片位，请求更多、噪音略高"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal",
                                "text": "数据源为站点自带 RSS（passkey 授权）与首页浏览，只通知不下载。"
                                        "每站只报新增；本地已有的同名种子、以及带「刷流」等标签的种子会被过滤。"
                                        "面板显示每日新增台账。"}}]},
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": True,
            "cron": "25 8 * * *",
            "interval_minutes": 0,
            "site_ids": [],
            "rows": 50,
            "max_per_site": 8,
            "max_total": 30,
            "skip_local": True,
            "exclude_tags": "刷流",
            "first_notify": True,
            "browse_supplement": False,
        }

    def get_page(self) -> List[dict]:
        summary = self.__stats_summary()
        rows = []
        for name, count in (summary.get("today_by_site") or {}).items():
            rows.append({"component": "div", "props": {"class": "text-body-2 py-1"},
                         "text": f"📺 {name}：今日新增 {count} 个"})
        if not rows:
            rows.append({"component": "div", "props": {"class": "text-body-2 py-1"},
                         "text": "📺 今日暂无新增记录"})
        recent_lines = []
        for item in (self.get_data("feed_recent") or [])[:12]:
            recent_lines.append(f"- [{item.get('site')}] {item.get('title')} · {item.get('time')}")
        days_line = "最近 7 天：" + "　".join(
            f"{r['day'][5:]} {r['total']}" for r in summary.get("recent") or []) or "最近 7 天：暂无"
        head = (f"状态：{'运行中' if self._enabled else '未启用'} · "
                f"每站最多 {self._max_per_site} 条 / 单次最多 {self._max_total} 条 · "
                f"本地已有 {'过滤' if self._skip_local else '保留'} · "
                f"排除标签「{self._exclude_tags}」 · 最近运行 {summary.get('last_run') or '-'}")
        stat_line = (f"📊 今日新增 {summary.get('today_total', 0)} 个 · "
                     f"累计 {summary.get('total', 0)} 个 · 统计天数 {summary.get('days', 0)} 天")
        return [{
            "component": "VCard",
            "content": [{
                "component": "VCardText",
                "props": {"class": "pa-4"},
                "content": [
                    {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold mb-2"},
                     "text": "站点更新播报 · 每日新增台账"},
                    {"component": "div", "props": {"class": "text-body-2 mb-1"}, "text": head},
                    {"component": "div", "props": {"class": "text-subtitle-2 font-weight-bold mt-3 mb-1"},
                     "text": stat_line},
                    {"component": "div", "props": {"class": "text-body-2 mb-1"}, "text": days_line},
                    *rows,
                    {"component": "div", "props": {"class": "text-subtitle-2 font-weight-bold mt-3 mb-1"},
                     "text": "最近播报"},
                    {"component": "div", "props": {"class": "text-body-2 text-muted"},
                     "text": "\n".join(recent_lines) if recent_lines else "暂无"},
                    {"component": "div", "props": {"class": "text-caption mt-3"},
                     "text": "只通知不下载：插件不会添加任何下载任务。数据源为站点 RSS 与首页浏览，"
                             "按天差集只播新增，本地已有与刷流种默认过滤。"},
                ],
            }],
        }]
