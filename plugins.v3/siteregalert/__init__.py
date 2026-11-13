# -*- coding: utf-8 -*-
"""
站点开注警报 SiteRegAlert
持续监测 IYUU 28 个合作站点的注册页，仅在「关闭 / 满额 / 被拦截 -> 开放注册」时
推送一条独立的钉钉特别警报（@所有人 + 直达注册链接 + 辅种激活提示），不重复刷屏。
"""
import base64
import hashlib
import hmac
import html as html_lib
import json
import re
import threading
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from apscheduler.triggers.interval import IntervalTrigger

from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType

# (下拉简称, 中文站名, IYUU sid, 注册页候选地址)
_BUILTIN_SITES = [
    ("pthome", "铂金家", 2, ["https://pthome.net/signup.php"]),
    ("m-team", "馒头", 3, ["https://api.m-team.cc/signup.php", "https://m-team.cc/signup.php"]),
    ("hdhome", "家园", 7, ["https://hdhome.org/signup.php"]),
    ("ourbits", "我堡", 9, ["https://ourbits.club/signup.php"]),
    ("chdbits", "彩虹岛", 25, ["https://ptchdbits.co/signup.php"]),
    ("oshen", "奥申", 39, ["https://www.oshen.win/signup.php"]),
    ("discfan", "蝶粉", 40, ["https://discfan.net/signup.php"]),
    ("hdfans", "红豆饭", 57, ["https://hdfans.org/signup.php"]),
    ("hitpt", "百川", 59, ["https://www.hitpt.com/signup.php"]),
    ("audiences", "观众", 68, ["https://audiences.me/signup.php"]),
    ("piggo", "猪猪", 69, ["https://api.piggo.me/signup.php"]),
    ("pt0ffcc", "自由农场", 75, ["https://pt.0ff.cc/signup.php"]),
    ("zhuque", "朱雀", 80, ["https://zhuque.in/entry/register", "https://zhuque.in/signup.php"]),
    ("zmpt", "织梦", 81, ["https://zmpt.cc/signup.php"]),
    ("agsvpt", "末日", 93, ["https://www.agsvpt.com/signup.php"]),
    ("ptvicomo", "象岛", 94, ["https://ptvicomo.net/signup.php"]),
    ("xingtan", "杏坛", 96, ["https://xingtan.one/signup.php"]),
    ("hdkyl", "麒麟", 97, ["https://www.hdkyl.in/signup.php"]),
    ("ilolicon", "爱萝莉", 98, ["https://mua.xloli.cc/signup.php"]),
    ("hddolby", "杜比", 105, ["https://www.uhddolby.com/signup.php"]),
    ("ptcafe", "咖啡", 107, ["https://ptcafe.club/signup.php"]),
    ("ptlgs", "劳改所", 110, ["https://ptlgs.org/signup.php"]),
    ("raingfh", "雨", 112, ["https://raingfh.top/signup.php"]),
    ("ptzone", "PTzone", 114, ["https://ptzone.xyz/signup.php"]),
    ("ptlover", "AFun", 117, ["https://www.ptlover.cc/signup.php"]),
    ("cspt", "财神", 120, ["https://cspt.top/signup.php"]),
    ("ptskit", "PTSKIT", 133, ["https://www.ptskit.org/signup.php"]),
    ("baozi", "包子PT", 143, ["https://p.t-baozi.cc/signup.php"]),
]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
}


class SiteRegAlert(_PluginBase):
    """IYUU 合作站点开放注册监控 + 钉钉特别警报"""

    plugin_name = "站点开注警报"
    plugin_desc = "盯住 IYUU 28 个合作站注册页，一有站点开放注册就发 @所有人 的钉钉特别警报，不重复刷屏。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.0"
    plugin_author = "WorkBuddy"
    author_url = ""
    plugin_config_prefix = "siteregalert_"
    plugin_order = 32
    auth_level = 1

    DINGTALK_SEND_URL = "https://oapi.dingtalk.com/robot/send"

    # 明确关闭注册的文案（命中即不开放，优先于表单判断）
    CLOSED_KEYWORDS = (
        "自由注册当前关闭", "只允许邀请注册", "仅允许邀请注册", "仅开放邀请注册",
        "当前只允许邀请", "注册当前关闭", "关闭自由注册", "暂不开放注册",
        "注册已关闭", "尚未开放注册", "暂停注册", "停止注册",
        "invite only", "invitation only", "registration is closed",
        "registration closed", "signups are closed", "signup is closed",
    )
    # 满额暂闭（会周期性清理不活跃账号后重开，最值得盯）
    FULL_KEYWORDS = (
        "账户上限数已到", "账号上限数已到", "账户数已达上限", "用户数已达上限",
        "系统会经常删除不活动", "user limit", "account limit", "maximum users",
    )
    # 反爬拦截
    BLOCK_KEYWORDS = (
        "just a moment", "cf-challenge", "cf-browser-verification", "attention required",
        "checking your browser", "access denied", "403 forbidden", "cloudflare ray id",
    )
    # 明确开放的文案
    OPEN_TEXT = (
        "开放注册", "开放自由注册", "自由注册已开启", "现已开放注册", "注册现已开放",
        "registration is open", "signups are open", "free registration", "open registration",
    )
    # 注册表单专用字段（登录页一般没有，用来避免把登录页误判成开放注册）
    STRONG_FORM = (
        r'name=["\']?wantusername', r'name=["\']?wantpassword', r'name=["\']?wantemail',
        r'name=["\']?passwordagain', r'name=["\']?confirm_password',
        r'name=["\']?password_confirmation', r'name=["\']?email\b',
        r'id=["\']?wantusername', r'id=["\']?wantpassword', r'type=["\']?email',
    )
    WEAK_FORM = (
        r'name=["\']?username', r'type=["\']?password', r'用户名', r'密码',
        r'邮箱', r'验证码', r'确认密码', r'confirm',
    )

    STATUS_LABEL = {
        "open": "✅ 开放注册",
        "full": "⏳ 满额暂闭",
        "closed": "🔒 仅邀请注册",
        "blocked": "🌐 访问被拦截",
        "unknown": "❓ 无法判定",
    }

    # 部分站点（蝶粉/PTzone 等）用繁体中文，统一折算后再匹配关键词
    _TRAD2SIMP = str.maketrans({
        "註": "注", "冊": "册", "當": "当", "關": "关", "閉": "闭",
        "許": "许", "請": "请", "帳": "账", "戶": "户", "數": "数",
        "暫": "暂", "開": "开", "啟": "启", "動": "动", "後": "后",
        "來": "来", "認": "认", "為": "为", "這": "这", "個": "个",
        "們": "们", "時": "时", "會": "会", "經": "经", "過": "过",
        "進": "进", "種": "种", "體": "体", "僅": "仅", "刪": "删",
        "除": "除", "活": "活", "躍": "跃", "帳": "账", "點": "点",
    })

    # ---------------- 配置 ----------------
    _enabled = False
    _interval = 30
    _timeout = 12
    _dingtalk_token = ""
    _dingtalk_secret = ""
    _at_all = True
    _notify_mp = False
    _extra_sites = ""
    _onlyonce = False
    _test_notify = False
    _renotify_hours = 6

    _lock = threading.Lock()
    _statuses: Dict[str, Dict[str, Any]] = {}
    _baseline_done = False

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._interval = self.__safe_int(config.get("interval"), 30, min_value=5, max_value=720)
        self._timeout = self.__safe_int(config.get("timeout"), 12, min_value=3, max_value=60)
        self._dingtalk_token = self.__clean_token(config.get("dingtalk_token"))
        self._dingtalk_secret = (config.get("dingtalk_secret") or "").strip()
        self._at_all = bool(config.get("at_all", True))
        self._notify_mp = bool(config.get("notify_mp", False))
        self._extra_sites = config.get("extra_sites") or ""
        self._onlyonce = bool(config.get("onlyonce", False))
        self._test_notify = bool(config.get("test_notify", False))

        state = self.get_data("reg_state") or {}
        self._statuses = state.get("statuses") or {}
        self._baseline_done = bool(state.get("baseline_done"))

        if self._test_notify:
            self._test_notify = False
            self.update_config(self.__config_snapshot())
            threading.Thread(target=self.send_test, daemon=True).start()

        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self.__config_snapshot())
            threading.Thread(target=self.__run_safe, daemon=True).start()

        logger.info(
            f"站点开注警报初始化完成：启用={self._enabled}，间隔={self._interval}分钟，"
            f"监控站点={len(self.__targets())}个，钉钉特别通知={'已配置' if self._dingtalk_token else '未配置'}"
        )

    def __config_snapshot(self) -> Dict[str, Any]:
        """写回配置时必须带全字段，避免 PUT 时把其余配置清空"""
        return {
            "enabled": self._enabled,
            "interval": self._interval,
            "timeout": self._timeout,
            "dingtalk_token": self._dingtalk_token,
            "dingtalk_secret": self._dingtalk_secret,
            "at_all": self._at_all,
            "notify_mp": self._notify_mp,
            "extra_sites": self._extra_sites,
            "onlyonce": False,
            "test_notify": False,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/SiteRegAlert/run",
                "endpoint": self.__run_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即检查一轮站点注册状态（开放会发特别警报）",
            },
            {
                "path": "/SiteRegAlert/test",
                "endpoint": self.__test_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "发送一条特别通知样例，验证钉钉链路",
            },
        ]

    def __run_api(self, api_result=None):
        return api_result or {"success": True, "data": self.check_once()}

    def __test_api(self, api_result=None):
        return api_result or {"success": True, "data": self.send_test()}

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [{
            "id": "SiteRegAlert",
            "name": "站点开注警报轮询",
            "trigger": IntervalTrigger(minutes=self._interval),
            "func": self.__run_safe,
            "kwargs": {},
        }]

    def stop_service(self):
        pass

    def __run_safe(self):
        try:
            self.check_once()
        except Exception as err:
            logger.error(f"站点开注警报检查异常：{err}")

    # ---------------- 站点清单 ----------------
    def __targets(self) -> List[Dict[str, Any]]:
        items = [
            {"site": s, "name": n, "sid": sid, "urls": list(urls), "iyuu": True}
            for s, n, sid, urls in _BUILTIN_SITES
        ]
        raw = (self._extra_sites or "").strip()
        if not raw:
            return items
        # 支持每行「名称|https://xxx/signup.php」，也兼容 JSON 数组
        if raw.startswith("["):
            try:
                for i, obj in enumerate(json.loads(raw)):
                    if isinstance(obj, dict) and obj.get("url"):
                        items.append({
                            "site": f"custom_{i}", "name": obj.get("name") or obj["url"],
                            "sid": "", "urls": [obj["url"]], "iyuu": False,
                        })
            except Exception as err:
                logger.warn(f"站点开注警报：自定义站点 JSON 解析失败：{err}")
            return items
        for i, line in enumerate(raw.splitlines()):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "|" in line:
                name, url = line.split("|", 1)
                name, url = name.strip(), url.strip()
            else:
                name, url = line, line
            if url.startswith("http"):
                items.append({
                    "site": f"custom_{i}", "name": name, "sid": "",
                    "urls": [url], "iyuu": False,
                })
        return items

    # ---------------- 检测 ----------------
    @staticmethod
    def __plain(body: str) -> str:
        text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", body or "")
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = html_lib.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    def __judge(self, code: int, body: str) -> Tuple[str, str]:
        """返回 (status, detail)。宁可判 unknown，也不误报开放"""
        if code != 200:
            return "blocked", f"HTTP {code}"
        text = self.__plain(body)
        low = text.lower().translate(self._TRAD2SIMP)
        if any(k in low for k in self.BLOCK_KEYWORDS):
            return "blocked", "触发 Cloudflare / 反爬拦截"
        for k in self.FULL_KEYWORDS:
            if k in low:
                return "full", "账户数满额暂闭（站点清理不活跃账号后会重开）"
        for k in self.CLOSED_KEYWORDS:
            if k in low:
                return "closed", f"页面明确提示「{k}」"
        strong = sum(1 for p in self.STRONG_FORM if re.search(p, body, re.I))
        weak = sum(1 for p in self.WEAK_FORM if re.search(p, body, re.I))
        if strong >= 2:
            return "open", f"检测到注册表单（{strong} 个注册专用字段），且无关闭文案"
        hit = next((k for k in self.OPEN_TEXT if k in low), None)
        if hit and (strong >= 1 or weak >= 3):
            return "open", f"出现「{hit}」且存在注册表单"
        return "unknown", "页面可访问，但未发现注册表单（可能是跳转页 / 需浏览器确认）"

    def __check_site(self, item: Dict[str, Any]) -> Tuple[str, str, str]:
        best: Optional[Tuple[str, str, str]] = None
        for url in item["urls"]:
            try:
                res = requests.get(
                    url, headers=_HEADERS, timeout=self._timeout,
                    allow_redirects=True, verify=False,
                )
                status, detail = self.__judge(res.status_code, res.text or "")
            except Exception as err:
                status, detail = "blocked", f"请求失败：{type(err).__name__}"
            if status == "open":
                return "open", detail, url
            if best is None or (best[0] == "blocked" and status != "blocked"):
                best = (status, detail, url)
            time.sleep(0.4)
        return best or ("blocked", "无响应", item["urls"][0])

    def check_once(self) -> Dict[str, Any]:
        """检查一轮；只在状态变为开放时推送特别通知"""
        if not self._lock.acquire(blocking=False):
            return {"success": False, "message": "上一轮检查尚未结束"}
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            targets = self.__targets()
            newly = []
            current: Dict[str, Dict[str, Any]] = {}
            summary: Dict[str, int] = {}
            for item in targets:
                site = item["site"]
                status, detail, url = self.__check_site(item)
                old = self._statuses.get(site) or {}
                prev = old.get("status")
                last_notify = float(old.get("last_notify") or 0)
                rec = {
                    "status": status, "detail": detail, "url": url,
                    "last_check": now, "last_notify": last_notify,
                }
                if status == "open":
                    first_found = not self._baseline_done
                    reopened = prev not in (None, "open")
                    expired = time.time() - last_notify > self._renotify_hours * 3600
                    if first_found or reopened or expired:
                        rec["last_notify"] = time.time()
                        newly.append({
                            "site": site, "name": item["name"], "sid": item["sid"],
                            "url": url, "detail": detail, "iyuu": item["iyuu"],
                            "prev": self.STATUS_LABEL.get(prev, "首次检查 / 无历史状态"),
                            "time": now,
                        })
                current[site] = rec
                summary[status] = summary.get(status, 0) + 1
                time.sleep(0.6)

            self._statuses = current
            self._baseline_done = True
            self.save_data("reg_state", {
                "statuses": current, "baseline_done": True,
                "last_check": now, "summary": summary,
            })
            result = {
                "success": True, "last_check": now,
                "total": len(targets), "summary": summary,
                "newly_open": [x["name"] for x in newly],
            }
            if newly:
                ok, msg = self.__send_alert(newly)
                result["alert"] = "sent" if ok else f"failed: {msg}"
                logger.info(f"站点开注警报：{len(newly)} 个站点开放注册，特别通知{'已发送' if ok else '发送失败 ' + msg}")
            else:
                logger.info(f"站点开注警报：本轮检查 {len(targets)} 站，无新增开放（{summary}）")
            return result
        finally:
            self._lock.release()

    # ---------------- 特别通知 ----------------
    @staticmethod
    def __clean_token(raw: Any) -> str:
        token = (raw or "").strip()
        if "access_token=" in token:
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(token).query)
                token = (qs.get("access_token") or [token])[0]
            except Exception:
                pass
        return token

    @staticmethod
    def __dingtalk_sign(secret: str) -> Tuple[str, str]:
        import urllib.parse as _up
        ts = str(round(time.time() * 1000))
        code = hmac.new(
            secret.encode("utf-8"), f"{ts}\n{secret}".encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        return ts, _up.quote_plus(base64.b64encode(code))

    def __dingtalk_send(self, title: str, md: str) -> Tuple[bool, str]:
        if not self._dingtalk_token:
            return False, "未配置钉钉 access_token"
        url = f"{self.DINGTALK_SEND_URL}?access_token={self._dingtalk_token}"
        if self._dingtalk_secret:
            ts, sign = self.__dingtalk_sign(self._dingtalk_secret)
            url += f"&timestamp={ts}&sign={sign}"
        payload: Dict[str, Any] = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": md},
            "at": {"isAtAll": bool(self._at_all)},
        }
        try:
            res = requests.post(url, json=payload, timeout=12)
            ret = res.json()
            if ret.get("errcode") == 0:
                return True, "ok"
            return False, str(ret)
        except Exception as err:
            return False, f"{type(err).__name__}: {err}"

    def __send_alert(self, opens: List[Dict[str, Any]]) -> Tuple[bool, str]:
        title = f"🚨【开注警报】发现 {len(opens)} 个站点开放注册！"
        lines = [
            f"## {title}",
            "",
            "@所有人  **这是蹲开注的特别通知**，只在「关闭 / 满额 / 被拦截 → 开放」时推送一次，不会重复刷屏。" if self._at_all
            else "> 这是蹲开注的特别通知，状态变化时才推送，不会重复刷屏。",
            "",
        ]
        for i, item in enumerate(opens, 1):
            lines += [
                f"### {i}. {item['name']} `{item['site']}`" + (f" · sid {item['sid']}" if item["sid"] else ""),
                f"- **状态变化**：{item['prev']} → ✅ **开放注册**",
                f"- **直达注册页**：[点此立即注册]({item['url']})",
                f"- **判定依据**：{item['detail']}",
                f"- **检测时间**：{item['time']}",
                "",
            ]
        lines += [
            "---",
            "### 下一步：激活辅种",
            "注册成功后，到 IYUUPlus 顶部「**绑定合作站点**」选这个站的简称，填该站 **用户数字ID + passkey** 绑定一次即可激活 TOKEN；"
            "之后 5 个站（含躺平 PT 填错的 passkey 记得修正）就能正常自动辅种。",
            "",
            "> 提示：开放注册常为限时名额，看到消息建议尽快注册。",
        ]
        md = "\n\n".join(x for x in lines if x)
        ok, msg = self.__dingtalk_send(title, md)
        if self._notify_mp:
            try:
                self.post_message(mtype=NotificationType.Plugin, title=title, text=md)
            except Exception as err:
                logger.warn(f"站点开注警报：站内通知发送失败：{err}")
        return ok, msg

    def send_test(self) -> Dict[str, Any]:
        """发一条特别通知样例，方便确认钉钉链路和提醒样式"""
        title = "🚨【开注警报·测试】特别通知已就位"
        lines = [
            "## 🚨【开注警报·测试】特别通知已就位",
            "",
            "@所有人  看到这条，说明「站点开放注册 → 钉钉特别警报」链路已打通。" if self._at_all
            else "> 看到这条，说明「站点开放注册 → 钉钉特别警报」链路已打通。",
            "",
            "### 真正触发时的样子",
            "- **状态变化**：⏳ 满额暂闭 / 🔒 仅邀请注册 → ✅ **开放注册**",
            "- **消息内容**：站点中文名 + 下拉简称 + sid + 直达注册链接 + 判定依据 + 检测时间",
            "- **推送规则**：只在状态变为开放时发一次；持续开放每 6 小时最多提醒一次，不会一直刷屏",
            "- **监控范围**：IYUU 28 个合作站点（下拉里的全部选项）",
            "",
            f"- 样例时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ]
        md = "\n\n".join(lines)
        ok, msg = self.__dingtalk_send(title, md)
        logger.info(f"站点开注警报：测试特别通知{'发送成功' if ok else '发送失败 ' + msg}")
        if self._notify_mp:
            try:
                self.post_message(mtype=NotificationType.Plugin, title=title, text=md)
            except Exception:
                pass
        return {"success": ok, "message": msg, "at_all": self._at_all}

    # ---------------- 界面 ----------------
    @staticmethod
    def __safe_int(value, default: int, min_value=None, max_value=None) -> int:
        try:
            num = int(value)
        except (TypeError, ValueError):
            num = default
        if min_value is not None:
            num = max(num, min_value)
        if max_value is not None:
            num = min(num, max_value)
        return num

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "enabled", "label": "启用监控"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即检查一次"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "test_notify", "label": "发一条测试特别通知"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "at_all", "label": "钉钉 @所有人"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VTextField", "props": {
                                    "model": "interval", "label": "检查间隔（分钟）",
                                    "hint": "默认30分钟；28个站轮询一轮约1-3分钟", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VTextField", "props": {
                                    "model": "timeout", "label": "单站请求超时（秒）"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSwitch", "props": {
                                    "model": "notify_mp", "label": "同时发 MoviePilot 站内消息",
                                    "hint": "开启后若群机器人通知插件在转发消息，钉钉可能收到两条", "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VTextField", "props": {
                                    "model": "dingtalk_token", "label": "钉钉 access_token（或完整 Webhook）",
                                    "placeholder": "粘贴 Webhook 地址或 access_token"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VTextField", "props": {
                                    "model": "dingtalk_secret", "label": "钉钉加签密钥 SEC",
                                    "placeholder": "SECxxxxxxxx"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VTextarea", "props": {
                                    "model": "extra_sites", "label": "追加监控站点（可选，每行一个）",
                                    "placeholder": "站点名称|https://example.com/signup.php",
                                    "hint": "内置已包含 IYUU 下拉里的 28 个合作站；这里可额外加想蹲的其它站",
                                    "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VAlert", "props": {
                                    "type": "info", "variant": "tonal",
                                    "text": "只在「关闭 / 满额暂闭 / 被拦截 / 无法判定 → 开放注册」时推送一条钉钉特别警报（@所有人、直达注册链接、绑定激活辅种提示）。"
                                            "首次运行先建立基线；已开放的站持续开放时每6小时最多提醒一次，不会刷屏。"
                                            "监测使用未登录状态访问，不会带站点 Cookie。"}}]},
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "interval": 30,
            "timeout": 12,
            "dingtalk_token": "",
            "dingtalk_secret": "",
            "at_all": True,
            "notify_mp": False,
            "extra_sites": "",
            "onlyonce": False,
            "test_notify": False,
        }

    def get_page(self) -> List[dict]:
        state = self.get_data("reg_state") or {}
        statuses = state.get("statuses") or {}
        summary = state.get("summary") or {}
        open_cnt = summary.get("open", 0)
        head = (f"状态：{'运行中' if self._enabled else '未启用'} · 每 {self._interval} 分钟检查一轮 · "
                f"共 {len(self.__targets())} 站 · 开放 {open_cnt} / 满额暂闭 {summary.get('full', 0)} / "
                f"仅邀请 {summary.get('closed', 0)} / 被拦截 {summary.get('blocked', 0)} / "
                f"无法判定 {summary.get('unknown', 0)}")
        rows = []
        order = {"open": 0, "unknown": 1, "full": 2, "closed": 3, "blocked": 4}
        items = sorted(self.__targets(), key=lambda x: order.get((statuses.get(x["site"]) or {}).get("status"), 9))
        for item in items:
            rec = statuses.get(item["site"]) or {}
            status = rec.get("status") or "未检查"
            label = self.STATUS_LABEL.get(status, "未检查")
            rows.append({
                "component": "div",
                "props": {"class": "text-body-2 py-1"},
                "text": f"{label}  {item['name']} ({item['site']}) · {rec.get('detail') or '暂无记录'} · {rec.get('last_check') or '-'}",
            })
        return [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-4"},
                        "content": [
                            {"component": "div", "props": {"class": "text-subtitle-1 mb-2"}, "text": head},
                            {"component": "div", "props": {"class": "text-body-2 text-muted mb-2"},
                             "text": f"上次检查：{state.get('last_check') or '尚未检查'} · 特别通知：钉钉机器人"
                                     f"{'@所有人' if self._at_all else ''}"
                                     f"{'（已配置）' if self._dingtalk_token else '（未配置！）'}"},
                        ] + rows,
                    }
                ],
            }
        ]
