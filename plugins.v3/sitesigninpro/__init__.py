# -*- coding: utf-8 -*-
"""
站点签到助手 SiteSigninPro
------------------------------------------------------------
MoviePilot 自带 AutoSignIn 只会 GET attendance.php 然后读页面文案，
遇到「需要点【立即签到】提交 POST 表单」的站点（红豆包、躺平）就永远
显示「登录成功，签到未确认」，实际并没有签到。

本插件真正完成签到：
  1. 从 MoviePilot 站点管理读取 Cookie / UA / 站点地址（不硬编码凭据）
  2. GET 签到页 -> 已签则跳过 -> 未登录则报告 Cookie 失效
  3. 解析签到表单：无验证码直接 POST 提交；有 Cloudflare Turnstile
     则用真实浏览器打开页面、等待验证通过、点击「立即签到」
  4. 以站点返回的「签到成功 / 本次签到获得 / 签到已得N」为准判定结果
  5. 每日定时签到 + 定时补签巡检（只补当天未成功的站，已签不重复提交）
  6. 只在「需要手动处理」时发一条钉钉提醒（按站点按天去重，不刷屏）
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
from urllib.parse import urljoin

import requests
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.log import logger
from app.plugins import _PluginBase

try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}
_DEFAULT_DOMAINS = "hdbao.cc,tangpt.top,crabpt.vip,bilibili.download"
_DINGTALK_SEND_URL = "https://oapi.dingtalk.com/robot/send"


# ----------------------------------------------------------------------
# 纯函数：便于离线用真实页面做回归测试
# ----------------------------------------------------------------------
def plain_text(source: str) -> str:
    """去掉脚本样式与标签，只保留可见文字"""
    if not source:
        return ""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", source)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html_lib.unescape(text)).strip()


def is_signed(source: str) -> bool:
    """只认站点返回的当天签到凭据，绝不把按钮/说明文字当成成功"""
    text = plain_text(source)
    if not text:
        return False
    if re.search(r"签\s*到\s*已\s*得\s*[\d,]+", text):
        return True
    if "签到成功" in text and "本次签到获得" in text:
        return True
    if re.search(r"You have already attended", text, re.IGNORECASE):
        return True
    return False


def is_logged_in(source: str) -> bool:
    if not source:
        return False
    if re.search(r"(?i)<input[^>]*type\s*=\s*['\"]password", source):
        return False
    return bool(re.search(r"欢迎回来|logout\.php|userdetails\.php\?id=", source))


def extract_reward(source: str) -> str:
    text = plain_text(source)
    m = re.search(r"本次签到获得\s*([\d,]+)\s*个?\s*([^\s，。；]{0,10})", text)
    if m:
        unit = m.group(2).strip() or "魔力值"
        return f"本次获得 {m.group(1)} {unit}"
    m = re.search(r"签\s*到\s*已\s*得\s*([\d,]+)", text)
    return f"签到已得 {m.group(1)}" if m else ""


def find_sign_form(source: str, base_url: str) -> Optional[Dict[str, Any]]:
    """找出含「立即签到」提交按钮的 POST 表单及其隐藏字段"""
    if not source:
        return None
    for match in re.finditer(r"(?is)<form\b[^>]*>.*?</form>", source):
        raw = match.group(0)
        tag = raw[:raw.find(">") + 1]
        if re.search(r"(?i)method\s*=\s*['\"]get", tag):
            continue
        if not re.search(r"(?i)value\s*=\s*['\"]\s*(立即签到|立即簽到|签到|簽到|打卡|check\s*in)", raw):
            continue
        action = re.search(r"(?i)action\s*=\s*['\"]([^'\"]+)", tag)
        target = urljoin(base_url, action.group(1) if action else base_url)
        fields: Dict[str, str] = {}
        for inp in re.findall(r"(?is)<input\b[^>]*>", raw):
            if not re.search(r"(?i)type\s*=\s*['\"]hidden", inp):
                continue
            name = re.search(r"(?i)\bname\s*=\s*['\"]([^'\"]+)", inp)
            value = re.search(r"(?i)\bvalue\s*=\s*['\"]([^'\"]*)", inp)
            if name:
                fields[name.group(1)] = value.group(1) if value else ""
        return {
            "action": target,
            "fields": fields,
            "turnstile": bool(re.search(r"turnstile|cf-turnstile|captcha", source, re.IGNORECASE)),
            "raw": raw,
        }
    return None


def parse_cookie(raw: str) -> List[Dict[str, str]]:
    """把 'k=v; k2=v2' 解析成 playwright 需要的 cookie 列表"""
    out = []
    for part in (raw or "").split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key.strip():
            out.append({"name": key.strip(), "value": value.strip()})
    return out


class SiteSigninPro(_PluginBase):
    """真正提交签到表单的站点签到助手"""

    plugin_name = "站点签到助手"
    plugin_desc = "站点下拉多选（自动包含新增站点），真正提交「立即签到」表单（含 Cloudflare 人机验证站点），按站点返回的奖励文案判定签到结果"
    plugin_icon = "check.png"
    plugin_version = "1.1.0"
    plugin_author = "WorkBuddy"
    plugin_config_prefix = "sitesigninpro_"
    plugin_order = 33
    auth_level = 1

    # 运行参数
    _enabled = False
    _onlyonce = False
    _test_notify = False
    _timeout = 20
    _cron_hour = 7
    _cron_minute = 55
    _retry_minutes = 60
    _domains_text = _DEFAULT_DOMAINS
    _sign_sites: List[Any] = []
    _ALL_SITES = "__all__"
    _manual_cookies = ""
    _dingtalk_token = ""
    _dingtalk_secret = ""
    _notify_fail = True
    _notify_daily = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    @staticmethod
    def __clean_token(raw: Any) -> str:
        token = str(raw or "").strip()
        if "access_token=" in token:
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(token).query)
                token = (qs.get("access_token") or [token])[0]
            except Exception:
                pass
        return token

    @staticmethod
    def __safe_int(value, default: int, min_value=None, max_value=None) -> int:
        try:
            num = int(str(value).strip())
        except Exception:
            return default
        if min_value is not None:
            num = max(min_value, num)
        if max_value is not None:
            num = min(max_value, num)
        return num

    def __config_snapshot(self) -> Dict[str, Any]:
        """写回配置必须带全字段，避免清掉其余配置"""
        return {
            "enabled": self._enabled,
            "onlyonce": False,
            "test_notify": False,
            "timeout": self._timeout,
            "cron_hour": self._cron_hour,
            "cron_minute": self._cron_minute,
            "retry_minutes": self._retry_minutes,
            "domains": self._domains_text,
            "sign_sites": self._sign_sites,
            "manual_cookies": self._manual_cookies,
            "dingtalk_token": self._dingtalk_token,
            "dingtalk_secret": self._dingtalk_secret,
            "notify_fail": self._notify_fail,
            "notify_daily": self._notify_daily,
        }

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._test_notify = bool(config.get("test_notify", False))
        self._timeout = self.__safe_int(config.get("timeout"), 20, 8, 90)
        self._cron_hour = self.__safe_int(config.get("cron_hour"), 7, 0, 23)
        self._cron_minute = self.__safe_int(config.get("cron_minute"), 55, 0, 59)
        self._retry_minutes = self.__safe_int(config.get("retry_minutes"), 60, 15, 720)
        self._domains_text = config.get("domains") or _DEFAULT_DOMAINS
        self._sign_sites = config.get("sign_sites") or []
        # 过滤掉已从站点管理删除的站点 ID，避免残留配置指向不存在的站
        if self._sign_sites and self._ALL_SITES not in self._sign_sites:
            valid = {str(o["value"]) for o in self.__site_options()
                     if o["value"] != self._ALL_SITES}
            self._sign_sites = [x for x in self._sign_sites if str(x) in valid]
        self._manual_cookies = config.get("manual_cookies") or ""
        self._dingtalk_token = self.__clean_token(config.get("dingtalk_token"))
        self._dingtalk_secret = str(config.get("dingtalk_secret") or "").strip()
        self._notify_fail = bool(config.get("notify_fail", True))
        self._notify_daily = bool(config.get("notify_daily", False))

        run_now = bool(config.get("onlyonce"))
        test_now = bool(config.get("test_notify"))
        if run_now or test_now:
            self._onlyonce = False
            self._test_notify = False
            self.update_config(self.__config_snapshot())
        if run_now:
            threading.Thread(target=self.__run_safe, kwargs={"force": True}, daemon=True).start()
        if test_now:
            threading.Thread(target=self.__send_test, daemon=True).start()

        logger.info(
            f"站点签到助手初始化完成：启用={self._enabled}，每日 {self._cron_hour:02d}:{self._cron_minute:02d}，"
            f"补签巡检每 {self._retry_minutes} 分钟，目标站点={len(self.__sites())} 个"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/SiteSigninPro/run",
                "endpoint": self.__run_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行一轮站点签到",
            },
            {
                "path": "/SiteSigninPro/test",
                "endpoint": self.__test_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "发送一条钉钉测试通知",
            },
        ]

    def __run_api(self, api_result=None):
        return api_result or {"success": True, "data": self.run_once(force=True)}

    def __test_api(self, api_result=None):
        return api_result or {"success": True, "data": self.send_test()}

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [
            {
                "id": "SiteSigninProDaily",
                "name": "站点签到助手-每日签到",
                "trigger": CronTrigger(hour=self._cron_hour, minute=self._cron_minute),
                "func": self.__run_safe,
                "kwargs": {"force": True},
            },
            {
                "id": "SiteSigninProRetry",
                "name": "站点签到助手-补签巡检",
                "trigger": IntervalTrigger(minutes=self._retry_minutes),
                "func": self.__run_safe,
                "kwargs": {"force": False},
            },
        ]

    def stop_service(self):
        pass

    def __run_safe(self, force: bool = False):
        try:
            self.run_once(force=force)
        except Exception as err:
            logger.error(f"站点签到助手运行异常：{err}")

    # ------------------------------------------------------------------
    # 站点数据来源：MoviePilot 站点管理
    # ------------------------------------------------------------------
    def __targets(self) -> List[str]:
        """旧版域名配置兜底；新版优先使用站点多选 ID"""
        return [x.strip().lower() for x in re.split(r"[,，\s]+", self._domains_text or "") if x.strip()]

    @classmethod
    def __site_rows(cls) -> List[Any]:
        """从站点管理读取全部站点对象（含 ID），多版本 MoviePilot 兼容"""
        SiteOper = None
        for module in ("app.db.oper.site", "app.db.siteoper", "app.db.oper"):
            try:
                mod = __import__(module, fromlist=["SiteOper"])
                SiteOper = getattr(mod, "SiteOper", None)
                if SiteOper:
                    break
            except Exception:
                continue
        if not SiteOper:
            return []
        try:
            return list(SiteOper().list_order_by_pri() or [])
        except Exception as err:
            logger.warn(f"站点签到助手读取站点管理失败：{err}")
            return []

    @classmethod
    def __site_options(cls) -> List[Dict[str, Any]]:
        """配置页下拉项：全部 + 站点管理里的每一个站点（新增站点自动出现）"""
        options = [{"title": "全部站点（含以后新增）", "value": cls._ALL_SITES}]
        for item in cls.__site_rows():
            sid = cls.__field(item, "id")
            name = cls.__field(item, "name")
            if sid is not None:
                options.append({"title": str(name or sid), "value": sid})
        return options

    def __selected_ids(self) -> List[str]:
        """展开「全部站点」，返回当前选中的站点 ID（统一成字符串比较）"""
        if self._ALL_SITES in (self._sign_sites or []):
            return [str(o["value"]) for o in self.__site_options()
                    if o["value"] != self._ALL_SITES]
        return [str(x) for x in (self._sign_sites or [])]

    @staticmethod
    def __field(obj, key: str):
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def __sites(self) -> List[Dict[str, Any]]:
        """按配置页多选的站点 ID 取站点；未配置多选时回落旧版域名列表"""
        manual: Dict[str, str] = {}
        for line in (self._manual_cookies or "").splitlines():
            if "|" in line:
                domain, cookie = line.split("|", 1)
                manual[domain.strip().lower()] = cookie.strip()

        raw = self.__site_rows()
        if not raw:
            logger.warn("站点签到助手未能读取站点管理，只能使用手工 Cookie 配置")

        use_ids = bool(self._sign_sites)
        selected = set(self.__selected_ids())
        targets = set(self.__targets())

        sites: List[Dict[str, Any]] = []
        seen = set()
        for item in raw:
            sid = str(self.__field(item, "id") or "")
            domain = str(self.__field(item, "domain") or "").strip().lower()
            domain = re.sub(r"^https?://", "", domain).strip("/")
            if not domain or domain in seen:
                continue
            if use_ids:
                if sid not in selected:
                    continue
            elif domain not in targets:
                continue
            cookie = str(self.__field(item, "cookie") or "").strip() or manual.get(domain, "")
            url = str(self.__field(item, "url") or ("https://" + domain + "/")).strip()
            sites.append({
                "domain": domain,
                "name": str(self.__field(item, "name") or domain),
                "url": url,
                "cookie": cookie,
                "ua": str(self.__field(item, "ua") or "").strip() or _UA,
            })
            seen.add(domain)
        # 手工 Cookie 里额外追加的站点始终纳入，方便临时加站
        for domain, cookie in manual.items():
            if domain and domain not in seen:
                sites.append({
                    "domain": domain, "name": domain,
                    "url": "https://" + domain + "/", "cookie": cookie, "ua": _UA,
                })
        return sites

    def __http(self, site: Dict[str, Any], method: str, url: str,
               data: Dict[str, str] = None):
        headers = dict(_HEADERS)
        headers["User-Agent"] = site.get("ua") or _UA
        if site.get("cookie"):
            headers["Cookie"] = site["cookie"]
        if method == "POST":
            headers["Referer"] = url
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            return requests.post(url, headers=headers, data=data or {}, timeout=self._timeout,
                                 allow_redirects=True, verify=False)
        return requests.get(url, headers=headers, timeout=self._timeout,
                            allow_redirects=True, verify=False)

    # ------------------------------------------------------------------
    # 浏览器流程：过 Cloudflare Turnstile 并点击「立即签到」
    # ------------------------------------------------------------------
    def __browser_callback(self, page) -> Dict[str, Any]:
        token = ""
        for _ in range(25):
            try:
                token = page.evaluate(
                    "() => { const e = document.querySelector('input[name=\"cf-turnstile-response\"]');"
                    " if (e && e.value) return e.value;"
                    " try { return (window.turnstile && window.turnstile.getResponse)"
                    " ? (window.turnstile.getResponse() || '') : ''; } catch (x) { return ''; } }"
                ) or ""
            except Exception:
                token = ""
            if token:
                break
            try:
                page.wait_for_timeout(1000)
            except Exception:
                break
        try:
            source = page.content()
        except Exception as err:
            return {"error": f"读取页面失败：{err}", "token": bool(token)}
        if is_signed(source):
            return {"html": source, "token": bool(token), "method": "浏览器（已签）"}
        clicked = False
        for selector in ('input[type="submit"][value*="签到"]',
                         'input[type="submit"][value*="簽到"]',
                         'button:has-text("签到")', 'button:has-text("簽到")'):
            try:
                locator = page.locator(selector)
                if locator.count():
                    locator.first.click(timeout=8000)
                    clicked = True
                    break
            except Exception:
                continue
        if not clicked:
            return {"error": "页面未找到「立即签到」按钮", "token": bool(token), "html": source}
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            pass
        try:
            page.wait_for_timeout(2000)
            return {"html": page.content(), "token": bool(token), "method": "浏览器点击"}
        except Exception as err:
            return {"error": f"点击后读取失败：{err}", "token": bool(token)}

    def __browser_sign(self, site: Dict[str, Any], url: str) -> Dict[str, Any]:
        """优先用 playwright（Cookie 只注入本站域），再退回 MP 自带浏览器助手"""
        callback = self.__browser_callback
        try:
            from playwright.sync_api import sync_playwright
            last = ""
            for headless in (True, False):
                browser = None
                try:
                    with sync_playwright() as pw:
                        browser = pw.chromium.launch(
                            headless=headless,
                            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
                        )
                        context = browser.new_context(
                            user_agent=site.get("ua") or _UA,
                            ignore_https_errors=True,
                            locale="zh-CN",
                        )
                        cookies = []
                        for ck in parse_cookie(site.get("cookie")):
                            cookies.append({"name": ck["name"], "value": ck["value"],
                                            "url": site["url"]})
                        if cookies:
                            context.add_cookies(cookies)
                        page = context.new_page()
                        page.add_init_script(
                            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
                        )
                        page.goto(url, wait_until="domcontentloaded", timeout=self._timeout * 1000)
                        result = callback(page)
                        browser.close()
                        browser = None
                    if result:
                        logger.info(f"{site['name']} 浏览器签到完成（headless={headless}）")
                        return result
                except Exception as err:
                    last = f"{type(err).__name__}: {err}"
                    if browser:
                        try:
                            browser.close()
                        except Exception:
                            pass
                    logger.warn(f"{site['name']} playwright headless={headless} 失败：{str(err)[:160]}")
            if last:
                logger.warn(f"{site['name']} playwright 均失败，尝试 MoviePilot 浏览器助手：{last[:160]}")
        except Exception as err:
            logger.warn(f"{site['name']} 未加载 playwright：{err}")

        for module in ("app.adapters.network.browser", "app.helper.browser"):
            try:
                mod = __import__(module, fromlist=["PlaywrightHelper"])
                helper_cls = getattr(mod, "PlaywrightHelper", None)
                if not helper_cls:
                    continue
                result = helper_cls().action(url=url, callback=callback,
                                             cookies=site.get("cookie"), ua=site.get("ua"),
                                             timeout=self._timeout)
                if result:
                    return result
            except Exception as err:
                logger.warn(f"{site['name']} {module} 浏览器助手失败：{str(err)[:160]}")
        return {"error": "浏览器组件不可用或人机验证未通过"}

    # ------------------------------------------------------------------
    # 单站签到
    # ------------------------------------------------------------------
    def __sign_site(self, site: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now().strftime("%H:%M:%S")
        base = {"site": site["name"], "domain": site["domain"], "time": now,
                "url": urljoin(site["url"].rstrip("/") + "/", "attendance.php")}
        if not site.get("cookie"):
            return {**base, "status": "error", "detail": "站点管理里没有 Cookie（或手工 Cookie 未填）"}
        att = base["url"]
        try:
            res = self.__http(site, "GET", att)
            if res is None or res.status_code != 200:
                # 学校等没有签到功能的站点直接跳过，不算失败、不发钉钉
                if res is not None and res.status_code == 404:
                    return {**base, "status": "unsupported",
                            "detail": "该站没有签到页（HTTP 404），已自动跳过"}
                return {**base, "status": "error",
                        "detail": f"打开签到页失败 HTTP {getattr(res, 'status_code', '无响应')}"}
            source = res.text
            if not is_logged_in(source):
                return {**base, "status": "error", "detail": "Cookie 已失效，请在站点管理更新"}
            if is_signed(source):
                return {**base, "status": "already", "method": "GET 确认",
                        "reward": extract_reward(source), "detail": "今日已签到（站点返回成功凭据）"}

            form = find_sign_form(source, att)
            if not form:
                # 少数站点 GET 即签，追加一次带随机参数的请求再判定
                retry_url = att + ("&" if "?" in att else "?") + "_=" + str(int(time.time()))
                res2 = self.__http(site, "GET", retry_url)
                if res2 is not None and is_signed(res2.text):
                    return {**base, "status": "success", "method": "GET 触发签到",
                            "reward": extract_reward(res2.text), "detail": "GET 签到成功"}
                if not re.search(r"签\s*到|簽\s*到|attendance|check.?in", plain_text(source), re.I):
                    return {**base, "status": "unsupported",
                            "detail": "该站没有签到功能，已自动跳过"}
                return {**base, "status": "unknown", "detail": "签到页没有表单，也没有成功凭据"}

            if not form["turnstile"]:
                post = self.__http(site, "POST", form["action"], form["fields"])
                if post is not None and is_signed(post.text):
                    return {**base, "status": "success", "method": "POST 提交表单",
                            "reward": extract_reward(post.text), "detail": "已提交「立即签到」并确认奖励"}
                hint = plain_text(post.text if post is not None else "")[:120]
                return {**base, "status": "error", "method": "POST 提交表单",
                        "detail": f"表单提交后未确认成功：{hint or '空响应'}"}

            # 有 Turnstile：真实浏览器过验证并点击
            result = self.__browser_sign(site, att)
            if result.get("html") and is_signed(result["html"]):
                return {**base, "status": "success", "method": result.get("method") or "浏览器点击",
                        "reward": extract_reward(result["html"]),
                        "detail": "浏览器通过人机验证并提交签到"}
            detail = result.get("error") or "人机验证未通过，未拿到签到凭据"
            if result.get("token") is False:
                detail += "（Turnstile 未返回 token）"
            return {**base, "status": "manual", "method": "浏览器", "detail": detail}
        except Exception as err:
            return {**base, "status": "error", "detail": f"{type(err).__name__}: {str(err)[:120]}"}

    # ------------------------------------------------------------------
    # 通知（钉钉加签）
    # ------------------------------------------------------------------
    @staticmethod
    def __dingtalk_sign(secret: str) -> Tuple[str, str]:
        ts = str(round(time.time() * 1000))
        code = hmac.new(secret.encode("utf-8"), f"{ts}\n{secret}".encode("utf-8"),
                        digestmod=hashlib.sha256).digest()
        return ts, urllib.parse.quote_plus(base64.b64encode(code))

    def __dingtalk_send(self, title: str, md: str) -> Tuple[bool, str]:
        if not self._dingtalk_token:
            return False, "未配置钉钉 access_token"
        url = f"{_DINGTALK_SEND_URL}?access_token={self._dingtalk_token}"
        if self._dingtalk_secret:
            ts, sign = self.__dingtalk_sign(self._dingtalk_secret)
            url += f"&timestamp={ts}&sign={sign}"
        try:
            res = requests.post(url, json={"msgtype": "markdown",
                                           "markdown": {"title": title, "text": md}},
                                timeout=12)
            ret = res.json()
            return (True, "ok") if ret.get("errcode") == 0 else (False, str(ret))
        except Exception as err:
            return False, f"{type(err).__name__}: {err}"

    def __send_test(self):
        ok, msg = self.__dingtalk_send(
            "站点签到助手测试通知",
            "### ✅ 站点签到助手已就绪\n\n"
            "这是一条测试通知。每天会自动提交「立即签到」表单；\n"
            "只有当某站需要人工处理（Cookie 失效 / 人机验证没过）时才会再发提醒。",
        )
        logger.info(f"站点签到助手测试通知：{ok} {msg}")

    def send_test(self) -> Dict[str, Any]:
        ok, msg = self.__dingtalk_send(
            "站点签到助手测试通知",
            "### ✅ 站点签到助手已就绪\n\n"
            "这是一条测试通知。每天会自动提交「立即签到」表单；\n"
            "只有当某站需要人工处理（Cookie 失效 / 人机验证没过）时才会再发提醒。",
        )
        return {"success": ok, "message": msg}

    def __notify(self, state: Dict[str, Any], results: List[Dict[str, Any]], daily: bool):
        problems = [r for r in results if r.get("status") in ("manual", "error", "unknown")]
        notified = set(state.get("notified") or [])
        fresh = [r for r in problems if f"{r['domain']}:{r['status']}" not in notified]
        if self._notify_fail and fresh:
            lines = [f"- **{r['site']}**：{r['detail']}\n  [点此手动签到]({r['url']})" for r in fresh]
            md = ("### ⚠️ 有站点没能自动签到\n\n"
                  + "\n".join(lines)
                  + f"\n\n> {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
                    "插件会在下一次巡检自动重试，成功后不再提醒。")
            ok, msg = self.__dingtalk_send("站点签到需要手动处理", md)
            logger.info(f"站点签到助手失败通知：{ok} {msg}")
            for r in fresh:
                notified.add(f"{r['domain']}:{r['status']}")
            state["notified"] = sorted(notified)
        if daily and self._notify_daily and not state.get("daily_notified"):
            ok_rows = [r for r in results if r.get("status") in ("success", "already")]
            md = ("### ✅ 今日站点签到日报\n\n"
                  + "\n".join(
                      f"- {r['site']}：{r.get('reward') or r['detail']}（{r.get('method') or '-'}）"
                      for r in results)
                  + f"\n\n> 成功 {len(ok_rows)}/{len(results)} · "
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
            self.__dingtalk_send("站点签到日报", md)
            state["daily_notified"] = True

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def run_once(self, force: bool = False) -> Dict[str, Any]:
        today = datetime.now().strftime("%Y-%m-%d")
        state = self.get_data("sign_state") or {}
        if state.get("date") != today:
            state = {"date": today, "sites": {}, "notified": [], "daily_notified": False}
        known: Dict[str, Any] = state.get("sites") or {}
        sites = self.__sites()
        if not sites:
            logger.warn("站点签到助手没有可用站点（检查站点管理或手工 Cookie 配置）")
            return {"date": today, "total": 0, "results": [], "skipped": 0}

        # 无论定时、巡检还是手动立即运行，今天已确认签到的站点都不再重复提交表单
        pending = [s for s in sites
                   if (known.get(s["domain"]) or {}).get("status")
                   not in ("success", "already", "unsupported")]
        skipped = len(sites) - len(pending)
        if not pending:
            logger.info(f"站点签到助手：今日 {len(sites)} 站均已签到，跳过（不重复提交表单）")
            return {"date": today, "total": len(sites), "skipped": skipped,
                    "results": list(known.values()), "last_run": state.get("last_run")}
        sites = pending

        results: List[Dict[str, Any]] = []
        for site in sites:
            result = self.__sign_site(site)
            results.append(result)
            known[result["domain"]] = result
            label = {"success": "签到成功", "already": "今日已签", "manual": "需手动处理",
                     "error": "失败", "unknown": "无法判定",
                     "unsupported": "无签到功能(跳过)"}.get(result["status"], result["status"])
            logger.info(f"站点签到助手 [{label}] {result['site']}：{result['detail']}"
                        f" {result.get('reward') or ''}")

        state["sites"] = known
        state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_data("sign_state", state)
        self.__notify(state, results, daily=force)
        self.save_data("sign_state", state)
        return {
            "date": today,
            "total": len(self.__sites()),
            "checked": len(results),
            "skipped": skipped,
            "results": results,
            "last_run": state["last_run"],
        }

    # ------------------------------------------------------------------
    # 配置表单与状态页
    # ------------------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用自动签到"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即签到一次"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "test_notify", "label": "发一条测试通知"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "notify_fail", "label": "失败时钉钉提醒"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "cron_hour", "label": "每日签到小时(0-23)"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "cron_minute", "label": "每日签到分钟(0-59)",
                                "hint": "建议早于原插件 8:00，这样原面板能直接显示签到成功",
                                "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "retry_minutes", "label": "补签巡检间隔(分钟)",
                                "hint": "只检查当天未成功的站点；已签站点不重复提交",
                                "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "timeout", "label": "单站请求超时(秒)"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "dingtalk_token", "label": "钉钉 access_token（或完整 Webhook）"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "dingtalk_secret", "label": "钉钉加签密钥 SEC"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                            {"component": "VSelect", "props": {
                                "chips": True,
                                "multiple": True,
                                "model": "sign_sites",
                                "label": "签到站点",
                                "items": self.__site_options(),
                                "hint": "自动列出站点管理里的全部站点，以后新增的站点也会出现在这里；"
                                        "选「全部站点」可一劳永逸，没有签到功能的站点会自动跳过",
                                "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {
                                "model": "notify_daily", "label": "每天发签到日报",
                                "hint": "默认关闭，避免刷屏；只在每日签到那一轮发一条汇总",
                                "persistent-hint": True}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextarea", "props": {
                                "model": "manual_cookies", "label": "手工 Cookie（可选，每行 域名|Cookie）",
                                "placeholder": "hdbao.cc|c_secure_pass=...",
                                "hint": "正常情况下自动从站点管理读取，无需填写；仅在站点管理读取失败时作为兜底",
                                "persistent-hint": True}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal",
                                "text": "站点直接从「站点管理」读取，新增站点会自动出现在上面的下拉里。"
                                        "本插件真正提交「立即签到」表单：无验证码的站点走 POST；"
                                        "带 Cloudflare Turnstile 的站点用浏览器过验证后点击。"
                                        "以站点返回的「签到成功 / 本次签到获得 / 签到已得N」为准判定，"
                                        "不会把按钮或说明文字误判成成功；无签到页的站点自动跳过不报警。"}}]},
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "test_notify": False,
            "timeout": 20,
            "cron_hour": 7,
            "cron_minute": 55,
            "retry_minutes": 60,
            "domains": _DEFAULT_DOMAINS,
            "sign_sites": [],
            "manual_cookies": "",
            "dingtalk_token": "",
            "dingtalk_secret": "",
            "notify_fail": True,
            "notify_daily": False,
        }

    def get_page(self) -> List[dict]:
        state = self.get_data("sign_state") or {}
        records: Dict[str, Any] = state.get("sites") or {}
        icons = {"success": "✅", "already": "✅", "manual": "🖐️", "error": "❌",
                 "unknown": "❓", "unsupported": "⏭️"}
        labels = {"success": "签到成功", "already": "今日已签", "manual": "需手动处理",
                  "error": "失败", "unknown": "无法判定", "unsupported": "无签到功能(跳过)"}
        rows = []
        targets = [s["domain"] for s in self.__sites()]
        for domain in targets:
            rec = records.get(domain) or {}
            status = rec.get("status") or "none"
            icon = icons.get(status, "⚪")
            label = labels.get(status, "尚未检查")
            detail = rec.get("reward") or rec.get("detail") or "暂无记录"
            rows.append({
                "component": "div",
                "props": {"class": "text-body-2 py-1"},
                "text": f"{icon} {label}  {rec.get('site') or domain} · {detail} · "
                        f"{rec.get('method') or '-'} · {rec.get('time') or '-'}",
            })
        ok = sum(1 for r in records.values() if r.get("status") in ("success", "already"))
        head = (f"状态：{'运行中' if self._enabled else '未启用'} · "
                f"每日 {self._cron_hour:02d}:{self._cron_minute:02d} 签到 · "
                f"每 {self._retry_minutes} 分钟补签巡检 · "
                f"{state.get('date') or '-'} 成功 {ok}/{len(targets)} · "
                f"最近运行 {state.get('last_run') or '-'}")
        return [{
            "component": "VCard",
            "content": [{
                "component": "VCardText",
                "props": {"class": "pa-4"},
                "content": [
                    {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold mb-2"},
                     "text": "站点签到助手 · 今日状态"},
                    {"component": "div", "props": {"class": "text-body-2 mb-3"}, "text": head},
                    *rows,
                    {"component": "div", "props": {"class": "text-caption mt-3"},
                     "text": "Cookie 取自 MoviePilot 站点管理；带人机验证的站点使用真实浏览器提交。"
                             "若某站显示「需手动处理」，点击钉钉提醒里的链接手动签一次即可，插件会在下一轮自动重试。"},
                ],
            }],
        }]
