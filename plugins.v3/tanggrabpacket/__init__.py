# -*- coding: utf-8 -*-
import hashlib
import json
import random
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests
import urllib3
from apscheduler.triggers.interval import IntervalTrigger

from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.db.site_oper import SiteOper

urllib3.disable_warnings()


class TangGrabPacket(_PluginBase):
    """
    不可躺(tangpt.top) 抢红包插件
    定时轮询 /api/redpacket/latest，发现新红包自动走 PoW 验证并领取。
    """

    plugin_name = "不可躺抢红包"
    plugin_desc = "自动监测不可躺站点群聊区红包，PoW验证后自动拆红包，支持通知到微信。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.2"
    plugin_author = "WorkBuddy"
    author_url = ""
    plugin_config_prefix = "tanggrabpacket_"
    plugin_order = 31
    auth_level = 1

    SITE_DOMAIN = "tangpt.top"
    API_BASE = "https://www.tangpt.top"
    REFERER = "https://www.tangpt.top/index.php"

    _enabled = False
    _interval = 90
    _max_per_day = 100
    _notify = True
    _cookie = ""
    _onlyonce = False

    _lock = threading.Lock()
    _session = None
    _device = None
    # token 缓存: {token, expires_at, max_uses, used}
    _antibot = None
    # 运行状态
    _claimed_ids = set()
    _today = ""
    _today_count = 0
    _daily_limited = False
    _cooldown_until = 0.0

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._interval = self.__safe_int(config.get("interval"), 90, min_value=30)
        self._max_per_day = self.__safe_int(config.get("max_per_day"), 100, min_value=1)
        self._notify = bool(config.get("notify", True))
        self._cookie = (config.get("cookie") or self.__get_site_cookie() or "").strip()
        self._onlyonce = bool(config.get("onlyonce", False))

        # 载入持久化数据（已领取记录 / 今日计数）
        data = self.get_data("grab_state") or {}
        self._today = data.get("today") or ""
        self._today_count = int(data.get("today_count") or 0)
        self._claimed_ids = set(data.get("claimed_ids") or [])
        self._daily_limited = bool(data.get("daily_limited", False))
        self._cooldown_until = float(data.get("cooldown_until") or 0)
        today = datetime.now().strftime("%Y-%m-%d")
        if self._today != today:
            self._today = today
            self._today_count = 0
            self._daily_limited = False
            self.__save_state()

        if self._onlyonce:
            self._onlyonce = False
            self.update_config({
                "enabled": self._enabled,
                "interval": self._interval,
                "max_per_day": self._max_per_day,
                "notify": self._notify,
                "cookie": config.get("cookie", ""),
                "onlyonce": False,
            })
            threading.Thread(target=self.grab_once, daemon=True).start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/TangGrabPacket/run",
                "endpoint": self.__run_api,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行一轮抢红包",
            }
        ]

    def __run_api(self, api_result=None):
        result = self.grab_once()
        return api_result or {"success": True, "data": result}

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [
            {
                "id": "TangGrabPacket",
                "name": "不可躺抢红包轮询",
                "trigger": IntervalTrigger(seconds=self._interval),
                "func": self.__grab_task,
                "kwargs": {}
            }
        ]

    def __grab_task(self):
        try:
            self.grab_once()
        except Exception as err:
            logger.error(f"不可躺抢红包任务异常：{err}")

    # ---------------- 核心 ----------------

    def __get_site_cookie(self) -> str:
        try:
            site = SiteOper().get_by_domain(self.SITE_DOMAIN)
            if site and site.cookie:
                return site.cookie
            logger.warning(
                f"不可躺抢红包：站点库中未找到域名 {self.SITE_DOMAIN} 的记录，请检查站点配置")
        except Exception as err:
            logger.error(f"不可躺抢红包：获取站点Cookie异常：{err}")
        return ""

    @staticmethod
    def __safe_int(value, default, min_value=None):
        try:
            result = int(value) if value is not None else default
        except (ValueError, TypeError):
            result = default
        if min_value is not None:
            result = max(result, min_value)
        return result

    def __session(self):
        if self._session is None:
            s = requests.Session()
            s.verify = False
            s.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": self.API_BASE,
                "Referer": self.REFERER,
            })
            cookies = {}
            for item in (self._cookie or "").split(";"):
                if "=" in item:
                    k, v = item.strip().split("=", 1)
                    cookies[k.strip()] = v.strip()
            s.cookies.update(cookies)
            self._session = s
            if not self._device:
                self._device = "mp-" + uuid.uuid4().hex[:12]
        return self._session

    @staticmethod
    def __zero_bits(hex_str: str) -> int:
        bits = 0
        for ch in hex_str:
            if ch == "0":
                bits += 4
            else:
                n = int(ch, 16)
                bits += 4 - n.bit_length()
                break
        return bits

    def __get_antibot_token(self) -> Tuple[str, str]:
        """
        获取人机验证token，返回 (token, err_msg)
        """
        s = self.__session()
        headers = {"X-RedPacket-Device": self._device}
        try:
            ch = s.post(f"{self.API_BASE}/api/redpacket/antibot/challenge",
                        data={"fallback": 1}, headers=headers, timeout=20).json()
        except Exception as err:
            return "", f"验证接口请求失败：{err}"
        mode = ch.get("mode")
        if mode == "disabled":
            return "", ""
        if mode != "pow":
            # turnstile 等模式无法脚本验证，稍后重试
            return "", f"站点要求{mode}验证，本轮跳过"
        salt = str(ch.get("salt") or "")
        cid = str(ch.get("challenge_id") or "")
        bits = self.__safe_int(ch.get("difficulty_bits"), 12, min_value=1)
        if not salt or not cid:
            return "", "验证参数无效"
        prefix = f"{salt}:{cid}:"
        nonce = 0
        started = time.time()
        while time.time() - started < 30:
            digest = hashlib.sha256(f"{prefix}{nonce}".encode()).hexdigest()
            if self.__zero_bits(digest) >= bits:
                break
            nonce += 1
        else:
            return "", "PoW求解超时"
        try:
            v = s.post(f"{self.API_BASE}/api/redpacket/antibot/verify",
                       data={"challenge_id": cid, "mode": "pow", "nonce": nonce,
                             "confirm": 1 if ch.get("require_confirm") else 0},
                       headers=headers, timeout=20).json()
        except Exception as err:
            return "", f"验证确认请求失败：{err}"
        if not v.get("token"):
            msg = str(v.get("message") or v.get("msg") or "人机验证失败")
            return "", msg
        self._antibot = {
            "token": v.get("token"),
            "expires_at": self.__safe_int(v.get("expires_at"), 0),
            "max_uses": self.__safe_int(v.get("max_uses"), 1),
            "used": 0,
        }
        return self._antibot["token"], ""

    def __use_token(self) -> Tuple[str, str]:
        now = time.time()
        st = self._antibot
        if (st and st.get("token")
                and (not st.get("expires_at") or st["expires_at"] > now + 5)
                and st.get("used", 0) < st.get("max_uses", 1)):
            return st["token"], ""
        self._antibot = None
        return self.__get_antibot_token()

    def __save_state(self):
        self.save_data("grab_state", {
            "today": self._today,
            "today_count": self._today_count,
            "claimed_ids": list(self._claimed_ids)[-1000:],
            "daily_limited": self._daily_limited,
            "cooldown_until": self._cooldown_until,
        })

    def __notify(self, title: str, text: str):
        if self._notify:
            try:
                self.post_message(mtype=NotificationType.Plugin, title=title, text=text)
            except Exception as err:
                logger.error(f"不可躺抢红包：发送通知失败：{err}")

    def grab_once(self) -> Dict[str, Any]:
        """
        执行一轮：查询最新红包列表并领取所有可领取的
        """
        with self._lock:
            if not self._cookie:
                self._cookie = self.__get_site_cookie()
            if not self._cookie:
                return {"success": False, "message": "未获取到不可躺站点Cookie"}
            s = self.__session()
            today = datetime.now().strftime("%Y-%m-%d")
            if self._today != today:
                self._today = today
                self._today_count = 0
                self._daily_limited = False
            if self._daily_limited:
                return {"success": True, "message": f"今日已领满{self._today_count}个，明天继续"}
            if time.time() < self._cooldown_until:
                return {"success": True,
                        "message": f"风控冷却中，剩余{int(self._cooldown_until - time.time())}秒"}

            try:
                lat = s.get(f"{self.API_BASE}/api/redpacket/latest", timeout=20).json()
            except Exception as err:
                return {"success": False, "message": f"红包列表请求失败：{err}"}
            items = lat.get("items") or []
            todo = [it for it in items
                    if int(it.get("remain_count") or 0) > 0
                    and str(it.get("id")) not in self._claimed_ids]
            if not todo:
                return {"success": True, "message": "暂无新的可领红包"}

            grabbed = []
            skipped = []
            for it in todo:
                if self._today_count >= self._max_per_day:
                    break
                pid = it.get("id")
                ok, msg, magic = self.__claim(pid)
                if ok:
                    self._claimed_ids.add(str(pid))
                    self._today_count += 1
                    self.__save_state()
                    grabbed.append({"id": pid, "sender": it.get("sender"),
                                    "magic": magic, "message": it.get("message")})
                    self.__notify(
                        "【不可躺抢红包】拆到一个！",
                        f"红包 #{pid}\n发件人：{it.get('sender')}\n"
                        f"金额：{magic}\n祝福：{it.get('message') or '-'}\n"
                        f"今日已领：{self._today_count}/{self._max_per_day}"
                    )
                    # 人性化间隔（用户指定10-20秒），避免触发站点风控
                    time.sleep(random.uniform(10, 20))
                else:
                    # 站点风控升级（turnstile/人机验证/需勾选本人操作）：退避冷却，
                    # 该红包不标记已领，冷却后自动重试
                    if any(k in msg for k in ("turnstile", "人机", "验证", "勾选", "本人操作")):
                        self._cooldown_until = time.time() + random.uniform(480, 900)
                        self.__save_state()
                        logger.info(f"不可躺抢红包：触发站点风控（{msg}），冷却{int(self._cooldown_until - time.time())}秒后重试")
                        skipped.append({"id": pid, "reason": msg})
                        break
                    self._claimed_ids.add(str(pid))
                    self.__save_state()
                    skipped.append({"id": pid, "reason": msg})
                    if "最多领" in msg or "领取100个" in msg:
                        self._daily_limited = True
                        self.__save_state()
                        break
                    if "登录" in msg or "失效" in msg or "cookie" in msg.lower():
                        return {"success": False, "message": f"Cookie疑似失效：{msg}"}
                    time.sleep(random.uniform(1, 2))

            summary = f"本轮领取 {len(grabbed)} 个，跳过 {len(skipped)} 个，今日累计 {self._today_count} 个"
            if grabbed or skipped:
                logger.info(f"不可躺抢红包：{summary}")
            return {"success": True, "message": summary,
                    "grabbed": grabbed, "skipped": skipped}

    def __claim(self, pid) -> Tuple[bool, str, str]:
        token, err = self.__use_token()
        if err and not token:
            return False, err, "-"
        s = self.__session()
        headers = {"X-RedPacket-Device": self._device}
        if token:
            headers["X-RedPacket-Token"] = token
        try:
            r = s.post(f"{self.API_BASE}/api/redpacket/claim",
                       data={"packet_id": pid}, headers=headers, timeout=20)
        except Exception as err:
            return False, f"请求异常：{err}", "-"
        # 428 = token失效，重验一次
        if r.status_code == 428:
            self._antibot = None
            token, err = self.__get_antibot_token()
            if not token and err:
                return False, err, "-"
            headers["X-RedPacket-Token"] = token or ""
            try:
                r = s.post(f"{self.API_BASE}/api/redpacket/claim",
                           data={"packet_id": pid}, headers=headers, timeout=20)
            except Exception as err:
                return False, f"重试请求异常：{err}", "-"
        try:
            res = r.json()
        except ValueError:
            return False, f"非JSON响应 HTTP {r.status_code}", "-"
        if r.status_code == 200 and res.get("ok") is not False:
            magic = res.get("magic") or res.get("amount") or res.get("bonus")
            if magic is None and isinstance(res.get("data"), dict):
                d = res["data"]
                magic = d.get("magic") or d.get("amount") or d.get("bonus")
            return True, "", str(magic if magic is not None else "-")
        msg = str(res.get("message") or res.get("msg") or f"HTTP {r.status_code}")
        if st := self._antibot:
            st["used"] = st.get("used", 0) + 1
        return False, msg, "-"

    # ---------------- 配置页 ----------------

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "微信通知"}
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即运行一次"}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval",
                                            "label": "轮询间隔（秒）",
                                            "hint": "建议不低于60秒，过快可能触发站点风控",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_per_day",
                                            "label": "每日最多领取数",
                                            "hint": "站点限制每天100个",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cookie",
                                            "label": "Cookie（留空自动读取站点配置）",
                                            "clearable": True
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": False,
            "interval": self._interval,
            "max_per_day": self._max_per_day,
            "cookie": "",
        }

    def get_page(self) -> List[dict]:
        results = self.get_data("grab_state") or {}
        history_note = f"今日已领取 {results.get('today_count') or 0} 个"
        return [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-4"},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "text-subtitle-1 mb-2"},
                                "text": f"状态：{'运行中' if self._enabled else '未启用'} · {history_note}"
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-body-2 text-muted"},
                                "text": f"已领取记录 {len(self._claimed_ids)} 条（自动去重，防止重复领取）"
                            }
                        ]
                    }
                ]
            }
        ]

    def stop_service(self):
        pass
