"""群机器人消息通知插件（钉钉 / 飞书）。

v1.1.0：新增「哪些消息发 / 哪些不发」的关键词规则，以及高频消息的
每日汇总（刷流、抢红包等一天只推一条），从通知出口统一降噪。
"""

import base64
import hashlib
import hmac
import re
import threading
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.network import RequestUtils

DINGTALK_SEND_URL = "https://oapi.dingtalk.com/robot/send"
FEISHU_SEND_PREFIX = "https://open.feishu.cn/open-apis/bot/v2/hook/"

DEFAULT_DIGEST_KEYWORDS = "刷流,红包"
DEFAULT_INSTANT_KEYWORDS = "开注,失败,异常,告警,封禁,上限,错误,超时"
DEFAULT_DENY_KEYWORDS = ""
DEFAULT_ALLOW_KEYWORDS = ""

_DIGEST_KEY = "grouprobotmsg_digest_buffer"
_MAX_DETAILS = 200


class GroupRobotMsg(_PluginBase):
    """群机器人消息通知插件。

    监听 ``EventType.NoticeMessage``，按配置决定「立即推送 / 不推送 /
    计入每日汇总」，再转发到钉钉或飞书群机器人。群内消息完全展开。
    """

    plugin_name = "群机器人通知"
    plugin_desc = "支持将消息通知发送到钉钉/飞书群机器人，可设置哪些消息发/不发，刷流、抢红包等高频消息每日汇总只推一条。"
    plugin_icon = "Tang_A.png"
    plugin_version = "1.1.0"
    plugin_author = "WorkBuddy"
    author_url = "https://github.com/147392552"
    plugin_config_prefix = "grouprobotmsg_"
    plugin_order = 32
    auth_level = 1

    _enabled = False
    _dingtalk_token: Optional[str] = None
    _dingtalk_secret: Optional[str] = None
    _dingtalk_keyword: Optional[str] = None
    _feishu_webhook: Optional[str] = None
    _feishu_secret: Optional[str] = None
    _msgtypes: List[str] = []

    # v1.1.0 消息治理配置
    _digest_enabled = True
    _digest_keywords: List[str] = []
    _instant_keywords: List[str] = []
    _deny_keywords: List[str] = []
    _allow_keywords: List[str] = []
    _digest_hour = 23
    _digest_minute = 30
    _digest_max_items = 8
    _digest_empty_send = False
    _lock = threading.RLock()

    # ---------------- 生命周期 ----------------

    @staticmethod
    def _split_keywords(value: Any) -> List[str]:
        """把逗号/分号/换行分隔的关键词字符串转成列表。"""
        if isinstance(value, (list, tuple, set)):
            items = [str(x) for x in value]
        else:
            items = re.split(r"[,，;；\n]+", str(value or ""))
        return [x.strip() for x in items if x.strip()]

    @staticmethod
    def _to_int(value: Any, default: int, low: int, high: int) -> int:
        try:
            n = int(value)
        except (TypeError, ValueError):
            return default
        return max(low, min(high, n))

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        # 兼容粘贴完整 Webhook 地址：自动提取 access_token
        dt_raw = (config.get("dingtalk_token") or "").strip()
        if "access_token=" in dt_raw:
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(dt_raw).query)
                dt_raw = (qs.get("access_token") or [dt_raw])[0]
            except Exception:
                pass
        self._dingtalk_token = dt_raw or None
        self._dingtalk_secret = (config.get("dingtalk_secret") or "").strip() or None
        self._dingtalk_keyword = (config.get("dingtalk_keyword") or "").strip() or None
        fs_raw = (config.get("feishu_webhook") or "").strip()
        self._feishu_webhook = fs_raw or None
        self._feishu_secret = (config.get("feishu_secret") or "").strip() or None
        self._msgtypes = config.get("msgtypes") or []

        self._digest_enabled = bool(config.get("digest_enabled", True))
        self._digest_keywords = self._split_keywords(
            config.get("digest_keywords", DEFAULT_DIGEST_KEYWORDS))
        self._instant_keywords = self._split_keywords(
            config.get("instant_keywords", DEFAULT_INSTANT_KEYWORDS))
        self._deny_keywords = self._split_keywords(
            config.get("deny_keywords", DEFAULT_DENY_KEYWORDS))
        self._allow_keywords = self._split_keywords(
            config.get("allow_keywords", DEFAULT_ALLOW_KEYWORDS))
        self._digest_hour = self._to_int(config.get("digest_hour", 23), 23, 0, 23)
        self._digest_minute = self._to_int(config.get("digest_minute", 30), 30, 0, 59)
        self._digest_max_items = self._to_int(config.get("digest_max_items", 8), 8, 0, 50)
        self._digest_empty_send = bool(config.get("digest_empty_send", False))

    def get_state(self) -> bool:
        """启用且至少配置了一个渠道。"""
        return self._enabled and bool(self._dingtalk_token or self._feishu_webhook)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """当前插件不注册远程命令。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """注册测试发送 / 手动汇总 / 汇总预览 API。"""
        return [
            {
                "path": "/test",
                "endpoint": self.api_test,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "发送测试消息到群机器人",
            },
            {
                "path": "/digest",
                "endpoint": self.api_digest,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即发送当天的通知汇总",
            },
            {
                "path": "/preview",
                "endpoint": self.api_preview,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "查看当前待汇总消息统计（不发送）",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """每天固定时间把当天的高频消息合并成一条推送。"""
        if not self.get_state() or not self._digest_enabled:
            return []
        return [
            {
                "id": "GroupRobotMsgDigest",
                "name": "群机器人每日通知汇总",
                "trigger": CronTrigger(hour=self._digest_hour, minute=self._digest_minute),
                "func": self.__digest_task,
                "kwargs": {},
            }
        ]

    def __digest_task(self) -> None:
        try:
            result = self._flush_digest()
            logger.info(f"群机器人每日汇总任务：{result}")
        except Exception as err:
            logger.error(f"群机器人每日汇总任务异常：{err}")

    # ---------------- 配置表单 ----------------

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装插件配置页面。"""
        msg_type_options = [
            {"title": item.value, "value": item.name}
            for item in NotificationType
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "钉钉：群设置-智能群助手-添加机器人-自定义机器人，"
                                                "安全设置建议选『加签』，把 Webhook 里的 access_token 和 SEC 密钥填到下面。"
                                                "飞书：群设置-群机器人-添加机器人-自定义机器人，把 Webhook 地址填到下面。"
                                                "两个渠道都填则同时发送。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "dingtalk_token",
                                            "label": "钉钉 access_token（或粘贴完整Webhook）",
                                            "placeholder": "粘贴 Webhook 地址或 access_token",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "dingtalk_secret",
                                            "label": "钉钉加签密钥 SEC（可选）",
                                            "placeholder": "SECxxxxxxxx",
                                            "type": "password",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "dingtalk_keyword",
                                            "label": "钉钉自定义关键词（可选）",
                                            "placeholder": "若安全设置选了关键词，填写该词",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "feishu_webhook",
                                            "label": "飞书 Webhook 地址",
                                            "placeholder": "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "feishu_secret",
                                            "label": "飞书签名校验密钥（可选）",
                                            "type": "password",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "model": "msgtypes",
                                            "label": "消息类型（不选=全部）",
                                            "items": msg_type_options,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": (
                                                "消息治理优先级：① 不发送关键词（命中直接丢弃）→ "
                                                "② 消息类型 / 只发送关键词 → ③ 紧急即时关键词（重要消息立刻推）→ "
                                                "④ 每日汇总关键词（合并成一条，不再逐条打扰）→ ⑤ 其余照常即时推送。"
                                                "关键词支持多个，用英文逗号分隔，匹配标题和正文。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "allow_keywords",
                                            "label": "只发送这些关键词（留空=不限制）",
                                            "placeholder": "例如：开注,下载完成,签到",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "deny_keywords",
                                            "label": "不发送这些关键词（优先级最高）",
                                            "placeholder": "例如：心跳,检查磁盘文件",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "instant_keywords",
                                            "label": "紧急即时关键词（跳过汇总立刻推）",
                                            "placeholder": "例如：开注,失败,异常,封禁,上限",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "digest_keywords",
                                            "label": "每日汇总关键词（命中只进汇总）",
                                            "placeholder": "例如：刷流,红包",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "digest_enabled",
                                            "label": "启用每日汇总",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "digest_hour",
                                            "label": "汇总小时（0-23）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "digest_minute",
                                            "label": "汇总分钟（0-59）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "digest_max_items",
                                            "label": "汇总明细条数",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "digest_empty_send",
                                            "label": "无消息也发",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "dingtalk_token": "",
            "dingtalk_secret": "",
            "dingtalk_keyword": "",
            "feishu_webhook": "",
            "feishu_secret": "",
            "msgtypes": [],
            "allow_keywords": DEFAULT_ALLOW_KEYWORDS,
            "deny_keywords": DEFAULT_DENY_KEYWORDS,
            "instant_keywords": DEFAULT_INSTANT_KEYWORDS,
            "digest_keywords": DEFAULT_DIGEST_KEYWORDS,
            "digest_enabled": True,
            "digest_hour": 23,
            "digest_minute": 30,
            "digest_max_items": 8,
            "digest_empty_send": False,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页。"""
        return [
            {
                "component": "VCard",
                "props": {"class": "mb-4", "variant": "tonal"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {
                                "component": "VIcon",
                                "props": {"icon": "mdi-bell-outline", "class": "mr-2"},
                            },
                            "插件介绍",
                        ],
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": (
                                        "把 MoviePilot 通知转发到钉钉/飞书群机器人，消息完全展开。"
                                        "可设置「只发送什么 / 不发送什么」；刷流、抢红包等高频消息"
                                        "默认只累计条数，每天在设定时间合并成一条汇总推送，"
                                        "开注、失败/异常等重要消息仍即时推送。"
                                    ),
                                },
                            }
                        ],
                    },
                ],
            }
        ]

    # ---------------- 消息分类与汇总 ----------------

    def _classify(self, msg_type: Any, title: str, text: str):
        """返回 'deny' / 'instant' / ('digest', 分组关键词)。"""
        blob = f"{title or ''}\n{text or ''}"
        if self._deny_keywords and any(k in blob for k in self._deny_keywords):
            return "deny"
        type_name = getattr(msg_type, "name", str(msg_type) if msg_type else "")
        if self._msgtypes and type_name not in self._msgtypes:
            return "deny"
        if self._allow_keywords and not any(k in blob for k in self._allow_keywords):
            return "deny"
        if self._instant_keywords and any(k in blob for k in self._instant_keywords):
            return "instant"
        if self._digest_enabled and self._digest_keywords:
            for keyword in self._digest_keywords:
                if keyword in blob:
                    return ("digest", keyword)
        return "instant"

    @staticmethod
    def _today() -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _load_digest(self) -> Dict[str, Any]:
        try:
            data = self.get_data(_DIGEST_KEY) or {}
        except Exception as err:
            logger.warn(f"群机器人汇总缓存读取失败：{err}")
            data = {}
        if not isinstance(data, dict):
            data = {}
        data.setdefault("day", self._today())
        data.setdefault("total", 0)
        data.setdefault("groups", {})
        data.setdefault("details", [])
        data.setdefault("magic", 0.0)
        return data

    @staticmethod
    def _empty_digest(day: str) -> Dict[str, Any]:
        return {"day": day, "total": 0, "groups": {}, "details": [], "magic": 0.0}

    def _append_digest(self, group: str, title: str, text: str) -> None:
        """把一条高频消息计入汇总缓冲；跨天时先补发旧的一天。"""
        with self._lock:
            data = self._load_digest()
            today = self._today()
            if data.get("day") != today:
                if int(data.get("total") or 0) > 0:
                    self._flush_locked(data, force=True)
                data = self._empty_digest(today)
            data["total"] = int(data.get("total") or 0) + 1
            groups = data.setdefault("groups", {})
            bucket = groups.setdefault(group, {"count": 0, "titles": {}})
            bucket["count"] = int(bucket.get("count") or 0) + 1
            short_title = (title or "（无标题）").strip()[:60]
            titles = bucket.setdefault("titles", {})
            titles[short_title] = int(titles.get(short_title) or 0) + 1
            # 抢红包的魔力值：正文形如「金额：10 魔力」
            magic_match = re.search(r"金额[：:]\s*([0-9]+(?:\.[0-9]+)?)", text or "")
            if magic_match:
                try:
                    data["magic"] = float(data.get("magic") or 0) + float(magic_match.group(1))
                except ValueError:
                    pass
            details = data.setdefault("details", [])
            if len(details) < _MAX_DETAILS:
                brief = re.sub(r"\s+", " ", (text or "").strip())[:80]
                details.append({
                    "time": datetime.now().strftime("%H:%M"),
                    "title": short_title,
                    "brief": brief,
                })
            try:
                self.save_data(_DIGEST_KEY, data)
            except Exception as err:
                logger.error(f"群机器人汇总缓存写入失败：{err}")

    def _build_digest_text(self, data: Dict[str, Any]) -> str:
        day = str(data.get("day") or self._today())
        total = int(data.get("total") or 0)
        lines = [f"📅 **{day}** ｜ 共合并 **{total}** 条高频通知（逐条推送已关闭）", ""]
        magic = float(data.get("magic") or 0)
        for group, bucket in (data.get("groups") or {}).items():
            count = int(bucket.get("count") or 0)
            suffix = ""
            if magic and any(k in str(group) for k in ("红包", "魔力")):
                suffix = f" ｜ 合计魔力 **{magic:g}**"
            lines.append(f"**{group}**：{count} 条{suffix}")
            titles = bucket.get("titles") or {}
            for item_title, n in sorted(titles.items(), key=lambda kv: -kv[1])[:5]:
                lines.append(f"  · {item_title} × {n}")
        details = data.get("details") or []
        if self._digest_max_items > 0 and details:
            lines.append("")
            show = details[-self._digest_max_items:]
            lines.append(f"最近 {len(show)} 条明细：")
            for item in show:
                lines.append(f"  {item.get('time')} {item.get('title')} {item.get('brief') or ''}")
        lines.append("")
        lines.append("开注、失败/异常等重要消息仍即时推送；汇总时间与关键词可在插件设置里调整。")
        return "\n".join(lines)

    def _flush_locked(self, data: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
        """发送一条汇总并清空缓冲（调用方需持有锁）。"""
        total = int(data.get("total") or 0)
        day = str(data.get("day") or self._today())
        if total == 0 and not (force or self._digest_empty_send):
            return {"sent": False, "reason": "empty", "count": 0, "day": day}
        title = f"📊 MoviePilot 每日通知汇总（{day}）"
        if total == 0:
            text = f"📅 {day} ｜ 今天没有需要汇总的高频通知。"
        else:
            text = self._build_digest_text(data)
        ok = self._push(title, text)
        if ok:
            try:
                self.save_data(_DIGEST_KEY, self._empty_digest(self._today()))
            except Exception as err:
                logger.error(f"群机器人汇总缓存清空失败：{err}")
            logger.info(f"群机器人每日汇总已发送：{day} / {total} 条")
        else:
            logger.warn(f"群机器人每日汇总发送失败，保留 {total} 条待下次重试")
        return {"sent": bool(ok), "count": total, "day": day}

    def _flush_digest(self, force: bool = False) -> Dict[str, Any]:
        with self._lock:
            return self._flush_locked(self._load_digest(), force)

    # ---------------- 渠道发送 ----------------

    @staticmethod
    def _dingtalk_sign(secret: str) -> Tuple[int, str]:
        """钉钉加签：timestamp + "\n" + secret 的 HMAC-SHA256，Base64。"""
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(
            secret.encode("utf-8"), string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    @staticmethod
    def _feishu_sign(secret: str) -> Tuple[str, str]:
        """飞书签名：以 "{timestamp}\\n{secret}" 为密钥对空串做 HMAC-SHA256。"""
        timestamp = str(int(time.time()))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
        ).digest()
        sign = base64.b64encode(hmac_code).decode("utf-8")
        return timestamp, sign

    def _send_dingtalk(self, title: str, text: str) -> bool:
        """发送钉钉群机器人消息（markdown，群内完整展开）。"""
        if not self._dingtalk_token:
            return False
        url = f"{DINGTALK_SEND_URL}?access_token={self._dingtalk_token}"
        if self._dingtalk_secret:
            ts, sign = self._dingtalk_sign(self._dingtalk_secret)
            url += f"&timestamp={ts}&sign={sign}"
        md_text = (text or "").replace("\n", "\n\n")
        if self._dingtalk_keyword:
            title = f"{self._dingtalk_keyword} {title}"
        payload: Dict[str, Any] = {
            "msgtype": "markdown",
            "markdown": {"title": title or "MoviePilot", "text": f"**{title or 'MoviePilot'}**\n\n{md_text}"},
        }
        res = RequestUtils(content_type="application/json").post_res(
            url, json=payload, allow_redirects=False
        )
        if res is None:
            logger.warn("钉钉消息发送失败：未获取到返回信息")
            return False
        try:
            ret = res.json()
        except Exception as err:
            logger.warn(f"钉钉消息发送失败，响应非 JSON：{err}")
            return False
        if ret.get("errcode") == 0:
            logger.info("钉钉群机器人消息发送成功")
            return True
        logger.warn(f"钉钉消息发送失败：{ret}")
        return False

    def _send_feishu(self, title: str, text: str) -> bool:
        """发送飞书群机器人消息（text，群内完整展开）。"""
        if not self._feishu_webhook:
            return False
        url = self._feishu_webhook
        payload: Dict[str, Any] = {
            "msg_type": "text",
            "content": {"text": f"{title or 'MoviePilot'}\n{text or ''}".strip()},
        }
        if self._feishu_secret:
            ts, sign = self._feishu_sign(self._feishu_secret)
            payload["timestamp"] = ts
            payload["sign"] = sign
        res = RequestUtils(content_type="application/json").post_res(
            url, json=payload, allow_redirects=False
        )
        if res is None:
            logger.warn("飞书消息发送失败：未获取到返回信息")
            return False
        try:
            ret = res.json()
        except Exception as err:
            logger.warn(f"飞书消息发送失败，响应非 JSON：{err}")
            return False
        if ret.get("code") == 0:
            logger.info("飞书群机器人消息发送成功")
            return True
        logger.warn(f"飞书消息发送失败：{ret}")
        return False

    def _push(self, title: str, text: str) -> bool:
        """把一条消息推送到所有已配置的渠道。"""
        ok = False
        try:
            if self._dingtalk_token:
                ok |= self._send_dingtalk(title or "", text or "")
            if self._feishu_webhook:
                ok |= self._send_feishu(title or "", text or "")
        except Exception as msg_e:
            logger.error(f"群机器人消息发送异常：{str(msg_e)}")
        if not ok:
            logger.warn("群机器人消息未发送成功（检查渠道配置或网络）")
        return bool(ok)

    # ---------------- 手动 API ----------------

    def api_test(self, title: str = None, text: str = None) -> Dict[str, Any]:
        """手动发送测试消息，验证钉钉/飞书配置。"""
        t = title or "MoviePilot 测试消息"
        x = text or (
            "**MoviePilot 群机器人通知测试**\n\n"
            "如果你在群里看到这条消息，说明通知链路已通。\n\n"
            f"发送时间：{time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        if not (self._dingtalk_token or self._feishu_webhook):
            return {"success": False, "message": "未配置任何群机器人"}
        return {"success": self._push(t, x), "message": "已发送测试消息"}

    def api_digest(self) -> Dict[str, Any]:
        """立即发送当天汇总（不改动自动汇总计划）。"""
        result = self._flush_digest(force=True)
        return {"success": bool(result.get("sent")), "data": result}

    def api_preview(self) -> Dict[str, Any]:
        """查看当前待汇总统计，不发送消息。"""
        with self._lock:
            data = self._load_digest()
        return {
            "success": True,
            "data": {
                "day": data.get("day"),
                "total": data.get("total"),
                "groups": {k: v.get("count") for k, v in (data.get("groups") or {}).items()},
                "magic": data.get("magic"),
                "details_kept": len(data.get("details") or []),
            },
        }

    # ---------------- 事件入口 ----------------

    @eventmanager.register(EventType.NoticeMessage)
    def send(self, event: Event) -> None:
        """消息发送事件：按规则转发 / 丢弃 / 计入每日汇总。"""
        if not self.get_state():
            return
        if not event.event_data:
            return

        msg_body = event.event_data
        # 已指定专用通知渠道时不处理
        if msg_body.get("channel"):
            return

        msg_type: NotificationType = msg_body.get("type")
        title = msg_body.get("title")
        text = msg_body.get("text")
        if not title and not text:
            return

        decision = self._classify(msg_type, title or "", text or "")
        if decision == "deny":
            logger.info("消息命中不发送规则，已跳过群机器人推送")
            return
        if isinstance(decision, tuple):
            group = decision[1]
            self._append_digest(group, title or "", text or "")
            logger.info(f"消息计入每日汇总分组 [{group}]：{title or '（无标题）'}")
            return
        self._push(title or "", text or "")

    def stop_service(self) -> None:
        """退出插件。"""
        return None
