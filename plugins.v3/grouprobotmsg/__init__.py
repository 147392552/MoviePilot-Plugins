"""群机器人消息通知插件（钉钉 / 飞书）。

v1.1.0：新增「哪些消息发 / 哪些不发」的关键词规则，以及高频消息的
每日汇总（刷流、抢红包等一天只推一条），从通知出口统一降噪。

v1.2.0：
  1. 重要消息（失败/异常/开注等）硬直通，不受「只发送」白名单和消息类型限制；
  2. 「运行结束 / 达到上限」事件立即发一条当日汇总（含当天累计、结束原因）；
     缓冲为空也发，用来暴露「插件没运行」；每天最多一条结束汇总；
  3. 每日定时汇总退化为增量兜底，不重复推送；
  4. 设置面板与详情页显示实时生效的规则速览，新增 /rules 只读接口。
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
# 重要消息：命中即立刻推送，且不受「只发送」白名单 / 消息类型限制
DEFAULT_INSTANT_KEYWORDS = "开注,失败,异常,告警,封禁,错误,超时,风控,冷却,失效"
# 任务结束 / 达到上限：立刻把当天高频消息合并成一条汇总推出
DEFAULT_FLUSH_KEYWORDS = (
    "已达上限,领取上限,达到上限,今日已领满,已停止领取,"
    "运行结束,任务结束,执行完毕,运行完毕,本轮结束"
)
DEFAULT_DENY_KEYWORDS = ""
DEFAULT_ALLOW_KEYWORDS = ""

_DIGEST_KEY = "grouprobotmsg_digest_buffer"
_FLUSH_KEY = "grouprobotmsg_flush_state"
_MAX_DETAILS = 200


class GroupRobotMsg(_PluginBase):
    """群机器人消息通知插件。

    监听 ``EventType.NoticeMessage``，按配置决定「立即推送 / 不推送 /
    计入汇总 / 触发结束汇总」，再转发到钉钉或飞书群机器人。群内消息完全展开。
    """

    plugin_name = "群机器人通知"
    plugin_desc = "支持将消息通知发送到钉钉/飞书群机器人，可设置哪些消息发/不发；刷流、抢红包等高频消息合并汇总，失败/异常等重要消息必发，任务结束或达到上限时立即发当日汇总。"
    plugin_icon = "Tang_A.png"
    plugin_version = "1.2.0"
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

    # 消息治理配置
    _digest_enabled = True
    _digest_keywords: List[str] = []
    _instant_keywords: List[str] = []
    _deny_keywords: List[str] = []
    _allow_keywords: List[str] = []
    _flush_keywords: List[str] = []
    _flush_enabled = True
    _flush_empty_send = True
    _flush_once_per_day = True
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
        instant_raw = self._split_keywords(
            config.get("instant_keywords", DEFAULT_INSTANT_KEYWORDS))
        self._deny_keywords = self._split_keywords(
            config.get("deny_keywords", DEFAULT_DENY_KEYWORDS))
        self._allow_keywords = self._split_keywords(
            config.get("allow_keywords", DEFAULT_ALLOW_KEYWORDS))
        self._flush_keywords = self._split_keywords(
            config.get("flush_keywords", DEFAULT_FLUSH_KEYWORDS))
        self._flush_enabled = bool(config.get("flush_enabled", True))
        # 兼容 v1.1.0 旧配置：旧版把「上限」放在即时关键词里，会抢在结束汇总之前
        # 把状态消息单独推掉。开启结束即汇总时，自动把这类结束信号交给 flush 规则。
        _flush_signals = ("上限", "已达上限", "领取上限", "达到上限", "今日已领满")
        self._instant_keywords = (
            [k for k in instant_raw if k not in _flush_signals]
            if self._flush_enabled else instant_raw
        )
        if self._instant_keywords != instant_raw:
            logger.info("已自动把「上限」类关键词从重要消息移到结束即汇总规则，避免汇总被抢先吞掉")
        self._flush_empty_send = bool(config.get("flush_empty_send", True))
        self._flush_once_per_day = bool(config.get("flush_once_per_day", True))
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
        """注册测试发送 / 手动汇总 / 汇总预览 / 规则速览 API。"""
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
            {
                "path": "/rules",
                "endpoint": self.api_rules,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "查看当前生效的消息分流规则（不发送）",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """每天固定时间把当天尚未汇总的高频消息合并成一条推送（增量兜底）。"""
        if not self.get_state() or not self._digest_enabled:
            return []
        return [
            {
                "id": "GroupRobotMsgDigest",
                "name": "群机器人每日通知汇总（增量兜底）",
                "trigger": CronTrigger(hour=self._digest_hour, minute=self._digest_minute),
                "func": self.__digest_task,
                "kwargs": {},
            }
        ]

    def __digest_task(self) -> None:
        try:
            state = self._load_flush_state()
            # 当天已经因「结束/上限」发过汇总时，这里只补发之后新增的部分
            label = ("通知补充汇总" if state.get("day") == self._today()
                     else "MoviePilot 每日通知汇总")
            result = self._flush_digest(label=label)
            logger.info(f"群机器人每日汇总任务：{label} / {result}")
        except Exception as err:
            logger.error(f"群机器人每日汇总任务异常：{err}")

    # ---------------- 配置表单 ----------------

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装插件配置页面：按「渠道 / 发送规则 / 汇总 / 结束即汇总」分节。"""
        msg_type_options = [
            {"title": item.value, "value": item.name}
            for item in NotificationType
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    # ---------- 默认行为速查 ----------
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
                                            "type": "success",
                                            "variant": "tonal",
                                            "text": (
                                                "现在哪些消息会发（默认规则）："
                                                "① 失败 / 异常 / 告警 / 封禁 / 开注 → 立刻推，必发，不受下面的白名单限制；"
                                                "② 其它普通消息（订阅、签到、下载完成等）→ 照常立刻推；"
                                                "③ 刷流、抢红包的逐条明细 → 不逐条推，累计条数与魔力；"
                                                "④ 插件运行结束 / 达到领取上限 → 立刻发一条当日汇总，让你当场知道跑没跑、跑了多少；"
                                                "⑤ 每天设定时间再做一次增量兜底汇总，没有新消息就不发。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # ---------- 渠道 ----------
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
                                                "【第一步：填机器人】钉钉：群设置-智能群助手-添加机器人-自定义机器人，"
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
                    # ---------- 发送规则 ----------
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
                                                "【第二步：哪些发、哪些不发】判定顺序："
                                                "①「不发送」命中即丢弃 → ②「重要消息必发」命中即立刻推（不会被白名单挡住）→ "
                                                "③ 消息类型 / 「只发送」白名单 → ④「结束即汇总」→ ⑤「每日汇总」→ ⑥ 其余照常立刻推。"
                                                "多个关键词用英文逗号分隔，同时匹配标题和正文；留空＝不启用该规则。"
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "model": "msgtypes",
                                            "label": "消息类型（不选=全部；重要消息不受此限制）",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "allow_keywords",
                                            "label": "只发送这些关键词（留空=不限制）",
                                            "placeholder": "例如：开注,下载完成,签到",
                                            "hint": "设了之后，不含这些词的普通消息会被丢弃；失败/异常等重要消息仍然必发。",
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
                                            "label": "重要消息必发关键词（立刻推，跳过汇总）",
                                            "placeholder": "例如：开注,失败,异常,封禁,错误",
                                            "hint": "「刷流任务失败」「红包领取失败」这类也会立刻推，不会被当成高频明细合并掉。",
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
                                            "label": "高频消息汇总关键词（不逐条推）",
                                            "placeholder": "例如：刷流,红包",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # ---------- 结束即汇总 ----------
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
                                                "【第三步：跑完就汇报】抢红包达到站点上限、任务运行结束时，"
                                                "不用等晚上，立刻发一条当日汇总（累计条数、红包魔力合计、结束原因）。"
                                                "如果一条累计都没有，也会发一条提醒检查插件是否没运行 / Cookie 是否失效。"
                                                "每天最多一条结束汇总，之后新增的消息由晚上的定时汇总补发，不会重复打扰。"
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "flush_enabled",
                                            "label": "运行结束/达到上限时立即发汇总",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "flush_empty_send",
                                            "label": "没有累计消息也发（便于发现没跑）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "flush_once_per_day",
                                            "label": "每天最多一条结束汇总",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "flush_keywords",
                                            "label": "结束事件关键词（命中即触发汇总）",
                                            "placeholder": "例如：已达上限,领取上限,运行结束,任务结束",
                                            "hint": "用站点真实文案片段，别用太泛的词，避免把普通消息误判成结束。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    # ---------- 定时兜底 ----------
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
                                                "【第四步：晚上兜底】下面的时间只补发「上次汇总之后新增」的高频消息；"
                                                "若结束汇总已经发过且之后没有新消息，晚上就不会再发第二条。"
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "digest_enabled",
                                            "label": "启用每日兜底汇总",
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
            "flush_keywords": DEFAULT_FLUSH_KEYWORDS,
            "flush_enabled": True,
            "flush_empty_send": True,
            "flush_once_per_day": True,
            "digest_enabled": True,
            "digest_hour": 23,
            "digest_minute": 30,
            "digest_max_items": 8,
            "digest_empty_send": False,
        }

    def get_page(self) -> Optional[List[dict]]:
        """详情页：显示实时生效的推送规则与今日累计，不再只是一段静态说明。"""
        try:
            with self._lock:
                digest = self._load_digest()
                state = self._load_flush_state()
        except Exception:
            digest, state = {}, {}
        instant = self._instant_keywords or self._split_keywords(DEFAULT_INSTANT_KEYWORDS)
        digest_kw = self._digest_keywords or self._split_keywords(DEFAULT_DIGEST_KEYWORDS)
        flush_kw = self._flush_keywords or self._split_keywords(DEFAULT_FLUSH_KEYWORDS)
        deny = self._deny_keywords or ["（未设置）"]
        allow = self._allow_keywords or ["（未设置，普通消息全部发送）"]
        total = int(digest.get("total") or 0)
        groups = "、".join(
            f"{k} {int(v.get('count') or 0)} 条"
            for k, v in (digest.get("groups") or {}).items()
        ) or "暂无"
        rules = [
            f"✅ 立刻推送（必发）：{', '.join(instant)}",
            f"✅ 其它普通消息：{'只发送 ' + ', '.join(allow) if self._allow_keywords else '照常立刻推送'}",
            f"⛔ 不发送：{', '.join(deny)}",
            f"📊 合并汇总（不逐条推）：{', '.join(digest_kw)}",
            f"⚑ 跑完立即发当日汇总：{', '.join(flush_kw)}"
            f"{'（已开启）' if self._flush_enabled else '（已关闭）'}",
            f"🕒 兜底汇总：每天 {self._digest_hour:02d}:{self._digest_minute:02d}"
            f"{'，只补发新增' if self._digest_enabled else '（已关闭）'}",
        ]
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
                            "当前生效的推送规则",
                        ],
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "text-body-2 mb-2"},
                                "text": line,
                            }
                            for line in rules
                        ],
                    },
                ],
            },
            {
                "component": "VCard",
                "props": {"class": "mb-4"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {
                                "component": "VIcon",
                                "props": {"icon": "mdi-chart-box-outline", "class": "mr-2"},
                            },
                            "今日累计（等待汇总）",
                        ],
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "text-body-2 mb-1"},
                                "text": f"日期 {digest.get('day') or self._today()} ｜ 共 {total} 条 ｜ {groups}",
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-body-2 mb-1"},
                                "text": f"红包魔力合计：{float(digest.get('magic') or 0):g}",
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-body-2"},
                                "text": f"今日已发结束汇总：{state.get('count') or 0} 条"
                                        f"（{state.get('last_at') or '尚未触发'}）",
                            },
                        ],
                    },
                ],
            },
        ]

    # ---------------- 消息分类与汇总 ----------------

    def _classify(self, msg_type: Any, title: str, text: str):
        """返回 'deny' / 'instant' / ('digest', 关键词) / ('flush', 关键词)。"""
        blob = f"{title or ''}\n{text or ''}"
        # ① 用户明确不发送，优先级最高
        if self._deny_keywords and any(k in blob for k in self._deny_keywords):
            return "deny"
        # ② 重要消息硬直通：即使设了白名单/消息类型，也保证立刻推
        if self._instant_keywords and any(k in blob for k in self._instant_keywords):
            return "instant"
        # ③ 消息类型 / 只发送白名单
        type_name = getattr(msg_type, "name", str(msg_type) if msg_type else "")
        if self._msgtypes and type_name not in self._msgtypes:
            return "deny"
        if self._allow_keywords and not any(k in blob for k in self._allow_keywords):
            return "deny"
        # ④ 运行结束 / 达到上限 → 立即发当日汇总；关闭该功能时仍即时推状态消息
        if self._flush_keywords:
            for keyword in self._flush_keywords:
                if keyword in blob:
                    return ("flush", keyword) if self._flush_enabled else "instant"
        # ⑤ 高频逐条明细 → 累计进汇总
        if self._digest_enabled and self._digest_keywords:
            for keyword in self._digest_keywords:
                if keyword in blob:
                    return ("digest", keyword)
        # ⑥ 其余普通消息照常立刻推
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

    def _load_flush_state(self) -> Dict[str, Any]:
        try:
            data = self.get_data(_FLUSH_KEY) or {}
        except Exception as err:
            logger.warn(f"群机器人结束汇总状态读取失败：{err}")
            data = {}
        if not isinstance(data, dict):
            data = {}
        data.setdefault("day", "")
        data.setdefault("count", 0)
        data.setdefault("last_at", "")
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
        lines.append("失败/异常等重要消息始终即时推送，不在此汇总内；汇总时间与关键词可在插件设置里调整。")
        return "\n".join(lines)

    def _flush_locked(self, data: Dict[str, Any], force: bool = False,
                      label: str = None, reason_title: str = None,
                      reason_text: str = None) -> Dict[str, Any]:
        """发送一条汇总并清空缓冲（调用方需持有锁）。"""
        total = int(data.get("total") or 0)
        day = str(data.get("day") or self._today())
        is_finish = "任务结束" in (label or "")
        if total == 0 and not (force or self._digest_empty_send):
            return {"sent": False, "reason": "empty", "count": 0, "day": day}
        title = f"📊 {label or 'MoviePilot 每日通知汇总'}（{day}）"
        if total == 0:
            text = f"📅 {day} ｜ 本次汇总前没有累计到任何刷流 / 红包消息。"
            if is_finish and self._flush_empty_send:
                text += ("\n⚠️ 若今天本应有任务运行，请检查插件是否启用、Cookie 是否失效、"
                         "任务是否被跳过 —— 这条提醒就是为了不用等到晚上才发现没跑。")
        else:
            text = self._build_digest_text(data)
        if reason_title:
            text += f"\n\n⚑ 本次触发：{reason_title}"
            brief = re.sub(r"\s+", " ", (reason_text or "").strip())[:160]
            if brief:
                text += f"\n{brief}"
        ok = self._push(title, text)
        if ok:
            try:
                self.save_data(_DIGEST_KEY, self._empty_digest(self._today()))
            except Exception as err:
                logger.error(f"群机器人汇总缓存清空失败：{err}")
            logger.info(f"群机器人汇总已发送：{title} / {total} 条")
        else:
            logger.warn(f"群机器人汇总发送失败，保留 {total} 条待下次重试")
        return {"sent": bool(ok), "count": total, "day": day, "title": title}

    def _flush_digest(self, force: bool = False, label: str = None,
                      reason_title: str = None, reason_text: str = None) -> Dict[str, Any]:
        with self._lock:
            return self._flush_locked(
                self._load_digest(), force=force, label=label,
                reason_title=reason_title, reason_text=reason_text)

    def __handle_flush(self, keyword: str, title: str, text: str) -> None:
        """收到「运行结束 / 达到上限」消息：立即发一条当日汇总。"""
        with self._lock:
            state = self._load_flush_state()
            today = self._today()
            if self._flush_once_per_day and state.get("day") == today:
                # 当天已发过结束汇总：状态消息仍即时推送，避免完全静默，但不重复汇总
                logger.info(f"当天已发过结束汇总，状态消息改为即时推送：{title}")
                self._push(title, text)
                return
            label = f"任务结束汇总 · {keyword}"
            result = self._flush_locked(
                self._load_digest(),
                force=self._flush_empty_send,
                label=label,
                reason_title=title,
                reason_text=text,
            )
        if result.get("sent"):
            try:
                self.save_data(_FLUSH_KEY, {
                    "day": today,
                    "count": int(state.get("count") or 0) + 1,
                    "last_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
            except Exception as err:
                logger.error(f"群机器人结束汇总状态写入失败：{err}")
        else:
            # 汇总没发出去也要把结束状态推出来，别把异常吞掉
            logger.warn(f"结束汇总未发出，改为即时推送状态消息：{title}")
            self._push(title, text)

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
            state = self._load_flush_state()
        return {
            "success": True,
            "data": {
                "day": data.get("day"),
                "total": data.get("total"),
                "groups": {k: v.get("count") for k, v in (data.get("groups") or {}).items()},
                "magic": data.get("magic"),
                "details_kept": len(data.get("details") or []),
                "finish_digest_today": state.get("count") if state.get("day") == self._today() else 0,
                "last_finish_at": state.get("last_at"),
            },
        }

    def api_rules(self) -> Dict[str, Any]:
        """只读返回当前生效的分流规则，便于核对「哪些发、哪些不发」。"""
        return {
            "success": True,
            "data": {
                "version": self.plugin_version,
                "enabled": self.get_state(),
                "order": [
                    "1 不发送关键词", "2 重要消息必发（跳过白名单）",
                    "3 消息类型 / 只发送白名单", "4 结束即汇总",
                    "5 高频消息累计", "6 其余照常即时推送",
                ],
                "instant_keywords": self._instant_keywords,
                "deny_keywords": self._deny_keywords,
                "allow_keywords": self._allow_keywords,
                "msgtypes": self._msgtypes,
                "digest_keywords": self._digest_keywords,
                "flush_keywords": self._flush_keywords,
                "flush_enabled": self._flush_enabled,
                "flush_empty_send": self._flush_empty_send,
                "flush_once_per_day": self._flush_once_per_day,
                "digest_enabled": self._digest_enabled,
                "digest_time": f"{self._digest_hour:02d}:{self._digest_minute:02d}",
                "digest_max_items": self._digest_max_items,
            },
        }

    # ---------------- 事件入口 ----------------

    @eventmanager.register(EventType.NoticeMessage)
    def send(self, event: Event) -> None:
        """消息发送事件：按规则转发 / 丢弃 / 累计 / 触发结束汇总。"""
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
            kind, keyword = decision[0], decision[1]
            if kind == "flush":
                self.__handle_flush(keyword, title or "", text or "")
                return
            self._append_digest(keyword, title or "", text or "")
            logger.info(f"消息计入汇总分组 [{keyword}]：{title or '（无标题）'}")
            return
        self._push(title or "", text or "")

    def stop_service(self) -> None:
        """退出插件。"""
        return None
