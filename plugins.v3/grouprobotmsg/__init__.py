"""群机器人消息通知插件（钉钉 / 飞书）。"""

import base64
import hashlib
import hmac
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.network import RequestUtils

DINGTALK_SEND_URL = "https://oapi.dingtalk.com/robot/send"
FEISHU_SEND_PREFIX = "https://open.feishu.cn/open-apis/bot/v2/hook/"


class GroupRobotMsg(_PluginBase):
    """群机器人消息通知插件。

    监听 ``EventType.NoticeMessage``，将 MoviePilot 的通知消息转发到
    钉钉群机器人或飞书群机器人（自定义机器人 Webhook），群内消息完全
    展开，不折叠。两个渠道都配置时同时发送。
    """

    plugin_name = "群机器人通知"
    plugin_desc = "支持将消息通知发送到钉钉群机器人或飞书群机器人（可同时配置）。"
    plugin_icon = "Tang_A.png"
    plugin_version = "1.0.2"
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

    def get_state(self) -> bool:
        """启用且至少配置了一个渠道。"""
        return self._enabled and bool(
            self._dingtalk_token or self._feishu_webhook
        )

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """当前插件不注册远程命令。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """注册测试发送 API。"""
        return [
            {
                "path": "/test",
                "endpoint": self.api_test,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "发送测试消息到群机器人",
            },
        ]

    def api_test(self, title: str = None, text: str = None) -> Dict[str, Any]:
        """手动发送测试消息，验证钉钉/飞书配置。"""
        t = title or "MoviePilot 测试消息"
        x = text or (
            "**MoviePilot 群机器人通知测试**\n\n"
            "如果你在群里看到这条消息，说明通知链路已通。\n\n"
            f"发送时间：{time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        results = {}
        if self._dingtalk_token:
            results["dingtalk"] = self._send_dingtalk(t, x)
        if self._feishu_webhook:
            results["feishu"] = self._send_feishu(t, x)
        if not results:
            return {"success": False, "message": "未配置任何群机器人"}
        ok = all(results.values())
        return {"success": ok, "message": results}

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
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
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
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "钉钉：群设置-智能群助手-添加机器人-自定义机器人，"
                                                "安全设置建议选『加签』，把 Webhook 里的 access_token 和 SEC 密钥填到下面。"
                                                "飞书：群设置-群机器人-添加机器人-自定义机器人，"
                                                "把 Webhook 地址填到下面（签名校验可选）。两个渠道都填则同时发送。"
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
                                        "将 MoviePilot 的各类通知转发到钉钉/飞书群机器人，"
                                        "群消息完全展开显示。钉钉需在群里添加自定义机器人（建议加签），"
                                        "飞书需在群里添加自定义机器人并获取 Webhook。"
                                    ),
                                },
                            }
                        ],
                    },
                ],
            }
        ]

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
        md_text = text or ""
        # 钉钉 markdown 单个 \n 不换行，替换为两个
        md_text = md_text.replace("\n", "\n\n")
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

    @eventmanager.register(EventType.NoticeMessage)
    def send(self, event: Event) -> None:
        """消息发送事件：转发到已配置的群机器人。"""
        if not self.get_state():
            return
        if not event.event_data:
            return

        msg_body = event.event_data
        # 已指定专用通知渠道时不处理
        channel = msg_body.get("channel")
        if channel:
            return

        msg_type: NotificationType = msg_body.get("type")
        title = msg_body.get("title")
        text = msg_body.get("text")

        if not title and not text:
            logger.warn("标题和内容不能同时为空")
            return

        if msg_type and self._msgtypes and msg_type.name not in self._msgtypes:
            logger.info(f"消息类型 {msg_type.value} 未开启群机器人发送")
            return

        ok_any = False
        try:
            if self._dingtalk_token:
                ok_any |= self._send_dingtalk(title or "", text or "")
            if self._feishu_webhook:
                ok_any |= self._send_feishu(title or "", text or "")
        except Exception as msg_e:
            logger.error(f"群机器人消息发送异常：{str(msg_e)}")
        if not ok_any:
            logger.warn("群机器人消息未发送成功（检查渠道配置或网络）")

    def stop_service(self) -> None:
        """退出插件。"""
        return None
