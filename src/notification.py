# -*- coding: utf-8 -*-
"""
===================================
投资帮帮 - 通知模块
===================================

职责：
1. 多渠道推送通知（微信/飞书/Telegram/邮件/自定义Webhook）
2. 通用 Markdown 发送接口
3. 自动检测已配置的渠道
"""

import logging
import smtplib
import re
from datetime import datetime
from typing import List, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from enum import Enum

import requests

from .config import get_config

logger = logging.getLogger(__name__)


class NotificationChannel(Enum):
    WECHAT = "wechat"
    FEISHU = "feishu"
    TELEGRAM = "telegram"
    EMAIL = "email"
    CUSTOM = "custom"


SMTP_CONFIGS = {
    "qq.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    "163.com": {"server": "smtp.163.com", "port": 465, "ssl": True},
    "126.com": {"server": "smtp.126.com", "port": 465, "ssl": True},
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "ssl": False},
    "outlook.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "hotmail.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "aliyun.com": {"server": "smtp.aliyun.com", "port": 465, "ssl": True},
    "139.com": {"server": "smtp.139.com", "port": 465, "ssl": True},
}


class NotificationService:
    """
    通知服务

    向所有已配置的渠道发送 Markdown 消息
    """

    def __init__(self):
        config = get_config()

        self._wechat_url = config.wechat_webhook_url
        self._feishu_url = config.feishu_webhook_url

        self._telegram_config = {
            'bot_token': config.telegram_bot_token,
            'chat_id': config.telegram_chat_id,
        }

        self._email_config = {
            'sender': config.email_sender,
            'password': config.email_password,
            'receivers': config.email_receivers or [],
        }

        self._custom_webhook_urls = config.custom_webhook_urls or []
        self._telegram_api_base_url = config.telegram_api_base_url

        self._available_channels = self._detect_channels()

        if not self._available_channels:
            logger.warning("未配置有效的通知渠道")
        else:
            names = [ch.value for ch in self._available_channels]
            logger.info(f"已配置 {len(self._available_channels)} 个通知渠道：{', '.join(names)}")

    def _detect_channels(self) -> List[NotificationChannel]:
        channels = []
        if self._wechat_url:
            channels.append(NotificationChannel.WECHAT)
        if self._feishu_url:
            channels.append(NotificationChannel.FEISHU)
        if self._telegram_config['bot_token'] and self._telegram_config['chat_id']:
            channels.append(NotificationChannel.TELEGRAM)
        if self._email_config['sender'] and self._email_config['password']:
            channels.append(NotificationChannel.EMAIL)
        if self._custom_webhook_urls:
            channels.append(NotificationChannel.CUSTOM)
        return channels

    def is_available(self) -> bool:
        return len(self._available_channels) > 0

    def send(self, content: str, subject: str = None) -> bool:
        """
        统一发送接口 - 向所有已配置的渠道发送

        Args:
            content: Markdown 格式的消息内容
            subject: 邮件主题（仅邮件渠道使用）

        Returns:
            是否至少有一个渠道发送成功
        """
        if not self.is_available():
            logger.warning("通知服务不可用，跳过推送")
            return False

        success_count = 0

        for channel in self._available_channels:
            try:
                result = False
                if channel == NotificationChannel.WECHAT:
                    result = self._send_wechat(content)
                elif channel == NotificationChannel.FEISHU:
                    result = self._send_feishu(content)
                elif channel == NotificationChannel.TELEGRAM:
                    result = self._send_telegram(content)
                elif channel == NotificationChannel.EMAIL:
                    result = self._send_email(content, subject)
                elif channel == NotificationChannel.CUSTOM:
                    result = self._send_custom(content)

                if result:
                    success_count += 1
            except Exception as e:
                logger.error(f"{channel.value} 发送失败: {e}")

        logger.info(f"通知发送完成：成功 {success_count}/{len(self._available_channels)}")
        return success_count > 0

    def _send_wechat(self, content: str) -> bool:
        """推送到企业微信"""
        if len(content) > 4000:
            content = content[:3950] + "\n\n...(已截断)"

        payload = {"msgtype": "markdown", "markdown": {"content": content}}
        response = requests.post(self._wechat_url, json=payload, timeout=10)

        if response.status_code == 200 and response.json().get('errcode') == 0:
            logger.info("企业微信消息发送成功")
            return True

        logger.error(f"企业微信发送失败: {response.text}")
        return False

    def _send_feishu(self, content: str) -> bool:
        """推送到飞书"""
        payload = {"msg_type": "text", "content": {"text": content}}
        response = requests.post(self._feishu_url, json=payload, timeout=10)

        if response.status_code == 200:
            result = response.json()
            code = result.get('code') if 'code' in result else result.get('StatusCode')
            if code == 0:
                logger.info("飞书消息发送成功")
                return True

        logger.error(f"飞书发送失败: {response.text}")
        return False

    def _send_telegram(self, content: str) -> bool:
        """推送到 Telegram"""
        bot_token = self._telegram_config['bot_token']
        chat_id = self._telegram_config['chat_id']
        api_url = f"{self._telegram_api_base_url}/bot{bot_token}/sendMessage"

        # 简单转换 Markdown 格式
        telegram_text = re.sub(r'^#{1,6}\s+', '', content, flags=re.MULTILINE)
        telegram_text = re.sub(r'\*\*(.+?)\*\*', r'*\1*', telegram_text)

        # 分段发送（4096 字符限制）
        max_len = 4096
        chunks = [telegram_text[i:i + max_len] for i in range(0, len(telegram_text), max_len)]

        all_success = True
        for chunk in chunks:
            payload = {
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            response = requests.post(api_url, json=payload, timeout=10)

            if response.status_code != 200 or not response.json().get('ok'):
                # 回退到纯文本
                payload.pop('parse_mode', None)
                response = requests.post(api_url, json=payload, timeout=10)
                if response.status_code != 200:
                    all_success = False

        if all_success:
            logger.info("Telegram 消息发送成功")
        return all_success

    def send_email_to(self, receivers: List[str], content: str, subject: str = None) -> bool:
        """向指定邮箱发送邮件（用于策略告警等场景）"""
        if not receivers:
            return False
        return self._send_email(content, subject, receivers=receivers)

    def _send_email(self, content: str, subject: str = None, receivers: List[str] = None) -> bool:
        """通过 SMTP 发送邮件"""
        sender = self._email_config['sender']
        password = self._email_config['password']
        if receivers is None:
            receivers = self._email_config['receivers']

        if not receivers:
            logger.debug("邮件接收人为空，跳过发送")
            return False

        if subject is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            subject = f"基金估值报告 - {date_str}"

        try:
            html_content = self._markdown_to_html(content)

            msg = MIMEMultipart('alternative')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = sender
            msg['To'] = ', '.join(receivers)

            msg.attach(MIMEText(content, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain, {
                'server': f'smtp.{domain}', 'port': 465, 'ssl': True
            })

            if smtp_config['ssl']:
                server = smtplib.SMTP_SSL(smtp_config['server'], smtp_config['port'], timeout=30)
            else:
                server = smtplib.SMTP(smtp_config['server'], smtp_config['port'], timeout=30)
                server.starttls()

            server.login(sender, password)
            server.send_message(msg)
            server.quit()

            logger.info(f"邮件发送成功，收件人: {receivers}")
            return True
        except Exception as e:
            logger.error(f"发送邮件失败: {e}")
            return False

    def _send_custom(self, content: str) -> bool:
        """推送到自定义 Webhook"""
        success = 0
        for url in self._custom_webhook_urls:
            try:
                payload = self._build_custom_payload(url, content)
                response = requests.post(url, json=payload, timeout=30)
                if response.status_code == 200:
                    success += 1
            except Exception as e:
                logger.error(f"自定义 Webhook 推送失败: {e}")

        return success > 0

    def _build_custom_payload(self, url: str, content: str) -> dict:
        """根据 URL 构建 Webhook payload"""
        url_lower = url.lower()

        if 'dingtalk' in url_lower:
            return {"msgtype": "markdown", "markdown": {"title": "基金估值报告", "text": content}}
        if 'discord.com' in url_lower:
            return {"content": content[:1900]}
        if 'hooks.slack.com' in url_lower:
            return {"text": content, "mrkdwn": True}

        return {"text": content, "content": content}

    @staticmethod
    def _markdown_to_html(text: str) -> str:
        """简单 Markdown 转 HTML"""
        html = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
        html = re.sub(r'^---$', r'<hr>', html, flags=re.MULTILINE)
        html = re.sub(r'^- (.+)$', r'<li>\1</li>', html, flags=re.MULTILINE)
        html = html.replace('\n', '<br>\n')
        return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
        <style>body{{font-family:sans-serif;line-height:1.6;padding:20px;max-width:800px;margin:0 auto}}</style>
        </head><body>{html}</body></html>"""
