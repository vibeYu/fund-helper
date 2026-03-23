# -*- coding: utf-8 -*-
"""
===================================
基金估值助手 - 配置管理模块
===================================

职责：
1. 使用单例模式管理全局配置
2. 从 .env 文件加载敏感配置
3. 提供类型安全的配置访问接口
"""

import os
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv
from dataclasses import dataclass, field


@dataclass
class Config:
    """
    系统配置类 - 单例模式

    设计说明：
    - 使用 dataclass 简化配置属性定义
    - 所有配置项从环境变量读取，支持默认值
    - 类方法 get_instance() 实现单例访问
    """

    # === 基金列表配置 ===
    fund_list: List[str] = field(default_factory=list)
    fund_aliases: Dict[str, str] = field(default_factory=dict)

    # === 估值配置 ===
    refresh_interval: int = 60  # 刷新间隔（秒），仅 interval 模式生效
    schedule_times: List[str] = field(default_factory=list)  # 定时触发时间列表，如 ["11:30","14:30"]
    alert_rise_pct: float = 2.0  # 涨幅告警阈值%
    alert_drop_pct: float = -2.0  # 跌幅告警阈值%

    # === Web 服务配置 ===
    web_host: str = "0.0.0.0"
    web_port: int = 5000

    # === 通知配置（可同时配置多个，全部推送）===
    wechat_webhook_url: Optional[str] = None
    feishu_webhook_url: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    email_sender: Optional[str] = None
    email_password: Optional[str] = None
    email_receivers: List[str] = field(default_factory=list)
    custom_webhook_urls: List[str] = field(default_factory=list)

    # === 数据库配置 (SQLite) ===
    sqlite_path: str = "./data/fund.db"

    # === 日志配置 ===
    log_dir: str = "./logs"
    log_level: str = "INFO"

    # === 数据源 API 配置 ===
    eastmoney_valuation_url: str = "http://fundgz.1234567.com.cn/js/{code}.js"
    eastmoney_nav_url: str = "http://fund.eastmoney.com/f10/F10DataApi.aspx"
    eastmoney_referer: str = "http://fund.eastmoney.com/"
    eastmoney_mobile_api_url: str = "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo"
    danjuan_detail_url: str = "https://danjuanapp.com/djapi/fund/detail/{code}"
    danjuan_nav_url: str = "https://danjuanapp.com/djapi/fund/nav/history/{code}"
    danjuan_referer: str = "https://danjuanapp.com/"
    telegram_api_base_url: str = "https://api.telegram.org"

    # === 网络代理配置 ===
    # 设为 "none" 表示强制不走代理（即使系统有代理）
    # 设为具体地址如 "http://127.0.0.1:7890" 表示走指定代理
    # 留空表示跟随系统环境变量
    proxy_url: Optional[str] = None

    # === 系统配置 ===
    debug: bool = False

    # 单例实例存储
    _instance: Optional['Config'] = None

    @classmethod
    def get_instance(cls) -> 'Config':
        """获取配置单例实例"""
        if cls._instance is None:
            cls._instance = cls._load_from_env()
        return cls._instance

    @classmethod
    def _load_from_env(cls) -> 'Config':
        """从 .env 文件加载配置"""
        env_path = Path(__file__).parent.parent / '.env'
        load_dotenv(dotenv_path=env_path)

        # 解析基金列表
        fund_list_str = os.getenv('FUND_LIST', '')
        fund_list = [code.strip() for code in fund_list_str.split(',') if code.strip()]
        if not fund_list:
            fund_list = ['161725', '110011', '110020']

        # 解析基金简称映射（格式：161725:白酒,110011:中小盘）
        aliases_str = os.getenv('FUND_ALIASES', '')
        fund_aliases = {}
        for pair in aliases_str.split(','):
            pair = pair.strip()
            if ':' in pair:
                code, alias = pair.split(':', 1)
                fund_aliases[code.strip()] = alias.strip()

        return cls(
            fund_list=fund_list,
            fund_aliases=fund_aliases,
            refresh_interval=int(os.getenv('REFRESH_INTERVAL', '60')),
            schedule_times=[t.strip() for t in os.getenv('SCHEDULE_TIMES', '').split(',') if t.strip()],
            alert_rise_pct=float(os.getenv('ALERT_RISE_PCT', '2.0')),
            alert_drop_pct=float(os.getenv('ALERT_DROP_PCT', '-2.0')),
            web_host=os.getenv('WEB_HOST', '0.0.0.0'),
            web_port=int(os.getenv('WEB_PORT', '5000')),
            wechat_webhook_url=os.getenv('WECHAT_WEBHOOK_URL'),
            feishu_webhook_url=os.getenv('FEISHU_WEBHOOK_URL'),
            telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN'),
            telegram_chat_id=os.getenv('TELEGRAM_CHAT_ID'),
            email_sender=os.getenv('EMAIL_SENDER'),
            email_password=os.getenv('EMAIL_PASSWORD'),
            email_receivers=[r.strip() for r in os.getenv('EMAIL_RECEIVERS', '').split(',') if r.strip()],
            custom_webhook_urls=[u.strip() for u in os.getenv('CUSTOM_WEBHOOK_URLS', '').split(',') if u.strip()],
            eastmoney_valuation_url=os.getenv('EASTMONEY_VALUATION_URL', 'http://fundgz.1234567.com.cn/js/{code}.js'),
            eastmoney_nav_url=os.getenv('EASTMONEY_NAV_URL', 'http://fund.eastmoney.com/f10/F10DataApi.aspx'),
            eastmoney_referer=os.getenv('EASTMONEY_REFERER', 'http://fund.eastmoney.com/'),
            eastmoney_mobile_api_url=os.getenv('EASTMONEY_MOBILE_API_URL', 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo'),
            danjuan_detail_url=os.getenv('DANJUAN_DETAIL_URL', 'https://danjuanapp.com/djapi/fund/detail/{code}'),
            danjuan_nav_url=os.getenv('DANJUAN_NAV_URL', 'https://danjuanapp.com/djapi/fund/nav/history/{code}'),
            danjuan_referer=os.getenv('DANJUAN_REFERER', 'https://danjuanapp.com/'),
            telegram_api_base_url=os.getenv('TELEGRAM_API_BASE_URL', 'https://api.telegram.org'),
            sqlite_path=os.getenv('SQLITE_PATH', './data/fund.db'),
            log_dir=os.getenv('LOG_DIR', './logs'),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            proxy_url=os.getenv('PROXY_URL', 'none'),
            debug=os.getenv('DEBUG', 'false').lower() == 'true',
        )

    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（主要用于测试）"""
        cls._instance = None

    def get_fund_alias(self, fund_code: str) -> str:
        """获取基金简称，没有则返回基金代码"""
        return self.fund_aliases.get(fund_code, fund_code)

    def validate(self) -> List[str]:
        """验证配置完整性"""
        warnings = []

        if not self.fund_list:
            warnings.append("警告：未配置基金列表 (FUND_LIST)")

        has_notification = (
            self.wechat_webhook_url
            or self.feishu_webhook_url
            or (self.telegram_bot_token and self.telegram_chat_id)
            or (self.email_sender and self.email_password)
        )
        if not has_notification:
            warnings.append("提示：未配置通知渠道，将不发送推送通知")

        return warnings

    def get_proxies(self) -> Optional[Dict[str, str]]:
        """获取 requests 代理配置"""
        if not self.proxy_url:
            return None
        if self.proxy_url.lower() == 'none':
            return {"http": "", "https": ""}
        return {"http": self.proxy_url, "https": self.proxy_url}

    def get_db_url(self) -> str:
        """获取 SQLAlchemy 数据库连接 URL (SQLite)"""
        return f"sqlite:///{self.sqlite_path}"


def get_config() -> Config:
    """获取全局配置实例的快捷方式"""
    return Config.get_instance()
