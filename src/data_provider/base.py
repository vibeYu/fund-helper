# -*- coding: utf-8 -*-
"""
===================================
数据源基类与管理器
===================================

设计模式：策略模式 (Strategy Pattern)
- BaseFundFetcher: 抽象基类，定义统一接口
- FundFetcherManager: 策略管理器，实现自动切换

防封禁策略：
1. 随机 User-Agent 轮换
2. 请求间随机延迟
3. 失败自动切换到下一个数据源
"""

import logging
import random
import re
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


# === 自定义异常 ===

class FundFetchError(Exception):
    """数据获取异常基类"""
    pass


class RateLimitError(FundFetchError):
    """API 速率限制异常"""
    pass


class DataSourceUnavailableError(FundFetchError):
    """数据源不可用异常"""
    pass


class JSONPParseError(FundFetchError):
    """JSONP 解析异常"""
    pass


# === 标准化数据结构 ===

@dataclass
class FundValuationResult:
    """标准化的基金估值数据结构"""
    fund_code: str
    fund_name: str = ""
    estimate_nav: float = 0.0       # 估算净值
    estimate_pct: float = 0.0       # 估算涨跌幅%
    prev_nav: float = 0.0           # 前日官方净值
    prev_nav_date: date = None      # 前日净值日期
    valuation_time: datetime = None  # 估值更新时间
    data_source: str = ""           # 数据来源

    def to_dict(self) -> Dict[str, Any]:
        return {
            'fund_code': self.fund_code,
            'fund_name': self.fund_name,
            'estimate_nav': self.estimate_nav,
            'estimate_pct': self.estimate_pct,
            'prev_nav': self.prev_nav,
            'prev_nav_date': self.prev_nav_date,
            'valuation_time': self.valuation_time,
            'data_source': self.data_source,
        }


@dataclass
class FundNavHistoryItem:
    """历史净值数据项"""
    fund_code: str
    nav_date: date
    nav: float
    acc_nav: float = None
    daily_return: float = None
    data_source: str = ""


# === 抽象基类 ===

class BaseFundFetcher(ABC):
    """
    基金数据源抽象基类

    子类需实现：
    - fetch_valuation(): 获取单只基金的实时估值
    - fetch_nav_history(): 获取历史净值（可选）
    """

    name: str = "BaseFundFetcher"
    priority: int = 99  # 优先级数字越小越优先

    @abstractmethod
    def fetch_valuation(self, fund_code: str) -> Optional[FundValuationResult]:
        """
        获取单只基金的实时估值

        Args:
            fund_code: 基金代码

        Returns:
            FundValuationResult 或 None
        """
        pass

    # 并发控制参数
    max_workers: int = 5
    concurrent_sleep_range: tuple = (0.3, 0.8)

    def fetch_valuations_batch(self, fund_codes: List[str]) -> List[FundValuationResult]:
        """
        批量获取基金估值（线程池并发，max_workers 控制并发数）

        Args:
            fund_codes: 基金代码列表

        Returns:
            FundValuationResult 列表
        """
        if len(fund_codes) <= 2:
            # 数量少时直接串行，不值得开线程池
            results = []
            for code in fund_codes:
                try:
                    result = self.fetch_valuation(code)
                    if result:
                        results.append(result)
                    self.random_sleep(*self.concurrent_sleep_range)
                except Exception as e:
                    logger.warning(f"[{self.name}] 获取 {code} 估值失败: {e}")
            return results

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {}
            for code in fund_codes:
                future = pool.submit(self._fetch_one, code)
                futures[future] = code

            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f"[{self.name}] 获取 {code} 估值失败: {e}")

        return results

    def _fetch_one(self, fund_code: str) -> Optional[FundValuationResult]:
        """单只抓取（供线程池调用，带 sleep 控制节奏）"""
        self.random_sleep(*self.concurrent_sleep_range)
        return self.fetch_valuation(fund_code)

    def fetch_nav_history(self, fund_code: str, days: int = 30) -> List[FundNavHistoryItem]:
        """
        获取历史净值（可选实现）

        Args:
            fund_code: 基金代码
            days: 获取天数

        Returns:
            FundNavHistoryItem 列表
        """
        logger.debug(f"[{self.name}] 未实现 fetch_nav_history")
        return []

    @staticmethod
    def create_session() -> 'requests.Session':
        """
        创建带代理配置的 requests Session

        根据 Config.proxy_url 设置：
        - "none" → 强制直连（不走系统代理）
        - 具体 URL → 走指定代理
        - 空 → 跟随系统环境变量
        """
        import requests
        from ..config import get_config

        session = requests.Session()
        config = get_config()
        proxy_url = (config.proxy_url or '').strip().lower()

        if proxy_url == 'none':
            # 强制直连：禁止 requests 读取系统环境变量中的代理
            session.trust_env = False
        elif proxy_url:
            # 走指定代理
            session.proxies.update({"http": config.proxy_url, "https": config.proxy_url})

        # 空字符串 = 跟随系统环境变量，不做任何处理
        return session

    @staticmethod
    def random_sleep(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
        """随机延迟（防反爬）"""
        sleep_time = random.uniform(min_seconds, max_seconds)
        logger.debug(f"随机休眠 {sleep_time:.2f} 秒...")
        time.sleep(sleep_time)

    @staticmethod
    def get_random_ua() -> str:
        """获取随机 User-Agent"""
        try:
            from fake_useragent import UserAgent
            return UserAgent().random
        except Exception:
            return (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )


# === 数据源管理器 ===

class FundFetcherManager:
    """
    数据源策略管理器

    职责：
    1. 管理多个数据源（按优先级排序）
    2. 自动故障切换（Failover）
    3. 提供统一的数据获取接口
    """

    def __init__(self, fetchers: Optional[List[BaseFundFetcher]] = None):
        self._fetchers: List[BaseFundFetcher] = []

        if fetchers:
            self._fetchers = sorted(fetchers, key=lambda f: f.priority)
        else:
            self._init_default_fetchers()

    def _init_default_fetchers(self) -> None:
        """初始化默认数据源列表（按优先级）"""
        from .eastmoney_fetcher import EastMoneyFetcher
        from .eastmoney_detail_fetcher import EastMoneyDetailFetcher
        from .danjuan_fetcher import DanjuanFetcher
        from .akshare_fetcher import AkshareFundFetcher

        self._fetchers = [
            EastMoneyFetcher(),         # 优先级 1
            EastMoneyDetailFetcher(),   # 优先级 2
            DanjuanFetcher(),           # 优先级 3
            AkshareFundFetcher(),       # 优先级 4
        ]

        self._fetchers.sort(key=lambda f: f.priority)
        logger.info(
            f"已初始化 {len(self._fetchers)} 个数据源: "
            + ", ".join([f.name for f in self._fetchers])
        )

    def add_fetcher(self, fetcher: BaseFundFetcher) -> None:
        """添加数据源并重新排序"""
        self._fetchers.append(fetcher)
        self._fetchers.sort(key=lambda f: f.priority)

    def get_valuation(self, fund_code: str) -> Optional[FundValuationResult]:
        """
        获取单只基金估值（自动切换数据源）

        Args:
            fund_code: 基金代码

        Returns:
            FundValuationResult 或 None
        """
        errors = []

        for fetcher in self._fetchers:
            try:
                logger.info(f"尝试使用 [{fetcher.name}] 获取 {fund_code}...")
                result = fetcher.fetch_valuation(fund_code)

                if result:
                    logger.info(f"[{fetcher.name}] 成功获取 {fund_code}")
                    return result

            except Exception as e:
                error_msg = f"[{fetcher.name}] 失败: {e}"
                logger.warning(error_msg)
                errors.append(error_msg)

        error_summary = f"所有数据源获取 {fund_code} 失败:\n" + "\n".join(errors)
        logger.error(error_summary)
        return None

    def get_valuations_batch(self, fund_codes: List[str]) -> List[FundValuationResult]:
        """
        批量获取基金估值（自动切换数据源）

        策略：先用最高优先级批量获取，失败的再逐个用备用源重试
        """
        results = []
        failed_codes = list(fund_codes)

        for fetcher in self._fetchers:
            if not failed_codes:
                break

            try:
                logger.info(f"使用 [{fetcher.name}] 批量获取 {len(failed_codes)} 只基金...")
                batch_results = fetcher.fetch_valuations_batch(failed_codes)

                # 收集成功的
                success_codes = set()
                for r in batch_results:
                    results.append(r)
                    success_codes.add(r.fund_code)

                # 更新失败列表
                failed_codes = [c for c in failed_codes if c not in success_codes]

                if success_codes:
                    logger.info(f"[{fetcher.name}] 成功获取 {len(success_codes)} 只")
                if failed_codes:
                    logger.warning(f"[{fetcher.name}] 还有 {len(failed_codes)} 只失败，尝试下一数据源")

            except Exception as e:
                logger.warning(f"[{fetcher.name}] 批量获取失败: {e}")

        if failed_codes:
            logger.error(f"以下基金所有数据源均失败: {failed_codes}")

        return results

    def get_nav_history(self, fund_code: str, days: int = 30) -> List[FundNavHistoryItem]:
        """获取历史净值（自动切换数据源）"""
        for fetcher in self._fetchers:
            try:
                history = fetcher.fetch_nav_history(fund_code, days)
                if history:
                    logger.info(f"[{fetcher.name}] 成功获取 {fund_code} 历史净值 {len(history)} 条")
                    return history
            except Exception as e:
                logger.warning(f"[{fetcher.name}] 获取 {fund_code} 历史净值失败: {e}")

        logger.error(f"所有数据源获取 {fund_code} 历史净值失败")
        return []

    def fetch_fund_types(self, fund_codes: List[str]) -> Dict[str, str]:
        """
        从天天基金 fundcode_search.js 获取基金类型映射

        Args:
            fund_codes: 需要查询类型的基金代码列表

        Returns:
            {fund_code: fund_type} 字典，失败返回空 dict
        """
        url = 'http://fund.eastmoney.com/js/fundcode_search.js'
        try:
            import requests
            session = BaseFundFetcher.create_session()
            headers = {'User-Agent': BaseFundFetcher.get_random_ua()}
            resp = session.get(url, headers=headers, timeout=15)
            resp.raise_for_status()

            text = resp.text
            # 提取 JS 数组内容: var r = [["000001",...],...]
            match = re.search(r'\[(\[.*\])\]', text, re.DOTALL)
            if not match:
                logger.warning("fetch_fund_types: 无法解析 fundcode_search.js")
                return {}

            # 解析为 Python 列表
            import json
            array_str = '[' + match.group(1) + ']'
            data = json.loads(array_str)

            code_set = set(fund_codes)
            result = {}
            for item in data:
                if len(item) >= 4 and item[0] in code_set:
                    fund_type = item[3].strip()
                    if fund_type:
                        result[item[0]] = fund_type

            logger.info(f"fetch_fund_types: 成功匹配 {len(result)}/{len(fund_codes)} 只基金类型")
            return result

        except Exception as e:
            logger.warning(f"fetch_fund_types 失败: {e}")
            return {}

    @property
    def available_fetchers(self) -> List[str]:
        return [f.name for f in self._fetchers]
