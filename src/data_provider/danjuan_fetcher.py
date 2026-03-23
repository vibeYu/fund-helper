# -*- coding: utf-8 -*-
"""
===================================
蛋卷基金 API 数据源（优先级3）
===================================

数据源：https://danjuanapp.com/djapi/fund/detail/{code}
"""

import json
import logging
from datetime import datetime, date
from typing import Optional, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .base import (
    BaseFundFetcher,
    FundValuationResult,
    FundNavHistoryItem,
    FundFetchError,
    DataSourceUnavailableError,
)

logger = logging.getLogger(__name__)


class DanjuanFetcher(BaseFundFetcher):
    """
    蛋卷基金 API 数据源

    API 端点：https://danjuanapp.com/djapi/fund/detail/{fund_code}
    """

    name = "蛋卷基金"
    priority = 3

    def __init__(self):
        from ..config import get_config
        config = get_config()
        self._detail_url = config.danjuan_detail_url
        self._nav_history_url = config.danjuan_nav_url
        self._session = self.create_session()
        self._session.headers.update({
            'User-Agent': self.get_random_ua(),
            'Referer': config.danjuan_referer,
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def fetch_valuation(self, fund_code: str) -> Optional[FundValuationResult]:
        """获取单只基金的实时估值"""
        url = self._detail_url.format(code=fund_code)
        self._session.headers['User-Agent'] = self.get_random_ua()
        response = self._session.get(url, timeout=10)
        response.raise_for_status()

        try:
            resp_data = response.json()
        except json.JSONDecodeError:
            raise FundFetchError(f"JSON 解析失败: {response.text[:100]}")

        data = resp_data.get('data', {})
        if not data:
            raise DataSourceUnavailableError(f"蛋卷基金返回数据为空: {fund_code}")

        fund_derived = data.get('fund_derived', {}) or {}
        fund_detail = data.get('fund_detail', {}) or {}

        # 估值数据
        estimate_nav = fund_derived.get('estimate_value', '')
        estimate_pct = fund_derived.get('estimate_growth', '')
        estimate_time = fund_derived.get('estimate_time', '')
        prev_nav = fund_derived.get('unit_nav', '')
        nav_date = fund_derived.get('nav_date', '')
        fund_name = fund_detail.get('fund_name', '')

        # 解析时间
        valuation_time = None
        if estimate_time:
            try:
                valuation_time = datetime.strptime(estimate_time, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    valuation_time = datetime.strptime(estimate_time, '%Y-%m-%d %H:%M')
                except ValueError:
                    valuation_time = datetime.now()

        prev_nav_date = None
        if nav_date:
            try:
                prev_nav_date = datetime.strptime(nav_date, '%Y-%m-%d').date()
            except ValueError:
                pass

        return FundValuationResult(
            fund_code=fund_code,
            fund_name=fund_name,
            estimate_nav=float(estimate_nav) if estimate_nav else 0.0,
            estimate_pct=float(estimate_pct) if estimate_pct else 0.0,
            prev_nav=float(prev_nav) if prev_nav else 0.0,
            prev_nav_date=prev_nav_date,
            valuation_time=valuation_time,
            data_source=self.name,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def fetch_nav_history(self, fund_code: str, days: int = 30) -> List[FundNavHistoryItem]:
        """获取历史净值"""
        url = self._nav_history_url.format(code=fund_code)
        params = {'size': days, 'page': 1}

        self._session.headers['User-Agent'] = self.get_random_ua()
        response = self._session.get(url, params=params, timeout=10)
        response.raise_for_status()

        try:
            resp_data = response.json()
        except json.JSONDecodeError:
            raise FundFetchError(f"JSON 解析失败")

        data = resp_data.get('data', {})
        items = data.get('items', [])

        results = []
        for item in items:
            try:
                nav_date_str = item.get('date', '')
                if not nav_date_str:
                    continue

                nav_date = datetime.strptime(nav_date_str, '%Y-%m-%d').date()
                results.append(FundNavHistoryItem(
                    fund_code=fund_code,
                    nav_date=nav_date,
                    nav=float(item.get('value', 0)),
                    acc_nav=float(item.get('total_value', 0)) if item.get('total_value') else None,
                    daily_return=float(item.get('percentage', 0)) if item.get('percentage') else None,
                    data_source=self.name,
                ))
            except (ValueError, TypeError) as e:
                logger.debug(f"跳过无效数据: {e}")

        return results
