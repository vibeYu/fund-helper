# -*- coding: utf-8 -*-
"""
===================================
天天基金 JSONP API 数据源（优先级1）
===================================

数据源：http://fundgz.1234567.com.cn/js/{fund_code}.js
返回格式：JSONP - jsonpgz({...})

注意：2026年1月监管要求各平台下架基金实时估值功能，此 API 可能随时失效
"""

import json
import logging
import re
from datetime import datetime, date
from typing import Optional, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .base import (
    BaseFundFetcher,
    FundValuationResult,
    FundNavHistoryItem,
    FundFetchError,
    JSONPParseError,
    DataSourceUnavailableError,
)

logger = logging.getLogger(__name__)


class EastMoneyFetcher(BaseFundFetcher):
    """
    天天基金 JSONP API 数据源

    API 端点：http://fundgz.1234567.com.cn/js/{fund_code}.js?rt={timestamp}
    返回：jsonpgz({"fundcode":"161725","name":"招商中证白酒","dwjz":"1.3768",
                    "gsz":"1.3797","gszzl":"0.21","gztime":"2026-02-07 14:30"})
    """

    name = "天天基金"
    priority = 1

    def __init__(self):
        from ..config import get_config
        config = get_config()
        self._valuation_url = config.eastmoney_valuation_url
        self._nav_history_url = config.eastmoney_nav_url
        self._session = self.create_session()
        self._session.headers.update({
            'Referer': config.eastmoney_referer,
            'User-Agent': self.get_random_ua(),
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, JSONPParseError)),
        reraise=True,
    )
    def fetch_valuation(self, fund_code: str) -> Optional[FundValuationResult]:
        """获取单只基金的实时估值"""
        url = self._valuation_url.format(code=fund_code)
        params = {'rt': int(datetime.now().timestamp() * 1000)}

        self._session.headers['User-Agent'] = self.get_random_ua()
        response = self._session.get(url, params=params, timeout=10)
        response.raise_for_status()

        text = response.text.strip()
        if not text or 'jsonpgz' not in text:
            raise DataSourceUnavailableError(f"无效响应: {text[:100]}")

        # 解析 JSONP：jsonpgz({...})
        match = re.search(r'jsonpgz\((.+)\)', text)
        if not match:
            raise JSONPParseError(f"JSONP 解析失败: {text[:100]}")

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError as e:
            raise JSONPParseError(f"JSON 解析失败: {e}")

        # 解析估值时间
        valuation_time = None
        gztime = data.get('gztime', '')
        if gztime:
            try:
                valuation_time = datetime.strptime(gztime, '%Y-%m-%d %H:%M')
            except ValueError:
                valuation_time = datetime.now()

        # 解析前日净值日期
        prev_nav_date = None
        jzrq = data.get('jzrq', '')
        if jzrq:
            try:
                prev_nav_date = datetime.strptime(jzrq, '%Y-%m-%d').date()
            except ValueError:
                pass

        return FundValuationResult(
            fund_code=data.get('fundcode', fund_code),
            fund_name=data.get('name', ''),
            estimate_nav=float(data.get('gsz', 0)),
            estimate_pct=float(data.get('gszzl', 0)),
            prev_nav=float(data.get('dwjz', 0)),
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
        """
        获取历史净值（天天基金 F10DataApi）

        API: http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={code}&page=1&per=30
        返回 HTML 表格
        """
        params = {
            'type': 'lsjz',
            'code': fund_code,
            'page': 1,
            'per': days,
        }

        self._session.headers['User-Agent'] = self.get_random_ua()
        response = self._session.get(self._nav_history_url, params=params, timeout=10)
        response.raise_for_status()

        text = response.text

        # 解析 HTML 表格
        results = []
        # 匹配表格行：<td>日期</td><td>单位净值</td><td>累计净值</td><td>日增长率</td>...
        rows = re.findall(r'<tr>(.*?)</tr>', text, re.DOTALL)

        for row in rows:
            cells = re.findall(r'<td>(.*?)</td>', row)
            if len(cells) >= 4:
                try:
                    nav_date_str = cells[0].strip()
                    nav = cells[1].strip()
                    acc_nav = cells[2].strip()
                    daily_return_str = cells[3].strip().replace('%', '')

                    # 跳过表头
                    if not re.match(r'\d{4}-\d{2}-\d{2}', nav_date_str):
                        continue

                    nav_date = datetime.strptime(nav_date_str, '%Y-%m-%d').date()

                    results.append(FundNavHistoryItem(
                        fund_code=fund_code,
                        nav_date=nav_date,
                        nav=float(nav) if nav else 0.0,
                        acc_nav=float(acc_nav) if acc_nav else None,
                        daily_return=float(daily_return_str) if daily_return_str else None,
                        data_source=self.name,
                    ))
                except (ValueError, IndexError) as e:
                    logger.debug(f"跳过无效行: {e}")

        logger.info(f"[{self.name}] 获取 {fund_code} 历史净值 {len(results)} 条")
        return results
