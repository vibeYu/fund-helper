# -*- coding: utf-8 -*-
"""
===================================
天天基金移动端 API 数据源（优先级2）
===================================

数据源：https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo
备用数据源，当主 JSONP API 失效时使用
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


class EastMoneyDetailFetcher(BaseFundFetcher):
    """
    天天基金移动端 API 数据源

    API 端点：https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo
    参数：FCODE={fund_code}&plat=Android&appType=ttjj&product=EFund&Version=1&deviceid=xxx
    """

    name = "天天基金(移动端)"
    priority = 2

    def __init__(self):
        from ..config import get_config
        config = get_config()
        self._api_url = config.eastmoney_mobile_api_url
        self._session = self.create_session()
        self._session.headers.update({
            'User-Agent': 'okhttp/3.12.1',
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def fetch_valuation(self, fund_code: str) -> Optional[FundValuationResult]:
        """获取单只基金的实时估值"""
        params = {
            'FCODE': fund_code,
            'plat': 'Android',
            'appType': 'ttjj',
            'product': 'EFund',
            'Version': '1',
            'deviceid': 'fund_valuation_tool',
        }

        response = self._session.get(self._api_url, params=params, timeout=10)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            raise FundFetchError(f"JSON 解析失败: {response.text[:100]}")

        if data.get('ErrCode') != 0:
            raise DataSourceUnavailableError(f"API 返回错误: {data.get('ErrMsg', 'Unknown')}")

        expansion = data.get('Expansion', {})
        if not expansion:
            raise FundFetchError(f"返回数据为空: {fund_code}")

        # 解析估值数据
        gz = expansion.get('GZ', '')      # 估算净值
        gszzl = expansion.get('GSZZL', '')  # 估算涨跌幅
        gztime = expansion.get('GZTIME', '')  # 估值时间
        dwjz = expansion.get('DWJZ', '')   # 前日净值
        fsrq = expansion.get('FSRQ', '')   # 前日净值日期
        shortname = expansion.get('SHORTNAME', '')

        # 解析时间
        valuation_time = None
        if gztime:
            try:
                valuation_time = datetime.strptime(gztime, '%Y-%m-%d %H:%M')
            except ValueError:
                try:
                    valuation_time = datetime.strptime(gztime, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    valuation_time = datetime.now()

        prev_nav_date = None
        if fsrq:
            try:
                prev_nav_date = datetime.strptime(fsrq, '%Y-%m-%d').date()
            except ValueError:
                pass

        return FundValuationResult(
            fund_code=fund_code,
            fund_name=shortname,
            estimate_nav=float(gz) if gz else 0.0,
            estimate_pct=float(gszzl) if gszzl else 0.0,
            prev_nav=float(dwjz) if dwjz else 0.0,
            prev_nav_date=prev_nav_date,
            valuation_time=valuation_time,
            data_source=self.name,
        )
