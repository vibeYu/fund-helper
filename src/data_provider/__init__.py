# -*- coding: utf-8 -*-
"""数据源包 - 策略模式实现多数据源采集"""

from .base import FundFetcherManager, BaseFundFetcher, FundValuationResult

__all__ = ['FundFetcherManager', 'BaseFundFetcher', 'FundValuationResult']
