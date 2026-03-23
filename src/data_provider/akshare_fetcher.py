# -*- coding: utf-8 -*-
"""
===================================
akshare 数据源（优先级4）
===================================

使用 akshare 库获取基金数据，主要用于：
1. 历史净值获取
2. 作为兜底数据源
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List

from .base import (
    BaseFundFetcher,
    FundValuationResult,
    FundNavHistoryItem,
    FundFetchError,
)

logger = logging.getLogger(__name__)


class AkshareFundFetcher(BaseFundFetcher):
    """
    akshare 基金数据源

    主要用于历史净值获取，估值功能依赖其他 API
    """

    name = "AKShare"
    priority = 4

    def fetch_valuation(self, fund_code: str) -> Optional[FundValuationResult]:
        """
        通过 akshare 获取基金估值

        akshare 没有直接的实时估值接口，此方法通过获取最新净值数据模拟
        实际效果不如专用 API，仅作为兜底
        """
        try:
            import akshare as ak

            # 尝试获取基金实时估值
            try:
                df = ak.fund_etf_fund_info_em(fund=fund_code)
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    return FundValuationResult(
                        fund_code=fund_code,
                        fund_name=str(latest.get('基金简称', '')),
                        estimate_nav=float(latest.get('单位净值', 0)),
                        estimate_pct=float(latest.get('日增长率', 0)),
                        prev_nav=float(latest.get('单位净值', 0)),
                        valuation_time=datetime.now(),
                        data_source=self.name,
                    )
            except Exception:
                pass

            # 回退：获取开放式基金净值
            df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                nav_date = latest.get('净值日期')
                if hasattr(nav_date, 'date'):
                    nav_date = nav_date.date()
                elif isinstance(nav_date, str):
                    nav_date = datetime.strptime(nav_date, '%Y-%m-%d').date()

                return FundValuationResult(
                    fund_code=fund_code,
                    fund_name='',
                    estimate_nav=float(latest.get('单位净值', 0)),
                    estimate_pct=float(latest.get('日增长率', 0)) if '日增长率' in latest.index else 0.0,
                    prev_nav=float(latest.get('单位净值', 0)),
                    prev_nav_date=nav_date,
                    valuation_time=datetime.now(),
                    data_source=self.name,
                )

        except ImportError:
            logger.warning("akshare 未安装，跳过此数据源")
        except Exception as e:
            logger.warning(f"[{self.name}] 获取 {fund_code} 失败: {e}")

        return None

    def fetch_nav_history(self, fund_code: str, days: int = 30) -> List[FundNavHistoryItem]:
        """通过 akshare 获取历史净值"""
        try:
            import akshare as ak

            df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
            if df is None or df.empty:
                return []

            # 取最近 N 条
            df = df.tail(days)

            results = []
            for _, row in df.iterrows():
                try:
                    nav_date = row.get('净值日期')
                    if hasattr(nav_date, 'date'):
                        nav_date = nav_date.date()
                    elif isinstance(nav_date, str):
                        nav_date = datetime.strptime(nav_date, '%Y-%m-%d').date()
                    else:
                        continue

                    results.append(FundNavHistoryItem(
                        fund_code=fund_code,
                        nav_date=nav_date,
                        nav=float(row.get('单位净值', 0)),
                        daily_return=float(row.get('日增长率', 0)) if '日增长率' in row.index else None,
                        data_source=self.name,
                    ))
                except (ValueError, TypeError) as e:
                    logger.debug(f"跳过无效行: {e}")

            logger.info(f"[{self.name}] 获取 {fund_code} 历史净值 {len(results)} 条")
            return results

        except ImportError:
            logger.warning("akshare 未安装")
        except Exception as e:
            logger.warning(f"[{self.name}] 获取 {fund_code} 历史净值失败: {e}")

        return []
