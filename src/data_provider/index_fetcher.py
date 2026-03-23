# -*- coding: utf-8 -*-
"""
===================================
大盘指数数据获取 — 天天基金 Push2 API
===================================

封装东方财富 push2 接口，提供：
- fetch_indices_realtime: 批量获取实时点位
- fetch_index_daily: 获取单个指数历史日线
"""

import logging
import time
from datetime import date
from typing import List, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)

_session: requests.Session = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            'User-Agent': _UA,
            'Referer': 'https://finance.eastmoney.com/',
        })
        retry = Retry(total=2, backoff_factor=0.5,
                      status_forcelist=[500, 502, 503, 504])
        _session.mount('http://', HTTPAdapter(max_retries=retry))
        _session.mount('https://', HTTPAdapter(max_retries=retry))
    return _session


def fetch_indices_realtime(secids: List[str]) -> List[Dict[str, Any]]:
    """
    批量获取大盘指数实时行情

    :param secids: secid 列表，如 ['1.000001', '0.399001']
    :return: list of dicts: {index_code, current_value, change_pct}
             index_code 与 secid 中的代码部分对应（不含市场前缀）
    """
    if not secids:
        return []

    url = 'http://push2.eastmoney.com/api/qt/ulist.np/get'
    params = {
        'secids': ','.join(secids),
        'fields': 'f2,f3,f12,f14',
        'fltt': 2,
        'invt': 2,
    }

    try:
        resp = _get_session().get(url, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        diff = (payload.get('data') or {}).get('diff') or []

        result = []
        for item in diff:
            code = item.get('f12')
            raw_value = item.get('f2')
            raw_pct = item.get('f3')

            # 非交易时段时部分字段为 '-'
            if code is None:
                continue
            try:
                current_value = float(raw_value) if raw_value not in (None, '-') else None
                change_pct = float(raw_pct) if raw_pct not in (None, '-') else None
            except (TypeError, ValueError):
                current_value = None
                change_pct = None

            result.append({
                'index_code': str(code),
                'current_value': current_value,
                'change_pct': change_pct,
            })
        return result
    except Exception as e:
        logger.error(f"获取大盘指数实时数据失败: {e}")
        return []


def _parse_sector_items(diff_raw) -> List[Dict[str, Any]]:
    """解析行业板块 diff 数据"""
    diff = diff_raw.values() if isinstance(diff_raw, dict) else diff_raw
    result = []
    for item in diff:
        code = item.get('f12')
        name = item.get('f14')
        if not code or not name:
            continue
        try:
            change_pct = float(item['f3']) if item.get('f3') not in (None, '-') else None
            current_value = float(item['f2']) if item.get('f2') not in (None, '-') else None
            up_count = int(item['f104']) if item.get('f104') not in (None, '-') else 0
            down_count = int(item['f105']) if item.get('f105') not in (None, '-') else 0
        except (TypeError, ValueError):
            continue
        result.append({
            'sector_code': str(code),
            'sector_name': str(name),
            'change_pct': change_pct,
            'current_value': current_value,
            'up_count': up_count,
            'down_count': down_count,
        })
    return result


def fetch_sector_heatmap(top_n: int = 300) -> List[Dict[str, Any]]:
    """
    获取个股热力图数据（按市值前 N，按行业分组）

    返回: [{name: '银行', children: [{name: '工商银行', value: 市值, change_pct: 1.14}, ...]}, ...]
    """
    url = 'http://push2.eastmoney.com/api/qt/clist/get'
    params = {
        'fs': 'm:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2',
        'fields': 'f3,f12,f14,f20,f100',
        'fid': 'f20',
        'po': 1,
        'pn': 1,
        'pz': top_n,
        'fltt': 2,
        'invt': 2,
    }

    try:
        resp = _get_session().get(url, params=params, timeout=15)
        resp.raise_for_status()
        diff_raw = (resp.json().get('data') or {}).get('diff') or []
        diff = diff_raw.values() if isinstance(diff_raw, dict) else diff_raw

        # 按行业分组
        groups = {}
        for item in diff:
            name = item.get('f14')
            industry = item.get('f100')
            if not name or not industry or industry == '-':
                continue
            try:
                change_pct = float(item['f3']) if item.get('f3') not in (None, '-') else 0
                market_cap = float(item['f20']) if item.get('f20') not in (None, '-') else 0
            except (TypeError, ValueError):
                continue
            if market_cap <= 0:
                continue

            industry = str(industry)
            if industry not in groups:
                groups[industry] = []
            groups[industry].append({
                'name': str(name),
                'value': market_cap,
                'change_pct': change_pct,
            })

        # 转换为嵌套结构，行业按总市值降序
        result = []
        for ind_name, stocks in groups.items():
            total_cap = sum(s['value'] for s in stocks)
            result.append({
                'name': ind_name,
                'total_cap': total_cap,
                'children': stocks,
            })
        result.sort(key=lambda x: x['total_cap'], reverse=True)
        # 移除 total_cap
        for r in result:
            del r['total_cap']

        return result
    except Exception as e:
        logger.error(f"获取热力图数据失败: {e}")
        return []


def fetch_sector_realtime(top_n: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    """
    获取行业板块涨跌幅排行（涨幅前 N + 跌幅前 N）

    数据源：东方财富 push2 API，细分行业板块（m:90+t:2）
    返回: {'top': [...], 'bottom': [...]}
    """
    url = 'http://push2.eastmoney.com/api/qt/clist/get'
    base_params = {
        'fs': 'm:90+t:2',
        'fields': 'f2,f3,f4,f12,f14,f104,f105',
        'fid': 'f3',
        'pn': 1,
        'pz': top_n,
        'fltt': 2,
        'invt': 2,
    }

    try:
        # 涨幅前 N（降序）
        params_top = {**base_params, 'po': 1}
        resp_top = _get_session().get(url, params=params_top, timeout=10)
        resp_top.raise_for_status()
        diff_top = (resp_top.json().get('data') or {}).get('diff') or []
        top_list = _parse_sector_items(diff_top)

        # 跌幅前 N（升序）
        params_bottom = {**base_params, 'po': 0}
        resp_bottom = _get_session().get(url, params=params_bottom, timeout=10)
        resp_bottom.raise_for_status()
        diff_bottom = (resp_bottom.json().get('data') or {}).get('diff') or []
        bottom_list = _parse_sector_items(diff_bottom)

        return {'top': top_list, 'bottom': bottom_list}
    except Exception as e:
        logger.error(f"获取行业板块实时数据失败: {e}")
        return {'top': [], 'bottom': []}


def fetch_sector_fund_flow(top_n: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    获取行业板块主力资金流向排行（流入前 N + 流出前 N）

    数据源：东方财富 push2 API，细分行业板块（m:90+t:2）
    返回: {'inflow': [...], 'outflow': [...]}
    每项: {sector_name, net_inflow, change_pct}
    """
    url = 'http://push2.eastmoney.com/api/qt/clist/get'
    base_params = {
        'fs': 'm:90+t:2',
        'fields': 'f3,f12,f14,f62,f184',
        'fid': 'f62',
        'pn': 1,
        'pz': top_n,
        'fltt': 2,
        'invt': 2,
    }

    def _parse_flow(diff_raw):
        diff = diff_raw.values() if isinstance(diff_raw, dict) else diff_raw
        result = []
        for item in diff:
            name = item.get('f14')
            if not name:
                continue
            try:
                net_inflow = float(item['f62']) if item.get('f62') not in (None, '-') else None
                change_pct = float(item['f3']) if item.get('f3') not in (None, '-') else None
            except (TypeError, ValueError):
                continue
            if net_inflow is None:
                continue
            result.append({
                'sector_name': str(name),
                'net_inflow': net_inflow,
                'change_pct': change_pct,
            })
        return result

    try:
        # 净流入前 N（降序）
        params_in = {**base_params, 'po': 1}
        resp_in = _get_session().get(url, params=params_in, timeout=10)
        resp_in.raise_for_status()
        diff_in = (resp_in.json().get('data') or {}).get('diff') or []
        inflow_list = _parse_flow(diff_in)

        # 净流出前 N（升序）
        params_out = {**base_params, 'po': 0}
        resp_out = _get_session().get(url, params=params_out, timeout=10)
        resp_out.raise_for_status()
        diff_out = (resp_out.json().get('data') or {}).get('diff') or []
        outflow_list = _parse_flow(diff_out)

        return {'inflow': inflow_list, 'outflow': outflow_list}
    except Exception as e:
        logger.error(f"获取行业资金流向数据失败: {e}")
        return {'inflow': [], 'outflow': []}


def fetch_hot_concepts() -> List[Dict[str, Any]]:
    """
    获取热门概念词（基于同花顺热股的概念标签词频统计）

    返回: [{word, count}, ...] 按 count 降序
    """
    url = 'https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock'
    params = {
        'stock_type': 'a',
        'type': 'hour',
        'list_type': 'normal',
    }

    try:
        resp = _get_session().get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        stocks = (data.get('data') or {}).get('stock_list') or []

        from collections import Counter
        tag_counter = Counter()
        for st in stocks:
            tags = st.get('tag') or {}
            for concept in (tags.get('concept_tag') or []):
                tag_counter[concept] += 1

        return [{'word': w, 'count': c} for w, c in tag_counter.most_common(50)]
    except Exception as e:
        logger.error(f"获取热门概念词失败: {e}")
        return []


def fetch_index_daily(secid: str, days: int = 180) -> List[Dict[str, Any]]:
    """
    获取单个指数历史日线数据

    :param secid: 如 '1.000001'
    :param days: 获取最近 N 个交易日
    :return: list of dicts: {trade_date, close_value, change_pct}
    """
    url = 'http://push2his.eastmoney.com/api/qt/stock/kline/get'
    params = {
        'secid': secid,
        'klt': 101,    # 日K
        'fqt': 0,      # 不复权
        'beg': 0,      # 不限起始日期
        'end': 20500101,
        'lmt': days,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
    }

    try:
        resp = _get_session().get(url, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        klines = (payload.get('data') or {}).get('klines') or []
        logger.debug(f"fetch_index_daily secid={secid} 返回 {len(klines)} 条 klines")

        result = []
        for line in klines:
            parts = line.split(',')
            # f51=日期, f52=开盘, f53=收盘, ..., f59=涨跌幅%
            if len(parts) < 9:
                continue
            try:
                trade_date = date.fromisoformat(parts[0])
                close_value = float(parts[2])
                change_pct = float(parts[8]) if parts[8] not in ('', '-') else None
                result.append({
                    'trade_date': trade_date,
                    'close_value': close_value,
                    'change_pct': change_pct,
                })
            except (ValueError, IndexError):
                continue

        return result
    except Exception as e:
        logger.error(f"获取指数日线数据失败 secid={secid}: {e}")
        return []
