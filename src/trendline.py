# -*- coding: utf-8 -*-
"""
===================================
趋势线自动识别模块
===================================

功能：
1. Zigzag 局部极值检测
2. 上涨/下跌趋势线拟合
3. 趋势线外推
4. 突破检测
5. 供 API 调用的公共接口
"""

from typing import List, Tuple, Dict, Optional


def find_extrema(prices: List[float], threshold_pct: float = 3.0) -> Tuple[List[int], List[int]]:
    """
    用 zigzag 算法找局部极大/极小值点的索引。

    threshold_pct: 最小波动百分比，过滤噪声
    返回: (local_min_indices, local_max_indices)
    """
    if len(prices) < 3:
        return [], []

    local_mins = []
    local_maxs = []

    # 初始化：找到第一个方向
    last_high_idx = 0
    last_low_idx = 0
    last_high = prices[0]
    last_low = prices[0]
    direction = 0  # 0=未定, 1=up, -1=down

    for i in range(1, len(prices)):
        p = prices[i]

        if direction == 0:
            # 尚未确定方向
            if p >= last_high:
                last_high = p
                last_high_idx = i
            if p <= last_low:
                last_low = p
                last_low_idx = i

            # 从低点涨了 threshold_pct% → 确认低点，方向向上
            if last_low > 0 and (p - last_low) / last_low * 100 >= threshold_pct:
                local_mins.append(last_low_idx)
                last_high = p
                last_high_idx = i
                direction = 1
            # 从高点跌了 threshold_pct% → 确认高点，方向向下
            elif last_high > 0 and (last_high - p) / last_high * 100 >= threshold_pct:
                local_maxs.append(last_high_idx)
                last_low = p
                last_low_idx = i
                direction = -1

        elif direction == 1:
            # 当前向上，追踪高点
            if p >= last_high:
                last_high = p
                last_high_idx = i
            # 从高点回落 threshold_pct% → 确认高点，转向下
            elif last_high > 0 and (last_high - p) / last_high * 100 >= threshold_pct:
                local_maxs.append(last_high_idx)
                last_low = p
                last_low_idx = i
                direction = -1

        elif direction == -1:
            # 当前向下，追踪低点
            if p <= last_low:
                last_low = p
                last_low_idx = i
            # 从低点反弹 threshold_pct% → 确认低点，转向上
            elif last_low > 0 and (p - last_low) / last_low * 100 >= threshold_pct:
                local_mins.append(last_low_idx)
                last_high = p
                last_high_idx = i
                direction = 1

    return local_mins, local_maxs


def _fit_uptrend(prices: List[float], local_mins: List[int], total_len: int) -> Optional[Dict]:
    """
    从局部低点中拟合上涨趋势线。
    从最近的低点对开始向前搜索，找到合格的递增低点对。
    """
    if len(local_mins) < 2:
        return None

    min_gap = max(int(total_len * 0.1), 3)
    tolerance = int(total_len * 0.05)

    # 从最近的低点往前搜索
    for i in range(len(local_mins) - 1, 0, -1):
        end_idx = local_mins[i]
        end_val = prices[end_idx]

        for j in range(i - 1, -1, -1):
            start_idx = local_mins[j]
            start_val = prices[start_idx]

            # 确保递增且间隔足够
            if end_val <= start_val:
                continue
            if end_idx - start_idx < min_gap:
                continue

            # 验证：线下方穿越点不超过容忍度
            slope = (end_val - start_val) / (end_idx - start_idx)
            violations = 0
            for k in range(start_idx, min(end_idx + 1, total_len)):
                line_val = start_val + slope * (k - start_idx)
                if prices[k] < line_val * 0.995:  # 允许微小偏差
                    violations += 1

            if violations <= tolerance:
                # 收集支撑点
                support_points = [local_mins[m] for m in range(j, i + 1)]
                return {
                    'start_idx': start_idx,
                    'end_idx': end_idx,
                    'start_val': start_val,
                    'end_val': end_val,
                    'slope': slope,
                    'support_points': support_points,
                }

    return None


def _fit_downtrend(prices: List[float], local_maxs: List[int], total_len: int) -> Optional[Dict]:
    """
    从局部高点中拟合下跌趋势线。
    从最近的高点对开始向前搜索，找到合格的递减高点对。
    """
    if len(local_maxs) < 2:
        return None

    min_gap = max(int(total_len * 0.1), 3)
    tolerance = int(total_len * 0.05)

    for i in range(len(local_maxs) - 1, 0, -1):
        end_idx = local_maxs[i]
        end_val = prices[end_idx]

        for j in range(i - 1, -1, -1):
            start_idx = local_maxs[j]
            start_val = prices[start_idx]

            # 确保递减且间隔足够
            if end_val >= start_val:
                continue
            if end_idx - start_idx < min_gap:
                continue

            # 验证：线上方穿越点不超过容忍度
            slope = (end_val - start_val) / (end_idx - start_idx)
            violations = 0
            for k in range(start_idx, min(end_idx + 1, total_len)):
                line_val = start_val + slope * (k - start_idx)
                if prices[k] > line_val * 1.005:
                    violations += 1

            if violations <= tolerance:
                support_points = [local_maxs[m] for m in range(j, i + 1)]
                return {
                    'start_idx': start_idx,
                    'end_idx': end_idx,
                    'start_val': start_val,
                    'end_val': end_val,
                    'slope': slope,
                    'support_points': support_points,
                }

    return None


def detect_trendlines(prices: List[float], dates: List[str],
                      threshold_pct: float = 3.0) -> Dict:
    """
    检测上涨和下跌趋势线。

    返回:
    {
        'uptrend': {...} | None,
        'downtrend': {...} | None
    }
    """
    if len(prices) < 10:
        return {'uptrend': None, 'downtrend': None}

    local_mins, local_maxs = find_extrema(prices, threshold_pct)

    uptrend = _fit_uptrend(prices, local_mins, len(prices))
    downtrend = _fit_downtrend(prices, local_maxs, len(prices))

    # 补充日期信息
    if uptrend:
        uptrend['start_date'] = dates[uptrend['start_idx']]
        uptrend['end_date'] = dates[uptrend['end_idx']]

    if downtrend:
        downtrend['start_date'] = dates[downtrend['start_idx']]
        downtrend['end_date'] = dates[downtrend['end_idx']]

    return {'uptrend': uptrend, 'downtrend': downtrend}


def extrapolate_line(start_idx: int, end_idx: int, start_val: float,
                     end_val: float, total_len: int) -> List[Optional[float]]:
    """将趋势线从 start_idx 外推到序列末尾，start 之前为 None"""
    result = [None] * total_len
    if start_idx == end_idx:
        return result

    slope = (end_val - start_val) / (end_idx - start_idx)
    for i in range(start_idx, total_len):
        result[i] = round(start_val + slope * (i - start_idx), 4)

    return result


def check_breakout(current_price: float, trendline_value: float,
                   line_type: str, margin_pct: float = 0.5) -> Optional[str]:
    """
    检测价格是否突破趋势线。

    line_type: 'uptrend' | 'downtrend'
    返回: 'break_below_uptrend' | 'break_above_downtrend' | None
    """
    if trendline_value is None or trendline_value <= 0:
        return None

    if line_type == 'uptrend':
        # 价格跌破上涨趋势线
        if current_price < trendline_value * (1 - margin_pct / 100):
            return 'break_below_uptrend'
    elif line_type == 'downtrend':
        # 价格突破下跌趋势线
        if current_price > trendline_value * (1 + margin_pct / 100):
            return 'break_above_downtrend'

    return None


def find_support_resistance(prices: List[float], dates: List[str],
                             threshold_pct: float = 3.0,
                             cluster_pct: float = 1.5,
                             min_touches: int = 2,
                             max_lines: int = 3) -> Dict:
    """
    检测水平支撑线和阻力线。

    算法：
    1. 用 zigzag 找出所有局部极值点
    2. 将价格相近（差距 < cluster_pct%）的极值点聚类
    3. 触碰次数 >= min_touches 的聚类作为支撑/阻力线
    4. 按触碰次数排序，最多返回 max_lines 条

    Args:
        prices: 价格序列
        dates: 日期序列（与 prices 等长）
        threshold_pct: zigzag 极值检测阈值
        cluster_pct: 聚类容差百分比（价格差在此范围内视为同一水平）
        min_touches: 最少触碰次数
        max_lines: 每种最多返回几条

    Returns:
        {'support': [...], 'resistance': [...]}
        每条: {'price': float, 'touches': int, 'touch_dates': [str, ...]}
    """
    if len(prices) < 10:
        return {'support': [], 'resistance': []}

    local_mins, local_maxs = find_extrema(prices, threshold_pct)

    def _cluster_levels(indices: List[int]) -> List[dict]:
        if not indices:
            return []
        # 按价格排序
        pts = sorted(indices, key=lambda i: prices[i])
        clusters = []
        current = [pts[0]]

        for i in range(1, len(pts)):
            # 与当前聚类的平均价比较
            avg_price = sum(prices[j] for j in current) / len(current)
            if avg_price > 0 and abs(prices[pts[i]] - avg_price) / avg_price * 100 <= cluster_pct:
                current.append(pts[i])
            else:
                clusters.append(current)
                current = [pts[i]]
        clusters.append(current)

        # 过滤触碰次数不够的，按触碰次数降序排序
        result = []
        for c in clusters:
            if len(c) >= min_touches:
                avg = round(sum(prices[i] for i in c) / len(c), 4)
                touch_dates = sorted(set(dates[i] for i in c))
                result.append({
                    'price': avg,
                    'touches': len(c),
                    'touch_dates': touch_dates,
                })
        result.sort(key=lambda x: x['touches'], reverse=True)
        return result[:max_lines]

    # 当前价格用于区分：低于当前价的极值聚类是支撑，高于的是阻力
    current_price = prices[-1]

    support_candidates = _cluster_levels(local_mins)
    resistance_candidates = _cluster_levels(local_maxs)

    # 支撑线应在当前价格下方，阻力线应在当前价格上方
    support = [s for s in support_candidates if s['price'] <= current_price * 1.01]
    resistance = [r for r in resistance_candidates if r['price'] >= current_price * 0.99]

    return {'support': support, 'resistance': resistance}


def compute_trendlines_for_api(nav_data: List[Dict],
                               threshold_pct: float = None) -> Dict:
    """
    供 API 路由调用。

    nav_data: [{'date': 'YYYY-MM-DD', 'nav': float}, ...]（已按日期升序）
    自动推断 threshold_pct（len<60 用 2%，否则 3%）

    返回:
    {
        'uptrend': {start_date, end_date, start_val, end_val,
                    extended_end_date, extended_end_val,
                    points: [{date, value}, ...]} | None,
        'downtrend': {...} | None
    }
    points: 趋势线上从 start_date 到数据末尾每个日期对应的值
    """
    if not nav_data or len(nav_data) < 10:
        return {'uptrend': None, 'downtrend': None}

    prices = [d['nav'] for d in nav_data]
    dates = [d['date'] for d in nav_data]

    if threshold_pct is None:
        threshold_pct = 2.0 if len(prices) < 60 else 3.0

    trendlines = detect_trendlines(prices, dates, threshold_pct)
    result = {}

    for key in ('uptrend', 'downtrend'):
        line = trendlines.get(key)
        if not line:
            result[key] = None
            continue

        # 外推到数据末尾
        extrapolated = extrapolate_line(
            line['start_idx'], line['end_idx'],
            line['start_val'], line['end_val'],
            len(prices)
        )

        points = []
        for i in range(line['start_idx'], len(prices)):
            if extrapolated[i] is not None:
                points.append({'date': dates[i], 'value': extrapolated[i]})

        extended_end_val = extrapolated[-1] if extrapolated[-1] is not None else line['end_val']

        result[key] = {
            'start_date': line['start_date'],
            'end_date': line['end_date'],
            'start_val': round(line['start_val'], 4),
            'end_val': round(line['end_val'], 4),
            'extended_end_date': dates[-1],
            'extended_end_val': round(extended_end_val, 4),
            'points': points,
        }

    # 支撑/阻力线
    sr = find_support_resistance(prices, dates, threshold_pct)
    result['support'] = sr['support']
    result['resistance'] = sr['resistance']

    return result
