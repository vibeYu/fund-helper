# -*- coding: utf-8 -*-
"""
===================================
投资帮帮 - 交易日历模块
===================================

职责：
1. 判断是否交易日（排除周末+中国法定节假日）
2. 判断是否在交易时段（9:30-15:00）
3. 使用 zoneinfo 处理北京时区
"""

import logging
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# 北京时区
BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# 交易时段
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(15, 0)

# 2026年中国法定节假日（非交易日）
# 来源：国务院办公厅发布的放假安排
HOLIDAYS_2026 = {
    # 元旦
    date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3),
    # 春节（2月17日为除夕）
    date(2026, 2, 15), date(2026, 2, 16), date(2026, 2, 17),
    date(2026, 2, 18), date(2026, 2, 19), date(2026, 2, 20),
    date(2026, 2, 21),
    # 清明节
    date(2026, 4, 4), date(2026, 4, 5), date(2026, 4, 6),
    # 劳动节
    date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3),
    date(2026, 5, 4), date(2026, 5, 5),
    # 端午节
    date(2026, 5, 31), date(2026, 6, 1), date(2026, 6, 2),
    # 中秋节
    date(2026, 9, 25), date(2026, 9, 26), date(2026, 9, 27),
    # 国庆节
    date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3),
    date(2026, 10, 4), date(2026, 10, 5), date(2026, 10, 6),
    date(2026, 10, 7), date(2026, 10, 8),
}

# 交易日历缓存（通过 akshare 获取后缓存）
_trade_dates_cache: set = set()


def _load_trade_dates_from_akshare() -> None:
    """从 akshare 加载交易日历（增强，非必需）"""
    global _trade_dates_cache
    if _trade_dates_cache:
        return

    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        _trade_dates_cache = set(df['trade_date'].dt.date)
        logger.info(f"从 akshare 加载交易日历成功，共 {len(_trade_dates_cache)} 个交易日")
    except Exception as e:
        logger.debug(f"从 akshare 加载交易日历失败（将使用硬编码节假日）: {e}")


def now_beijing() -> datetime:
    """获取当前北京时间"""
    return datetime.now(BEIJING_TZ)


def today_beijing() -> date:
    """获取当前北京日期"""
    return now_beijing().date()


def is_trading_day(d: date = None) -> bool:
    """
    判断是否为交易日

    排除条件：
    1. 周末（周六、周日）
    2. 中国法定节假日
    3. 如果 akshare 交易日历可用，以其为准
    """
    if d is None:
        d = today_beijing()

    # 优先使用 akshare 交易日历
    _load_trade_dates_from_akshare()
    if _trade_dates_cache:
        return d in _trade_dates_cache

    # 回退到硬编码逻辑
    if d.weekday() >= 5:
        return False

    if d in HOLIDAYS_2026:
        return False

    return True


def is_trading_hours(dt: datetime = None) -> bool:
    """
    判断是否在交易时段（9:30-15:00）

    Args:
        dt: 日期时间（默认当前北京时间）
    """
    if dt is None:
        dt = now_beijing()

    if not is_trading_day(dt.date()):
        return False

    current_time = dt.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def is_before_market_open(dt: datetime = None) -> bool:
    """判断是否在开盘前"""
    if dt is None:
        dt = now_beijing()
    return dt.time() < MARKET_OPEN


def is_after_market_close(dt: datetime = None) -> bool:
    """判断是否在收盘后"""
    if dt is None:
        dt = now_beijing()
    return dt.time() > MARKET_CLOSE


def next_trading_day(d: date = None) -> date:
    """获取下一个交易日"""
    if d is None:
        d = today_beijing()

    next_day = d + timedelta(days=1)
    while not is_trading_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def seconds_until_market_open(dt: datetime = None) -> float:
    """计算距离下次开盘的秒数"""
    if dt is None:
        dt = now_beijing()

    target_date = dt.date()
    target_time = datetime.combine(target_date, MARKET_OPEN, tzinfo=BEIJING_TZ)

    if dt >= target_time or not is_trading_day(target_date):
        # 已过开盘时间或非交易日，计算下一个交易日
        target_date = next_trading_day(target_date)
        target_time = datetime.combine(target_date, MARKET_OPEN, tzinfo=BEIJING_TZ)

    return (target_time - dt).total_seconds()
