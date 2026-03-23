# -*- coding: utf-8 -*-
"""
===================================
基金估值助手 - 调度器模块
===================================

职责：
1. 交易时段感知的调度
2. 支持两种盘中模式：
   - interval 模式：每 N 秒轮询（REFRESH_INTERVAL）
   - 定时模式：在指定时间点触发（SCHEDULE_TIMES）
3. 收盘后获取官方净值
4. 优雅退出
"""

import logging
import signal
import threading
import time
from datetime import datetime, time as dt_time
from typing import Callable, Optional, List

from . import trading_calendar as tc

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """优雅退出处理器"""

    def __init__(self):
        self.shutdown_requested = False
        self._lock = threading.Lock()

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        with self._lock:
            if not self.shutdown_requested:
                logger.info(f"收到退出信号 ({signum})，等待当前任务完成...")
                self.shutdown_requested = True

    @property
    def should_shutdown(self) -> bool:
        with self._lock:
            return self.shutdown_requested


class FundScheduler:
    """
    交易时段感知的调度器

    两种盘中调度模式：
    1. schedule_times 非空 → 定时模式：仅在指定时间点触发
    2. schedule_times 为空 → interval 模式：盘中每 refresh_interval 秒轮询

    收盘任务固定 15:30 / 15:35 执行
    """

    def __init__(self, refresh_interval: int = 60,
                 schedule_times: List[str] = None):
        self.refresh_interval = refresh_interval
        self.shutdown = GracefulShutdown()

        # 解析定时时间
        self._schedule_times: List[dt_time] = []
        if schedule_times:
            for t_str in schedule_times:
                try:
                    parts = t_str.strip().split(':')
                    self._schedule_times.append(dt_time(int(parts[0]), int(parts[1])))
                except (ValueError, IndexError):
                    logger.warning(f"忽略无效的定时时间: {t_str}")
            self._schedule_times.sort()

        self._use_schedule_mode = len(self._schedule_times) > 0

        # 任务回调
        self._intraday_task: Optional[Callable] = None
        self._eod_task: Optional[Callable] = None

        # 状态追踪
        self._eod_done_today = False
        self._fired_times_today: set = set()
        self._last_date = None

    def set_tasks(self, intraday: Callable = None,
                  eod: Callable = None) -> None:
        """设置任务回调"""
        self._intraday_task = intraday
        self._eod_task = eod

    def run(self) -> None:
        """运行调度器主循环"""
        if self._use_schedule_mode:
            times_str = ', '.join(t.strftime('%H:%M') for t in self._schedule_times)
            logger.info(f"调度器启动（定时模式: {times_str}）")
        else:
            logger.info(f"调度器启动（间隔模式: 每 {self.refresh_interval} 秒）")

        while not self.shutdown.should_shutdown:
            try:
                now = tc.now_beijing()
                today = now.date()

                # 日期切换，重置状态
                if self._last_date != today:
                    self._eod_done_today = False
                    self._fired_times_today = set()
                    self._last_date = today

                if not tc.is_trading_day(today):
                    wait = tc.seconds_until_market_open(now)
                    logger.info(f"非交易日，休眠 {wait / 3600:.1f} 小时到下个交易日开盘")
                    self._sleep(min(wait, 3600))
                    continue

                current_time = now.time()

                # 交易时段：执行盘中任务
                if tc.MARKET_OPEN <= current_time <= tc.MARKET_CLOSE:
                    if self._use_schedule_mode:
                        self._run_schedule_mode(current_time)
                    else:
                        self._run_interval_mode()

                # 收盘后：EOD 任务（15:30 起）
                elif current_time >= dt_time(15, 30) and not self._eod_done_today:
                    if self._eod_task:
                        self._safe_run("收盘净值", self._eod_task)
                    self._eod_done_today = True

                # 盘前等待
                elif tc.is_before_market_open(now):
                    wait = tc.seconds_until_market_open(now)
                    logger.info(f"盘前等待，距开盘 {wait / 60:.0f} 分钟")
                    self._sleep(min(wait, 300))

                # 盘后，所有任务完成
                elif self._eod_done_today:
                    wait = tc.seconds_until_market_open(now)
                    logger.info(f"今日任务已完成，休眠 {wait / 3600:.1f} 小时")
                    self._sleep(min(wait, 3600))
                else:
                    self._sleep(30)

            except Exception as e:
                logger.exception(f"调度器异常: {e}")
                self._sleep(60)

        logger.info("调度器已停止")

    def _run_interval_mode(self) -> None:
        """间隔模式"""
        if self._intraday_task:
            self._safe_run("盘中估值", self._intraday_task)
        self._sleep(self.refresh_interval)

    def _run_schedule_mode(self, current_time: dt_time) -> None:
        """定时模式"""
        fired = False
        for t in self._schedule_times:
            key = t.strftime('%H:%M')
            if key in self._fired_times_today:
                continue
            if current_time.hour == t.hour and current_time.minute == t.minute:
                if self._intraday_task:
                    self._safe_run(f"定时估值({key})", self._intraday_task)
                self._fired_times_today.add(key)
                fired = True

        if not fired:
            next_wait = self._seconds_to_next_schedule(current_time)
            if next_wait > 0:
                self._sleep(min(next_wait, 30))
            else:
                self._sleep(30)

    def _seconds_to_next_schedule(self, current_time: dt_time) -> float:
        """计算距离下一个未触发的定时时间还有多少秒"""
        now_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
        for t in self._schedule_times:
            key = t.strftime('%H:%M')
            if key in self._fired_times_today:
                continue
            target_seconds = t.hour * 3600 + t.minute * 60
            diff = target_seconds - now_seconds
            if diff > 0:
                return diff
        return -1

    def _safe_run(self, task_name: str, task: Callable) -> None:
        """安全执行任务"""
        try:
            logger.info(f"执行任务: {task_name}")
            task()
            logger.info(f"任务完成: {task_name}")
        except Exception as e:
            logger.exception(f"任务 [{task_name}] 执行失败: {e}")

    def _sleep(self, seconds: float) -> None:
        """可中断的休眠"""
        end_time = time.time() + seconds
        while time.time() < end_time and not self.shutdown.should_shutdown:
            time.sleep(min(1, end_time - time.time()))
