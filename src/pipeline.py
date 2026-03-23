# -*- coding: utf-8 -*-
"""
===================================
基金估值助手 - 业务编排模块
===================================

职责：
将 fetcher、storage、notification 模块串联，提供核心方法：
1. intraday_task(): 盘中估值抓取 → 存储 → 告警
2. eod_task(): 收盘后获取官方净值 → 存储 → 策略评估
"""

import logging
from datetime import datetime, date, timedelta, time as dtime
from typing import List, Optional

from .config import get_config, Config
from .storage import get_db, DatabaseManager, FundNavDaily
from .data_provider import FundFetcherManager, FundValuationResult
from .data_provider.index_fetcher import fetch_indices_realtime, fetch_index_daily
from .notification import NotificationService

logger = logging.getLogger(__name__)


class FundValuationPipeline:
    """基金估值业务编排器"""

    def __init__(self, config: Config = None):
        self.config = config or get_config()
        self.db = get_db()
        self.fetcher_manager = FundFetcherManager()
        self.notifier = NotificationService()

        logger.info(
            f"Pipeline 初始化完成，关注 {len(self.config.fund_list)} 只基金: "
            + ", ".join(self.config.fund_list)
        )

    def intraday_task(self) -> None:
        """
        盘中估值任务

        流程：
        1. 批量获取所有关注基金的估值
        2. 存储到数据库
        3. 更新基金信息
        4. 检查阈值告警 + 趋势线突破告警
        """
        logger.info("=" * 40)
        logger.info("开始盘中估值抓取")

        fund_codes = list(self.config.fund_list)
        logger.info(f"本轮抓取基金总数: {len(fund_codes)}")
        results = self.fetcher_manager.get_valuations_batch(fund_codes)

        if not results:
            logger.warning("未获取到任何估值数据")
            return

        # 存储估值数据
        valuations_to_save = []
        for r in results:
            valuations_to_save.append(r.to_dict())
            if r.fund_name:
                self.db.upsert_fund_info(
                    fund_code=r.fund_code,
                    fund_name=r.fund_name,
                    alias=self.config.get_fund_alias(r.fund_code),
                )

        self.db.save_valuations_batch(valuations_to_save)

        # 输出概要
        for r in results:
            alias = self.config.get_fund_alias(r.fund_code)
            display_name = alias if alias != r.fund_code else r.fund_name
            pct_str = f"{r.estimate_pct:+.2f}%"
            time_str = r.valuation_time.strftime('%H:%M') if r.valuation_time else 'N/A'
            logger.info(f"  {display_name}({r.fund_code}): {pct_str}  净值={r.estimate_nav:.4f}  [{time_str}]")

        # 检查阈值告警
        self._check_threshold_alerts(results)

        # 检查趋势线突破告警
        self._check_trendline_breakout_alerts(results)

        # 抓取大盘指数分时数据
        self._fetch_index_intraday()

        logger.info(f"盘中估值抓取完成，共 {len(results)} 只基金")

    def _check_threshold_alerts(self, results: List[FundValuationResult]) -> None:
        """检查涨跌阈值告警（每只基金每天每方向去重）"""
        strategies = self.db.get_all_enabled_strategies(strategy_type='threshold')
        if not strategies:
            return

        today = date.today()
        result_map = {r.fund_code: r for r in results}

        for strategy in strategies:
            try:
                params = strategy.get_params()
                rise_pct = float(params.get('rise_pct', 99))
                drop_pct = float(params.get('drop_pct', -99))

                r = result_map.get(strategy.fund_code)
                if not r or r.estimate_pct is None:
                    continue

                alert_type = None
                direction = None
                if r.estimate_pct >= rise_pct:
                    alert_type = f'threshold_rise_{strategy.fund_code}'
                    direction = '涨'
                elif r.estimate_pct <= drop_pct:
                    alert_type = f'threshold_drop_{strategy.fund_code}'
                    direction = '跌'

                if not alert_type:
                    continue

                alert = self.db.create_strategy_alert(
                    strategy_id=strategy.id,
                    alert_date=today,
                    alert_type=alert_type,
                    alert_detail={
                        'fund_code': strategy.fund_code,
                        'estimate_pct': r.estimate_pct,
                        'estimate_nav': r.estimate_nav,
                        'direction': direction,
                    },
                )
                if alert is None:
                    continue

                fund_info = self.db.get_fund_info(strategy.fund_code)
                fund_name = fund_info.fund_name if fund_info else strategy.fund_code

                logger.warning(
                    f"阈值告警: {fund_name}({strategy.fund_code}) {r.estimate_pct:+.2f}%"
                )

                # 发送通知
                content = (
                    f"## 涨跌阈值告警\n\n"
                    f"**{fund_name}**（{strategy.fund_code}）\n\n"
                    f"- 估算涨跌幅: **{r.estimate_pct:+.2f}%**\n"
                    f"- 估算净值: {r.estimate_nav:.4f}\n"
                    f"- 触发方向: {direction}\n"
                    f"- 阈值设置: 涨 ≥ {rise_pct}%, 跌 ≤ {drop_pct}%"
                )
                self.notifier.send(content, subject=f"阈值告警: {fund_name} {direction}幅{abs(r.estimate_pct):.2f}%")

            except Exception as e:
                logger.error(f"阈值告警检查失败 strategy_id={strategy.id}: {e}")

    def _check_trendline_breakout_alerts(self, results: List[FundValuationResult]) -> None:
        """盘中检测趋势线突破"""
        from .trendline import detect_trendlines, check_breakout

        strategies = self.db.get_all_enabled_strategies(strategy_type='trend_line')
        if not strategies:
            return

        today = date.today()
        result_map = {r.fund_code: r for r in results}

        for strategy in strategies:
            try:
                params = strategy.get_params()
                days = int(params.get('lookback_days', 90))

                r = result_map.get(strategy.fund_code)
                if not r or r.estimate_nav is None:
                    continue

                navs = self.db.get_nav_history(strategy.fund_code, days)
                if not navs or len(navs) < 20:
                    continue

                navs_asc = list(reversed(navs))
                prices = [n.nav for n in navs_asc]
                dates_list = [n.date.strftime('%Y-%m-%d') for n in navs_asc]

                trendlines = detect_trendlines(prices, dates_list)

                triggered_items = []
                for line_type in ['uptrend', 'downtrend']:
                    line = trendlines.get(line_type)
                    if not line:
                        continue

                    idx_span = line['end_idx'] - line['start_idx']
                    if idx_span == 0:
                        continue
                    slope_per_day = (line['end_val'] - line['start_val']) / idx_span
                    extrapolated_val = line['end_val'] + slope_per_day

                    breakout = check_breakout(r.estimate_nav, extrapolated_val, line_type)
                    if not breakout:
                        continue

                    alert_key = f'trendline_{breakout}'
                    is_new = self.db.log_intraday_alert(strategy.fund_code, today, alert_key)
                    if not is_new:
                        continue

                    fund_info = self.db.get_fund_info(strategy.fund_code)
                    fund_name = fund_info.fund_name if fund_info else strategy.fund_code
                    triggered_items.append({
                        'fund_code': strategy.fund_code,
                        'fund_name': fund_name,
                        'estimate_nav': r.estimate_nav,
                        'trendline_val': round(extrapolated_val, 4),
                        'breakout_type': breakout,
                    })

                if triggered_items:
                    lines = ["## 趋势线突破提醒\n"]
                    for item in triggered_items:
                        bt = '跌破上涨趋势线' if item['breakout_type'] == 'break_below_uptrend' else '突破下跌趋势线'
                        lines.append(
                            f"- **{item['fund_name']}**({item['fund_code']}): "
                            f"估算净值 {item['estimate_nav']:.4f}, 趋势线 {item['trendline_val']}, {bt}"
                        )
                    lines.append("\n*盘中估值可能存在偏差，趋势线突破仅供参考。*")
                    self.notifier.send('\n'.join(lines), subject=f"趋势线突破: {len(triggered_items)}只基金")

            except Exception as e:
                logger.error(f"趋势线突破检测异常 strategy_id={strategy.id}: {e}")

    def eod_task(self) -> None:
        """
        收盘任务

        流程：
        1. 获取各基金的官方净值
        2. 存储到 fund_nav_daily 表
        3. 抓取大盘指数日线
        4. 清理过期数据
        5. 补填缺失估值记录
        6. 执行策略评估
        """
        logger.info("=" * 40)
        logger.info("开始收盘净值获取")

        fund_codes = list(self.config.fund_list)
        total_new_nav = 0
        for fund_code in fund_codes:
            try:
                history = self.fetcher_manager.get_nav_history(fund_code, days=180)
                if not history:
                    logger.warning(f"未获取到 {fund_code} 的历史净值")
                    continue

                nav_list = []
                for item in history:
                    nav_list.append({
                        'fund_code': item.fund_code,
                        'date': item.nav_date,
                        'nav': item.nav,
                        'acc_nav': item.acc_nav,
                        'daily_return': item.daily_return,
                        'data_source': item.data_source,
                    })

                total_new_nav += self.db.save_nav_daily_batch(nav_list)

                # 对比估值准确度
                today = date.today()
                today_navs = [h for h in history if h.nav_date == today]
                if today_navs:
                    official_nav = today_navs[0].nav
                    valuations = self.db.get_today_valuations(fund_code, today)
                    if valuations:
                        last_v = valuations[-1]
                        diff = last_v.estimate_nav - official_nav
                        diff_pct = (diff / official_nav * 100) if official_nav else 0
                        alias = self.config.get_fund_alias(fund_code)
                        logger.info(
                            f"  {alias}: 官方={official_nav:.4f}, "
                            f"估算={last_v.estimate_nav:.4f}, 偏差={diff_pct:+.2f}%"
                        )

            except Exception as e:
                logger.error(f"获取 {fund_code} 收盘净值失败: {e}")

        logger.info(f"收盘净值获取完成，新增 {total_new_nav} 条记录")

        self._fetch_index_daily()
        self._cleanup_task()
        self._backfill_valuations_from_nav(fund_codes)

        if total_new_nav > 0:
            self.strategy_alert_task()
        else:
            logger.info("今日无新净值数据（可能是节假日），跳过策略评估")

    def _compute_ma(self, navs: List[FundNavDaily], period: int) -> Optional[float]:
        """计算 N 日均线"""
        if len(navs) < period:
            return None
        return sum(n.nav for n in navs[:period]) / period

    def strategy_alert_task(self) -> None:
        """策略评估任务（MA 策略）"""
        logger.info("开始策略评估")

        strategies = self.db.get_all_enabled_strategies(strategy_type='ma')
        if not strategies:
            logger.info("无启用的 MA 策略，跳过评估")
            return

        today = date.today()
        triggered = 0

        for strategy in strategies:
            try:
                params = strategy.get_params()
                ma_period = params.get('ma_period', 5)
                action = params.get('action', '加仓')

                navs = self.db.get_nav_history(strategy.fund_code, 30)
                if not navs or len(navs) < ma_period + 1:
                    continue

                latest_nav = navs[0].nav
                prev_nav = navs[1].nav
                ma_value = self._compute_ma(navs, ma_period)
                if ma_value is None:
                    continue

                if action == '加仓':
                    crossed = prev_nav >= ma_value and latest_nav < ma_value
                    alert_type = f'ma_cross_below_{ma_period}'
                    direction = '跌破'
                else:
                    crossed = prev_nav < ma_value and latest_nav >= ma_value
                    alert_type = f'ma_cross_above_{ma_period}'
                    direction = '突破'

                if not crossed:
                    continue

                diff_pct = round((latest_nav - ma_value) / ma_value * 100, 2)

                alert_detail = {
                    'nav': latest_nav,
                    'prev_nav': prev_nav,
                    'ma_value': round(ma_value, 4),
                    'ma_period': ma_period,
                    'action': action,
                    'direction': direction,
                    'diff_pct': diff_pct,
                }

                alert = self.db.create_strategy_alert(
                    strategy_id=strategy.id,
                    alert_date=today,
                    alert_type=alert_type,
                    alert_detail=alert_detail,
                )

                if alert is None:
                    continue

                triggered += 1
                fund_info = self.db.get_fund_info(strategy.fund_code)
                fund_name = fund_info.fund_name if fund_info else strategy.fund_code

                logger.warning(
                    f"策略告警: {fund_name}({strategy.fund_code}) "
                    f"净值{direction} MA{ma_period} "
                    f"净值={latest_nav:.4f} MA={ma_value:.4f} "
                    f"偏离={diff_pct:.2f}% 建议={action}"
                )

                # 发送通知
                content = (
                    f"## 均线策略告警\n\n"
                    f"**基金**: {fund_name}（{strategy.fund_code}）\n\n"
                    f"**当前净值**: {latest_nav:.4f}\n\n"
                    f"**MA{ma_period}**: {ma_value:.4f}\n\n"
                    f"**偏离**: {diff_pct:.2f}%\n\n"
                    f"**操作建议**: {action}\n\n"
                    f"---\n\n"
                    f"净值已{direction} MA{ma_period} 均线，请关注。"
                )
                self.notifier.send(content, subject=f"均线告警: {fund_name} {direction} MA{ma_period}")

            except Exception as e:
                logger.error(f"策略评估失败 strategy_id={strategy.id}: {e}")

        logger.info(f"策略评估完成，触发 {triggered} 条告警")

    def _fetch_index_intraday(self) -> None:
        """抓取大盘指数盘中分时数据"""
        try:
            indices = self.db.get_enabled_indices()
            if not indices:
                return
            secids = [idx.secid for idx in indices]

            realtime = fetch_indices_realtime(secids)
            if not realtime:
                return

            now = datetime.now()
            records = []
            for item in realtime:
                if item.get('current_value') is None:
                    continue
                records.append({
                    'index_code': item['index_code'],
                    'current_value': item['current_value'],
                    'change_pct': item.get('change_pct'),
                    'record_time': now,
                })
            if records:
                self.db.save_index_intraday(records)
                logger.debug(f"大盘指数分时数据已存库，共 {len(records)} 条")
        except Exception as e:
            logger.error(f"抓取大盘指数分时数据失败: {e}")

    def _fetch_index_daily(self) -> None:
        """抓取大盘指数日线数据"""
        try:
            indices = self.db.get_enabled_indices()
            if not indices:
                return

            for idx in indices:
                try:
                    daily_list = fetch_index_daily(idx.secid, days=180)
                    if not daily_list:
                        continue
                    records = [
                        {
                            'index_code': idx.index_code,
                            'trade_date': item['trade_date'],
                            'close_value': item['close_value'],
                            'change_pct': item.get('change_pct'),
                        }
                        for item in daily_list
                    ]
                    self.db.save_index_daily(records)
                    logger.info(f"指数 {idx.index_name}({idx.index_code}) 日线数据已更新，共 {len(records)} 条")
                except Exception as e:
                    logger.error(f"更新指数 {idx.index_code} 日线失败: {e}")
        except Exception as e:
            logger.error(f"抓取大盘指数日线数据失败: {e}")

    def _backfill_valuations_from_nav(self, fund_codes: List[str]) -> None:
        """用官方净值补填缺失的盘中估值记录"""
        cutoff = datetime.combine(date.today() - timedelta(days=2), datetime.min.time())
        backfilled = 0
        for fund_code in fund_codes:
            try:
                latest_vals = self.db.get_latest_valuations([fund_code])
                if latest_vals and latest_vals[0].valuation_time >= cutoff:
                    continue

                navs = self.db.get_nav_history(fund_code, days=2)
                if not navs:
                    continue

                latest_rec = navs[0]
                prev_rec = navs[1] if len(navs) >= 2 else None

                estimate_pct = latest_rec.daily_return
                if estimate_pct is None and prev_rec and prev_rec.nav:
                    estimate_pct = (latest_rec.nav - prev_rec.nav) / prev_rec.nav * 100

                val_time = datetime.combine(latest_rec.date, dtime(15, 0, 0))

                self.db.save_valuation(
                    fund_code=fund_code,
                    estimate_nav=latest_rec.nav,
                    estimate_pct=round(estimate_pct, 4) if estimate_pct is not None else 0.0,
                    prev_nav=prev_rec.nav if prev_rec else None,
                    prev_nav_date=prev_rec.date if prev_rec else None,
                    valuation_time=val_time,
                    data_source='official_nav',
                )
                backfilled += 1
            except Exception as e:
                logger.error(f"补填 {fund_code} 估值记录失败: {e}")

        if backfilled:
            logger.info(f"用官方净值补填了 {backfilled} 只基金的估值记录")

    def _cleanup_task(self) -> None:
        """清理过期数据"""
        try:
            logger.info("开始清理过期数据")
            self.db.cleanup_stale_data()
        except Exception as e:
            logger.error(f"数据清理失败: {e}")

    def run_once(self) -> None:
        """单次运行（抓取一次估值并输出到控制台）"""
        logger.info("单次估值抓取模式")

        fund_codes = list(self.config.fund_list)
        results = self.fetcher_manager.get_valuations_batch(fund_codes)

        if not results:
            logger.warning("未获取到任何估值数据")
            return

        # 存储
        self.db.save_valuations_batch([r.to_dict() for r in results])

        # 更新基金信息
        for r in results:
            if r.fund_name:
                self.db.upsert_fund_info(
                    fund_code=r.fund_code,
                    fund_name=r.fund_name,
                    alias=self.config.get_fund_alias(r.fund_code),
                )

        # 输出到控制台
        display_data = []
        for r in results:
            alias = self.config.get_fund_alias(r.fund_code)
            display_name = alias if alias != r.fund_code else (r.fund_name or r.fund_code)
            time_str = r.valuation_time.strftime('%H:%M') if r.valuation_time else 'N/A'
            pct_str = f"{r.estimate_pct:+.2f}%"
            display_data.append((display_name, r.fund_code, pct_str, r.estimate_nav, r.prev_nav, time_str))

        def display_width(s: str) -> int:
            w = 0
            for c in s:
                w += 2 if '\u4e00' <= c <= '\u9fff' else 1
            return w

        def pad_right(s: str, width: int) -> str:
            return s + ' ' * (width - display_width(s))

        name_width = max(display_width("基金名称"), *(display_width(d[0]) for d in display_data)) + 2
        total_width = name_width + 60

        print()
        print("=" * total_width)
        print(f"  基金估值助手  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * total_width)
        header_name = pad_right("基金名称", name_width)
        print(f"  {header_name}{'代码':>8}  {'估算涨跌':>10}  {'估算净值':>10}  {'前日净值':>10}  {'更新时间':>8}")
        print("-" * total_width)

        for name, code, pct, nav, prev, t in display_data:
            padded_name = pad_right(name, name_width)
            print(f"  {padded_name}{code:>8}  {pct:>10}  {nav:>10.4f}  {prev:>10.4f}  {t:>8}")

        print("=" * total_width)
        print()
