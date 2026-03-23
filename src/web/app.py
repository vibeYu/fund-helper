# -*- coding: utf-8 -*-
"""
===================================
基金估值助手 - Web 应用
===================================

Flask 应用工厂模式，提供轻量估值看板：
- / : 首页估值表格 + 大盘指数（自动刷新）
- /docs : 策略文档
- /api/valuations : 最新估值 JSON
- /api/fund/<code>/history : 单基金历史净值
- /api/fund/<code>/intraday : 基金盘中数据
- /api/fund/<code>/trendlines : 趋势线
- /api/market/* : 大盘指数、热力图、板块、资金流
- /api/strategies : 策略 CRUD
"""

import logging
import os
from datetime import datetime, date

import markdown
from flask import Flask, render_template, jsonify, request, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from ..config import get_config
from ..storage import get_db
from ..data_provider import FundFetcherManager

logger = logging.getLogger(__name__)


def create_app() -> Flask:
    """Flask 应用工厂"""
    app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static',
    )

    config = get_config()
    app.secret_key = os.urandom(32).hex()

    # === 速率限制 ===
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=["200 per minute"],
        storage_uri="memory://",
    )

    # === 全局错误处理 ===
    @app.errorhandler(429)
    def ratelimit_handler(e):
        return jsonify({'code': 1, 'msg': '请求过于频繁，请稍后再试'}), 429

    @app.errorhandler(404)
    def not_found(e):
        if request.path.startswith('/api/'):
            return jsonify({'code': 1, 'msg': '接口不存在'}), 404
        return render_template('base.html'), 404

    @app.errorhandler(500)
    def internal_error(e):
        logger.exception("Internal Server Error")
        if request.path.startswith('/api/'):
            return jsonify({'code': 1, 'msg': '服务器内部错误'}), 500
        return render_template('base.html'), 500

    # === 页面路由 ===

    @app.route('/')
    def index():
        """首页 - 估值看板"""
        db = get_db()
        valuations = db.get_latest_valuations(config.fund_list)
        confirmed = db.get_today_confirmed_navs(config.fund_list)

        fund_data = []
        for v in valuations:
            alias = config.get_fund_alias(v.fund_code)
            fund_info = db.get_fund_info(v.fund_code)
            fund_name = fund_info.fund_name if fund_info else v.fund_code

            nav_confirmed = confirmed.get(v.fund_code)
            if nav_confirmed and nav_confirmed.nav is not None:
                fund_data.append({
                    'fund_code': v.fund_code,
                    'fund_name': fund_name,
                    'alias': alias if alias != v.fund_code else fund_name,
                    'estimate_nav': nav_confirmed.nav,
                    'estimate_pct': nav_confirmed.daily_return,
                    'prev_nav': v.prev_nav,
                    'valuation_time': nav_confirmed.date.strftime('%Y-%m-%d') + ' 15:00',
                    'data_source': nav_confirmed.data_source or v.data_source,
                    'fund_type': fund_info.fund_type if fund_info else '',
                    'is_confirmed': True,
                })
            else:
                fund_data.append({
                    'fund_code': v.fund_code,
                    'fund_name': fund_name,
                    'alias': alias if alias != v.fund_code else fund_name,
                    'estimate_nav': v.estimate_nav,
                    'estimate_pct': v.estimate_pct,
                    'prev_nav': v.prev_nav,
                    'valuation_time': v.valuation_time.strftime('%Y-%m-%d %H:%M') if v.valuation_time else 'N/A',
                    'data_source': v.data_source,
                    'fund_type': fund_info.fund_type if fund_info else '',
                    'is_confirmed': False,
                })

        fund_data.sort(key=lambda x: x['estimate_pct'] or 0, reverse=True)

        return render_template(
            'index.html',
            fund_data=fund_data,
            refresh_interval=config.refresh_interval,
            update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

    # === 文档配置 ===
    DOCS_CATALOG = [
        ('ma-strategy', '均线策略'),
        ('grid-strategy', '网格策略'),
        ('martingale-strategy', '马丁格尔策略'),
        ('tipp-strategy', 'TIPP保本策略'),
        ('enhanced-grid-strategy', '增强型网格策略'),
        ('dca-strategy', '定投策略'),
        ('trendline-strategy', '趋势线策略'),
    ]
    DOCS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'docs')
    _md = markdown.Markdown(extensions=['extra', 'toc'])

    @app.route('/docs')
    @app.route('/docs/<slug>')
    def docs(slug=None):
        """策略文档页"""
        slug = slug or 'ma-strategy'
        valid_slugs = [s for s, _ in DOCS_CATALOG]
        if slug not in valid_slugs:
            abort(404)

        md_path = os.path.join(DOCS_DIR, f'{slug}.md')
        try:
            with open(md_path, 'r', encoding='utf-8') as f:
                md_text = f.read()
        except FileNotFoundError:
            abort(404)

        _md.reset()
        html_content = _md.convert(md_text)

        return render_template(
            'docs.html',
            slug=slug,
            catalog=DOCS_CATALOG,
            content=html_content,
        )

    # === 公开 API ===

    @app.route('/api/valuations')
    def api_valuations():
        """最新估值数据"""
        db = get_db()
        valuations = db.get_latest_valuations(config.fund_list)
        confirmed = db.get_today_confirmed_navs(config.fund_list)

        data = []
        for v in valuations:
            alias = config.get_fund_alias(v.fund_code)
            fund_info = db.get_fund_info(v.fund_code)
            fund_name = fund_info.fund_name if fund_info else ''

            nav_confirmed = confirmed.get(v.fund_code)
            if nav_confirmed and nav_confirmed.nav is not None:
                item = {
                    'fund_code': v.fund_code,
                    'estimate_nav': nav_confirmed.nav,
                    'estimate_pct': nav_confirmed.daily_return,
                    'prev_nav': v.prev_nav,
                    'prev_nav_date': v.prev_nav_date.isoformat() if v.prev_nav_date else None,
                    'valuation_time': nav_confirmed.date.strftime('%Y-%m-%d') + ' 15:00',
                    'data_source': nav_confirmed.data_source or v.data_source,
                    'is_confirmed': True,
                }
            else:
                item = v.to_dict()
                item['is_confirmed'] = False

            item['alias'] = alias if alias != v.fund_code else fund_name
            item['fund_type'] = fund_info.fund_type if fund_info else ''
            data.append(item)

        return jsonify({
            'code': 0,
            'data': data,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })

    @app.route('/api/fund/<code>/intraday')
    def api_fund_intraday(code: str):
        """当天分时估值数据"""
        db = get_db()
        valuations = db.get_today_valuations(code, date.today())

        minute_map = {}
        for v in valuations:
            if not v.valuation_time:
                continue
            t = v.valuation_time
            hm = t.strftime('%H:%M')
            hour = t.hour
            minute = t.minute
            if (hour < 9) or (hour > 15) or (hour == 15 and minute > 0):
                continue
            minute_map[hm] = {
                'time': hm,
                'estimate_nav': v.estimate_nav,
                'estimate_pct': v.estimate_pct,
            }

        data = sorted(minute_map.values(), key=lambda x: x['time'])
        return jsonify({'code': 0, 'data': data})

    @app.route('/api/fund/<code>/history')
    def api_fund_history(code: str):
        """单基金历史净值"""
        days = request.args.get('days', 30, type=int)
        days = min(max(days, 1), 180)

        db = get_db()
        history = db.get_nav_history(code, days=days)

        if len(history) < days:
            try:
                fetcher = FundFetcherManager()
                fetched = fetcher.get_nav_history(code, days=days)
                if fetched:
                    nav_list = [{
                        'fund_code': h.fund_code,
                        'date': h.nav_date,
                        'nav': h.nav,
                        'acc_nav': h.acc_nav,
                        'daily_return': h.daily_return,
                        'data_source': h.data_source,
                    } for h in fetched]
                    db.save_nav_daily_batch(nav_list)
                    history = db.get_nav_history(code, days=days)
            except Exception as e:
                logger.warning(f"拉取 {code} 历史净值失败: {e}")

        history_asc = list(reversed(history))
        navs = [r.nav for r in history_asc]

        def _calc_ma(values, window):
            result = [None] * len(values)
            for i in range(window - 1, len(values)):
                result[i] = round(sum(values[i - window + 1:i + 1]) / window, 4)
            return result

        ma5 = _calc_ma(navs, 5)
        ma15 = _calc_ma(navs, 15)
        ma30 = _calc_ma(navs, 30)

        data = []
        for i, record in enumerate(history_asc):
            d = record.to_dict()
            d['ma5'] = ma5[i]
            d['ma15'] = ma15[i]
            d['ma30'] = ma30[i]
            data.append(d)

        return jsonify({'code': 0, 'data': data})

    @app.route('/api/fund/<code>/trendlines')
    def api_fund_trendlines(code: str):
        """趋势线检测"""
        from ..trendline import compute_trendlines_for_api

        days = request.args.get('days', 90, type=int)
        days = min(max(days, 30), 180)

        db = get_db()
        history = db.get_nav_history(code, days=days)

        if not history:
            return jsonify({'code': 0, 'data': {'uptrend': None, 'downtrend': None}})

        navs_asc = list(reversed(history))
        nav_data = [{'date': n.date.strftime('%Y-%m-%d'), 'nav': n.nav} for n in navs_asc]

        result = compute_trendlines_for_api(nav_data)
        return jsonify({'code': 0, 'data': result})

    # === 大盘指数 API ===

    @app.route('/api/market/indices')
    def api_market_indices():
        """各指数最新行情"""
        db = get_db()
        indices = db.get_enabled_indices()
        result = []
        for idx in indices:
            intraday = db.get_latest_index_intraday(idx.index_code)
            if intraday and intraday.current_value is not None:
                current_value = intraday.current_value
                change_pct = intraday.change_pct
                record_time = intraday.record_time.strftime('%H:%M')
            else:
                daily = db.get_index_daily_history(idx.index_code, days=1)
                d = daily[0] if daily else None
                current_value = d.close_value if d else None
                change_pct = d.change_pct if d else None
                record_time = d.trade_date.isoformat() if d else None
            result.append({
                'index_code': idx.index_code,
                'index_name': idx.index_name,
                'current_value': current_value,
                'change_pct': change_pct,
                'record_time': record_time,
            })
        return jsonify({'code': 0, 'data': result})

    @app.route('/api/market/heatmap')
    def api_market_heatmap():
        """行业热力图"""
        from ..data_provider.index_fetcher import fetch_sector_heatmap
        data = fetch_sector_heatmap()
        return jsonify({'code': 0, 'data': data})

    @app.route('/api/market/sectors')
    def api_market_sectors():
        """行业涨跌排行"""
        from ..data_provider.index_fetcher import fetch_sector_realtime
        sectors = fetch_sector_realtime()
        return jsonify({'code': 0, 'data': sectors})

    @app.route('/api/market/fund-flow')
    def api_market_fund_flow():
        """行业资金流向"""
        from ..data_provider.index_fetcher import fetch_sector_fund_flow
        data = fetch_sector_fund_flow()
        return jsonify({'code': 0, 'data': data})

    @app.route('/api/market/hot-concepts')
    def api_market_hot_concepts():
        """热门概念词云"""
        from ..data_provider.index_fetcher import fetch_hot_concepts
        data = fetch_hot_concepts()
        return jsonify({'code': 0, 'data': data})

    @app.route('/api/market/index/<code>/intraday')
    def api_market_index_intraday(code: str):
        """指数今日分时"""
        db = get_db()
        records = db.get_today_index_intraday(code, date.today())

        minute_map = {}
        for r in records:
            if not r.record_time or r.current_value is None:
                continue
            t = r.record_time
            h, m = t.hour, t.minute
            if h < 9 or h > 15 or (h == 15 and m > 0):
                continue
            hm = t.strftime('%H:%M')
            minute_map[hm] = {
                'time': hm,
                'current_value': r.current_value,
                'change_pct': r.change_pct,
            }

        data = sorted(minute_map.values(), key=lambda x: x['time'])
        return jsonify({'code': 0, 'data': data})

    @app.route('/api/market/index/<code>/history')
    def api_market_index_history(code: str):
        """指数历史日线"""
        from ..data_provider.index_fetcher import fetch_index_daily as _fetch_daily

        days = request.args.get('days', 30, type=int)
        days = min(max(days, 1), 180)

        db = get_db()
        history = db.get_index_daily_history(code, days=days)

        if len(history) < days:
            try:
                indices = db.get_enabled_indices()
                idx = next((i for i in indices if i.index_code == code), None)
                if idx:
                    fetched = _fetch_daily(idx.secid, days=days)
                    if fetched:
                        db.save_index_daily([
                            {'index_code': code, **item} for item in fetched
                        ])
                        history = db.get_index_daily_history(code, days=days)
            except Exception as e:
                logger.warning(f"补拉指数 {code} 日线失败: {e}")

        data = [
            {
                'trade_date': r.trade_date.isoformat(),
                'close_value': r.close_value,
                'change_pct': r.change_pct,
            }
            for r in sorted(history, key=lambda x: x.trade_date)
        ]
        return jsonify({'code': 0, 'data': data})

    # === 策略 API ===

    @app.route('/api/strategies', methods=['GET'])
    def api_list_strategies():
        """获取所有策略"""
        db = get_db()
        strategies = db.get_strategies()
        return jsonify({
            'code': 0,
            'data': [s.to_dict() for s in strategies],
        })

    @app.route('/api/strategies', methods=['POST'])
    @limiter.limit("20 per minute")
    def api_create_strategy():
        """创建策略"""
        data = request.get_json()
        if not data:
            return jsonify({'code': 1, 'msg': '请求数据无效'}), 400

        fund_code = (data.get('fund_code') or '').strip()
        strategy_type = (data.get('strategy_type') or '').strip()
        params = data.get('params', {})

        if not fund_code:
            return jsonify({'code': 1, 'msg': '请填写基金代码'})
        if strategy_type not in ('ma', 'threshold', 'trend_line'):
            return jsonify({'code': 1, 'msg': '策略类型无效，可选: ma, threshold, trend_line'})

        db = get_db()
        try:
            strategy = db.create_strategy(
                fund_code=fund_code,
                strategy_type=strategy_type,
                params=params,
            )
            return jsonify({'code': 0, 'data': strategy.to_dict()})
        except Exception as e:
            logger.error(f"创建策略失败: {e}")
            return jsonify({'code': 1, 'msg': '创建策略失败'})

    @app.route('/api/strategies/<int:strategy_id>', methods=['PUT'])
    def api_update_strategy(strategy_id: int):
        """更新策略"""
        data = request.get_json()
        if not data:
            return jsonify({'code': 1, 'msg': '请求数据无效'}), 400

        db = get_db()
        success = db.update_strategy(
            strategy_id=strategy_id,
            params=data.get('params'),
            is_enabled=data.get('is_enabled'),
        )
        if success:
            return jsonify({'code': 0, 'msg': '更新成功'})
        return jsonify({'code': 1, 'msg': '策略不存在'})

    @app.route('/api/strategies/<int:strategy_id>', methods=['DELETE'])
    def api_delete_strategy(strategy_id: int):
        """删除策略"""
        db = get_db()
        if db.delete_strategy(strategy_id):
            return jsonify({'code': 0, 'msg': '删除成功'})
        return jsonify({'code': 1, 'msg': '策略不存在'})

    return app
