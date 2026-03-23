# -*- coding: utf-8 -*-
"""
===================================
基金估值助手 - 存储层 (SQLite)
===================================

职责：
1. 管理 SQLite 数据库连接（单例模式）
2. 定义 ORM 数据模型
3. 提供数据存取接口
"""

import logging
import math
import json
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Date,
    DateTime,
    Integer,
    Boolean,
    Text,
    Index,
    UniqueConstraint,
    select,
    and_,
    desc,
    func,
    event,
)
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)

from .config import get_config

logger = logging.getLogger(__name__)

Base = declarative_base()


# === 数据模型定义 ===

class FundInfo(Base):
    """基金基本信息"""
    __tablename__ = 'fund_info'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_code = Column(String(10), unique=True, nullable=False, index=True)
    fund_name = Column(String(100))
    fund_type = Column(String(50))
    alias = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<FundInfo(code={self.fund_code}, name={self.fund_name})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'fund_code': self.fund_code,
            'fund_name': self.fund_name,
            'fund_type': self.fund_type,
            'alias': self.alias,
        }


class FundValuation(Base):
    """盘中估值记录"""
    __tablename__ = 'fund_valuation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_code = Column(String(10), nullable=False, index=True)
    estimate_nav = Column(Float)
    estimate_pct = Column(Float)
    prev_nav = Column(Float)
    prev_nav_date = Column(Date)
    valuation_time = Column(DateTime, nullable=False, index=True)
    data_source = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index('ix_valuation_code_time', 'fund_code', 'valuation_time'),
    )

    def __repr__(self):
        return f"<FundValuation(code={self.fund_code}, nav={self.estimate_nav}, pct={self.estimate_pct}%)>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'fund_code': self.fund_code,
            'estimate_nav': self.estimate_nav,
            'estimate_pct': self.estimate_pct,
            'prev_nav': self.prev_nav,
            'prev_nav_date': self.prev_nav_date.isoformat() if self.prev_nav_date else None,
            'valuation_time': self.valuation_time.strftime('%Y-%m-%d %H:%M') if self.valuation_time else None,
            'data_source': self.data_source,
        }


class FundNavDaily(Base):
    """每日官方净值"""
    __tablename__ = 'fund_nav_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_code = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    nav = Column(Float)
    acc_nav = Column(Float)
    daily_return = Column(Float)
    data_source = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('fund_code', 'date', name='uix_nav_code_date'),
        Index('ix_nav_code_date', 'fund_code', 'date'),
    )

    def __repr__(self):
        return f"<FundNavDaily(code={self.fund_code}, date={self.date}, nav={self.nav})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'fund_code': self.fund_code,
            'date': self.date.isoformat() if self.date else None,
            'nav': self.nav,
            'acc_nav': self.acc_nav,
            'daily_return': self.daily_return,
            'data_source': self.data_source,
        }


class FundStrategy(Base):
    """策略配置"""
    __tablename__ = 'fund_strategies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_code = Column(String(10), nullable=False)
    strategy_type = Column(String(30), nullable=False)
    params = Column(Text, nullable=False)  # JSON string
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<FundStrategy(fund_code={self.fund_code}, type={self.strategy_type})>"

    def get_params(self) -> Dict[str, Any]:
        if isinstance(self.params, dict):
            return self.params
        return json.loads(self.params) if self.params else {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'fund_code': self.fund_code,
            'strategy_type': self.strategy_type,
            'params': self.get_params(),
            'is_enabled': self.is_enabled,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None,
        }


class StrategyAlert(Base):
    """策略触发记录"""
    __tablename__ = 'strategy_alerts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(Integer, nullable=False, index=True)
    alert_date = Column(Date, nullable=False)
    alert_type = Column(String(30), nullable=False)
    alert_detail = Column(Text)  # JSON string
    notified = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('strategy_id', 'alert_date', 'alert_type', name='uix_strategy_alert_date'),
    )

    def __repr__(self):
        return f"<StrategyAlert(strategy_id={self.strategy_id}, date={self.alert_date}, type={self.alert_type})>"

    def get_detail(self) -> Dict[str, Any]:
        if isinstance(self.alert_detail, dict):
            return self.alert_detail
        return json.loads(self.alert_detail) if self.alert_detail else {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'strategy_id': self.strategy_id,
            'alert_date': self.alert_date.isoformat() if self.alert_date else None,
            'alert_type': self.alert_type,
            'alert_detail': self.get_detail(),
            'notified': self.notified,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
        }


class IntradayAlertLog(Base):
    """盘中告警去重日志"""
    __tablename__ = 'intraday_alert_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_code = Column(String(10), nullable=False)
    alert_date = Column(Date, nullable=False, index=True)
    alert_key = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('fund_code', 'alert_date', 'alert_key', name='uix_fund_date_key'),
    )


class MarketIndex(Base):
    """大盘指数配置表"""
    __tablename__ = 'market_index'

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(String(10), unique=True, nullable=False, index=True)
    index_name = Column(String(50), nullable=False)
    secid = Column(String(20), nullable=False)
    sort_order = Column(Integer, default=0)
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<MarketIndex(code={self.index_code}, name={self.index_name})>"


class IndexIntraday(Base):
    """大盘指数盘中分时记录"""
    __tablename__ = 'index_intraday'

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(String(10), nullable=False, index=True)
    current_value = Column(Float)
    change_pct = Column(Float)
    record_time = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index('ix_index_intraday_code_time', 'index_code', 'record_time'),
    )

    def __repr__(self):
        return f"<IndexIntraday(code={self.index_code}, value={self.current_value}, time={self.record_time})>"


class IndexDaily(Base):
    """大盘指数历史日线"""
    __tablename__ = 'index_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    close_value = Column(Float)
    change_pct = Column(Float)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('index_code', 'trade_date', name='uix_index_daily_code_date'),
        Index('ix_index_daily_code_date', 'index_code', 'trade_date'),
    )

    def __repr__(self):
        return f"<IndexDaily(code={self.index_code}, date={self.trade_date}, close={self.close_value})>"


# === 数据库管理器 ===

class DatabaseManager:
    """数据库管理器 - 单例模式 (SQLite)"""

    _instance: Optional['DatabaseManager'] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_url: Optional[str] = None):
        if self._initialized:
            return

        if db_url is None:
            config = get_config()
            db_url = config.get_db_url()

        self._engine = create_engine(db_url, echo=False)

        # SQLite: 启用 WAL 模式和外键支持
        @event.listens_for(self._engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
        )

        Base.metadata.create_all(self._engine)
        self._initialized = True
        logger.info(f"数据库初始化完成: {db_url}")
        self.seed_default_indices()

    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        if cls._instance is None or not cls._instance._initialized:
            cls._instance = None
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        if cls._instance is not None:
            cls._instance._engine.dispose()
            cls._instance = None

    def get_session(self) -> Session:
        return self._SessionLocal()

    # === 基金信息操作 ===

    def upsert_fund_info(self, fund_code: str, fund_name: str = None,
                         fund_type: str = None, alias: str = None) -> None:
        """新增或更新基金信息"""
        session = self.get_session()
        try:
            existing = session.execute(
                select(FundInfo).where(FundInfo.fund_code == fund_code)
            ).scalar_one_or_none()

            if existing:
                if fund_name:
                    existing.fund_name = fund_name
                if fund_type:
                    existing.fund_type = fund_type
                if alias:
                    existing.alias = alias
                existing.updated_at = datetime.now()
            else:
                record = FundInfo(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    fund_type=fund_type,
                    alias=alias,
                )
                session.add(record)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"更新基金信息失败 {fund_code}: {e}")
            raise
        finally:
            session.close()

    def get_fund_info(self, fund_code: str) -> Optional[FundInfo]:
        """获取基金信息"""
        session = self.get_session()
        try:
            return session.execute(
                select(FundInfo).where(FundInfo.fund_code == fund_code)
            ).scalar_one_or_none()
        finally:
            session.close()

    # === 估值数据操作 ===

    @staticmethod
    def _sanitize_float(val):
        """将 NaN / Inf 转为 None"""
        if val is None:
            return None
        try:
            if math.isnan(val) or math.isinf(val):
                return None
        except (TypeError, ValueError):
            pass
        return val

    def save_valuation(self, fund_code: str, estimate_nav: float, estimate_pct: float,
                       prev_nav: float = None, prev_nav_date: date = None,
                       valuation_time: datetime = None, data_source: str = None) -> None:
        """保存一条估值记录"""
        estimate_nav = self._sanitize_float(estimate_nav)
        estimate_pct = self._sanitize_float(estimate_pct)
        prev_nav = self._sanitize_float(prev_nav)

        if estimate_nav is None and estimate_pct is None:
            logger.debug(f"跳过无效估值记录: {fund_code}")
            return

        session = self.get_session()
        try:
            record = FundValuation(
                fund_code=fund_code,
                estimate_nav=estimate_nav,
                estimate_pct=estimate_pct,
                prev_nav=prev_nav,
                prev_nav_date=prev_nav_date,
                valuation_time=valuation_time or datetime.now(),
                data_source=data_source,
            )
            session.add(record)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"保存估值数据失败 {fund_code}: {e}")
            raise
        finally:
            session.close()

    def save_valuations_batch(self, valuations: List[Dict[str, Any]]) -> int:
        """批量保存估值记录"""
        session = self.get_session()
        saved = 0
        skipped = 0
        try:
            for val in valuations:
                estimate_nav = self._sanitize_float(val.get('estimate_nav'))
                estimate_pct = self._sanitize_float(val.get('estimate_pct'))
                prev_nav = self._sanitize_float(val.get('prev_nav'))

                vt = val.get('valuation_time')
                if vt is None:
                    vt = datetime.now()

                if estimate_nav is None and estimate_pct is None:
                    skipped += 1
                    continue

                record = FundValuation(
                    fund_code=val['fund_code'],
                    estimate_nav=estimate_nav,
                    estimate_pct=estimate_pct,
                    prev_nav=prev_nav,
                    prev_nav_date=val.get('prev_nav_date'),
                    valuation_time=vt,
                    data_source=val.get('data_source'),
                )
                session.add(record)
                saved += 1

            session.commit()
            msg = f"批量保存估值数据成功，共 {saved} 条"
            if skipped:
                msg += f"，跳过 {skipped} 条无效记录"
            logger.info(msg)
        except Exception as e:
            session.rollback()
            logger.error(f"批量保存估值数据失败: {e}")
            raise
        finally:
            session.close()
        return saved

    def get_latest_valuations(self, fund_codes: List[str] = None) -> List[FundValuation]:
        """获取各基金最新一条估值记录"""
        session = self.get_session()
        try:
            if fund_codes is None:
                config = get_config()
                fund_codes = config.fund_list

            results = []
            for code in fund_codes:
                record = session.execute(
                    select(FundValuation)
                    .where(FundValuation.fund_code == code)
                    .order_by(desc(FundValuation.valuation_time))
                    .limit(1)
                ).scalar_one_or_none()
                if record:
                    results.append(record)

            return results
        finally:
            session.close()

    def get_today_valuations(self, fund_code: str, target_date: date = None) -> List[FundValuation]:
        """获取某只基金今日所有估值记录"""
        session = self.get_session()
        try:
            if target_date is None:
                target_date = date.today()

            start = datetime.combine(target_date, datetime.min.time())
            end = datetime.combine(target_date, datetime.max.time())

            records = session.execute(
                select(FundValuation)
                .where(and_(
                    FundValuation.fund_code == fund_code,
                    FundValuation.valuation_time >= start,
                    FundValuation.valuation_time <= end,
                ))
                .order_by(FundValuation.valuation_time)
            ).scalars().all()

            return list(records)
        finally:
            session.close()

    # === 官方净值操作 ===

    def save_nav_daily(self, fund_code: str, nav_date: date, nav: float,
                       acc_nav: float = None, daily_return: float = None,
                       data_source: str = None) -> None:
        """保存每日官方净值（UPSERT）"""
        session = self.get_session()
        try:
            existing = session.execute(
                select(FundNavDaily).where(and_(
                    FundNavDaily.fund_code == fund_code,
                    FundNavDaily.date == nav_date,
                ))
            ).scalar_one_or_none()

            if existing:
                existing.nav = nav
                if acc_nav is not None:
                    existing.acc_nav = acc_nav
                if daily_return is not None:
                    existing.daily_return = daily_return
                if data_source:
                    existing.data_source = data_source
            else:
                record = FundNavDaily(
                    fund_code=fund_code,
                    date=nav_date,
                    nav=nav,
                    acc_nav=acc_nav,
                    daily_return=daily_return,
                    data_source=data_source,
                )
                session.add(record)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"保存净值数据失败 {fund_code}: {e}")
            raise
        finally:
            session.close()

    def save_nav_daily_batch(self, nav_list: List[Dict[str, Any]]) -> int:
        """批量保存净值数据（upsert: 存在则更新，不存在则插入）"""
        if not nav_list:
            return 0

        session = self.get_session()
        total = 0
        try:
            for d in nav_list:
                existing = session.execute(
                    select(FundNavDaily).where(and_(
                        FundNavDaily.fund_code == d['fund_code'],
                        FundNavDaily.date == d['date'],
                    ))
                ).scalar_one_or_none()

                if existing:
                    if d.get('nav') is not None:
                        existing.nav = d['nav']
                    if d.get('acc_nav') is not None:
                        existing.acc_nav = d['acc_nav']
                    if d.get('daily_return') is not None:
                        existing.daily_return = d['daily_return']
                    if d.get('data_source'):
                        existing.data_source = d['data_source']
                else:
                    session.add(FundNavDaily(
                        fund_code=d['fund_code'],
                        date=d['date'],
                        nav=d.get('nav'),
                        acc_nav=d.get('acc_nav'),
                        daily_return=d.get('daily_return'),
                        data_source=d.get('data_source'),
                    ))
                total += 1

                # 每 500 条提交一次
                if total % 500 == 0:
                    session.commit()

            session.commit()
            logger.info(f"批量保存净值数据成功，共 {total} 条")
        except Exception as e:
            session.rollback()
            logger.error(f"批量保存净值数据失败: {e}")
            raise
        finally:
            session.close()
        return total

    def get_latest_daily_returns(self, fund_codes: List[str]) -> Dict[str, dict]:
        """批量获取多只基金最近一个交易日的日涨跌幅"""
        if not fund_codes:
            return {}
        session = self.get_session()
        try:
            subq = (
                select(FundNavDaily.fund_code, func.max(FundNavDaily.date).label('max_date'))
                .where(FundNavDaily.fund_code.in_(fund_codes))
                .group_by(FundNavDaily.fund_code)
                .subquery()
            )
            records = session.execute(
                select(FundNavDaily.fund_code, FundNavDaily.daily_return, FundNavDaily.date)
                .join(subq, (FundNavDaily.fund_code == subq.c.fund_code) &
                             (FundNavDaily.date == subq.c.max_date))
            ).all()
            return {
                r.fund_code: {
                    'daily_return': r.daily_return,
                    'date': r.date,
                }
                for r in records
            }
        except Exception as e:
            logger.error(f"批量查询日涨跌幅失败: {e}")
            return {}
        finally:
            session.close()

    def get_nav_history(self, fund_code: str, days: int = 30) -> List[FundNavDaily]:
        """获取历史净值（降序）"""
        session = self.get_session()
        try:
            records = session.execute(
                select(FundNavDaily)
                .where(FundNavDaily.fund_code == fund_code)
                .order_by(desc(FundNavDaily.date))
                .limit(days)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    def get_today_confirmed_navs(self, fund_codes: List[str],
                                  target_date: date = None) -> Dict[str, FundNavDaily]:
        """批量查询当日已确认的官方净值"""
        if not fund_codes:
            return {}
        if target_date is None:
            target_date = date.today()
        session = self.get_session()
        try:
            records = session.execute(
                select(FundNavDaily).where(
                    and_(
                        FundNavDaily.fund_code.in_(fund_codes),
                        FundNavDaily.date == target_date,
                    )
                )
            ).scalars().all()
            return {r.fund_code: r for r in records}
        finally:
            session.close()

    def get_latest_nav(self, fund_code: str) -> Optional[FundNavDaily]:
        """获取最新一条净值记录"""
        session = self.get_session()
        try:
            return session.execute(
                select(FundNavDaily)
                .where(FundNavDaily.fund_code == fund_code)
                .order_by(desc(FundNavDaily.date))
                .limit(1)
            ).scalar_one_or_none()
        finally:
            session.close()

    def get_nav_by_date_range(self, fund_code: str, start_date: date, end_date: date) -> List[FundNavDaily]:
        """按日期范围查询基金净值，升序返回"""
        session = self.get_session()
        try:
            records = session.execute(
                select(FundNavDaily)
                .where(and_(
                    FundNavDaily.fund_code == fund_code,
                    FundNavDaily.date >= start_date,
                    FundNavDaily.date <= end_date,
                ))
                .order_by(FundNavDaily.date)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    # === 策略操作 ===

    def create_strategy(self, fund_code: str, strategy_type: str,
                        params: Dict[str, Any]) -> FundStrategy:
        """创建策略"""
        session = self.get_session()
        try:
            record = FundStrategy(
                fund_code=fund_code,
                strategy_type=strategy_type,
                params=json.dumps(params, ensure_ascii=False),
            )
            session.add(record)
            session.commit()
            session.refresh(record)
            return record
        except Exception as e:
            session.rollback()
            logger.error(f"创建策略失败 fund_code={fund_code}: {e}")
            raise
        finally:
            session.close()

    def get_strategies(self) -> List[FundStrategy]:
        """获取所有策略"""
        session = self.get_session()
        try:
            records = session.execute(
                select(FundStrategy).order_by(FundStrategy.created_at)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    def get_strategy_by_id(self, strategy_id: int) -> Optional[FundStrategy]:
        """根据 ID 获取策略"""
        session = self.get_session()
        try:
            return session.execute(
                select(FundStrategy).where(FundStrategy.id == strategy_id)
            ).scalar_one_or_none()
        finally:
            session.close()

    def update_strategy(self, strategy_id: int, params: Dict[str, Any] = None,
                        is_enabled: bool = None) -> bool:
        """更新策略"""
        session = self.get_session()
        try:
            record = session.execute(
                select(FundStrategy).where(FundStrategy.id == strategy_id)
            ).scalar_one_or_none()
            if not record:
                return False
            if params is not None:
                record.params = json.dumps(params, ensure_ascii=False)
            if is_enabled is not None:
                record.is_enabled = is_enabled
            record.updated_at = datetime.now()
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"更新策略失败 strategy_id={strategy_id}: {e}")
            return False
        finally:
            session.close()

    def delete_strategy(self, strategy_id: int) -> bool:
        """删除策略"""
        session = self.get_session()
        try:
            record = session.execute(
                select(FundStrategy).where(FundStrategy.id == strategy_id)
            ).scalar_one_or_none()
            if record:
                session.delete(record)
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"删除策略失败 strategy_id={strategy_id}: {e}")
            return False
        finally:
            session.close()

    def get_all_enabled_strategies(self, strategy_type: str = None) -> List[FundStrategy]:
        """获取所有启用的策略（供 pipeline 使用）"""
        session = self.get_session()
        try:
            query = select(FundStrategy).where(FundStrategy.is_enabled == True)
            if strategy_type:
                query = query.where(FundStrategy.strategy_type == strategy_type)
            records = session.execute(query).scalars().all()
            return list(records)
        finally:
            session.close()

    def create_strategy_alert(self, strategy_id: int, alert_date: date, alert_type: str,
                              alert_detail: Dict[str, Any] = None) -> Optional[StrategyAlert]:
        """创建策略告警记录（去重：同策略同天同类型只记录一次）"""
        session = self.get_session()
        try:
            record = StrategyAlert(
                strategy_id=strategy_id,
                alert_date=alert_date,
                alert_type=alert_type,
                alert_detail=json.dumps(alert_detail, ensure_ascii=False) if alert_detail else None,
            )
            session.add(record)
            session.commit()
            session.refresh(record)
            return record
        except Exception as e:
            session.rollback()
            if 'UNIQUE' in str(e).upper():
                logger.debug(f"策略告警已存在，跳过: strategy_id={strategy_id}, date={alert_date}")
                return None
            logger.error(f"创建策略告警失败: {e}")
            raise
        finally:
            session.close()

    def get_strategy_alerts(self, strategy_id: int, days: int = 30) -> List[StrategyAlert]:
        """获取策略告警记录"""
        session = self.get_session()
        try:
            cutoff = date.today() - timedelta(days=days)
            records = session.execute(
                select(StrategyAlert)
                .where(and_(
                    StrategyAlert.strategy_id == strategy_id,
                    StrategyAlert.alert_date >= cutoff,
                ))
                .order_by(desc(StrategyAlert.alert_date))
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    # === 盘中告警去重 ===

    def log_intraday_alert(self, fund_code: str, alert_date: date, alert_key: str) -> bool:
        """记录盘中告警，返回 True 表示新记录，False 表示今天已发过"""
        session = self.get_session()
        try:
            record = IntradayAlertLog(
                fund_code=fund_code,
                alert_date=alert_date,
                alert_key=alert_key,
            )
            session.add(record)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            if 'UNIQUE' in str(e).upper():
                return False
            logger.error(f"记录盘中告警失败: {e}")
            return False
        finally:
            session.close()

    # === 大盘指数操作 ===

    _DEFAULT_INDICES = [
        {'index_code': '000001', 'index_name': '上证指数', 'secid': '1.000001', 'sort_order': 1},
        {'index_code': '399001', 'index_name': '深证成指', 'secid': '0.399001', 'sort_order': 2},
        {'index_code': '399006', 'index_name': '创业板指', 'secid': '0.399006', 'sort_order': 3},
        {'index_code': '899050', 'index_name': '北证50',   'secid': '0.899050', 'sort_order': 4},
    ]

    def seed_default_indices(self) -> None:
        """初始化默认指数配置（幂等）"""
        session = self.get_session()
        try:
            for item in self._DEFAULT_INDICES:
                existing = session.execute(
                    select(MarketIndex).where(MarketIndex.index_code == item['index_code'])
                ).scalar_one_or_none()
                if not existing:
                    session.add(MarketIndex(**item))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"seed_default_indices 失败: {e}")
        finally:
            session.close()

    def get_enabled_indices(self) -> List[MarketIndex]:
        """获取所有启用的指数配置"""
        session = self.get_session()
        try:
            records = session.execute(
                select(MarketIndex)
                .where(MarketIndex.is_enabled == True)
                .order_by(MarketIndex.sort_order, MarketIndex.id)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    def save_index_intraday(self, records: List[Dict[str, Any]]) -> int:
        """批量插入指数分时记录"""
        session = self.get_session()
        saved = 0
        try:
            for r in records:
                session.add(IndexIntraday(
                    index_code=r['index_code'],
                    current_value=r.get('current_value'),
                    change_pct=r.get('change_pct'),
                    record_time=r.get('record_time', datetime.now()),
                ))
                saved += 1
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"保存指数分时数据失败: {e}")
        finally:
            session.close()
        return saved

    def get_today_index_intraday(self, index_code: str, target_date: date = None) -> List[IndexIntraday]:
        """获取指定指数今日分时记录"""
        session = self.get_session()
        try:
            if target_date is None:
                target_date = date.today()
            start = datetime.combine(target_date, datetime.min.time())
            end = datetime.combine(target_date, datetime.max.time())
            records = session.execute(
                select(IndexIntraday)
                .where(and_(
                    IndexIntraday.index_code == index_code,
                    IndexIntraday.record_time >= start,
                    IndexIntraday.record_time <= end,
                ))
                .order_by(IndexIntraday.record_time)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    def get_latest_index_intraday(self, index_code: str) -> Optional[IndexIntraday]:
        """获取指定指数最新一条分时记录"""
        session = self.get_session()
        try:
            return session.execute(
                select(IndexIntraday)
                .where(IndexIntraday.index_code == index_code)
                .order_by(desc(IndexIntraday.record_time))
                .limit(1)
            ).scalar_one_or_none()
        finally:
            session.close()

    def save_index_daily(self, records: List[Dict[str, Any]]) -> int:
        """批量 upsert 指数日线数据"""
        if not records:
            return 0

        total = 0
        for i in range(0, len(records), 500):
            batch = records[i:i + 500]
            session = self.get_session()
            try:
                for r in batch:
                    existing = session.execute(
                        select(IndexDaily).where(and_(
                            IndexDaily.index_code == r['index_code'],
                            IndexDaily.trade_date == r['trade_date'],
                        ))
                    ).scalar_one_or_none()

                    if existing:
                        if r.get('close_value') is not None:
                            existing.close_value = r['close_value']
                        if r.get('change_pct') is not None:
                            existing.change_pct = r['change_pct']
                    else:
                        session.add(IndexDaily(
                            index_code=r['index_code'],
                            trade_date=r['trade_date'],
                            close_value=r.get('close_value'),
                            change_pct=r.get('change_pct'),
                        ))
                    total += 1

                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存指数日线数据批次失败 (offset={i}): {e}")
            finally:
                session.close()
        return total

    def get_index_daily_history(self, index_code: str, days: int = 180) -> List[IndexDaily]:
        """获取指数历史日线数据"""
        session = self.get_session()
        try:
            records = session.execute(
                select(IndexDaily)
                .where(IndexDaily.index_code == index_code)
                .order_by(desc(IndexDaily.trade_date))
                .limit(days)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    def get_index_daily_by_range(self, index_code: str, start_date: date, end_date: date) -> List[IndexDaily]:
        """按日期范围查询指数日线，升序返回"""
        session = self.get_session()
        try:
            records = session.execute(
                select(IndexDaily)
                .where(and_(
                    IndexDaily.index_code == index_code,
                    IndexDaily.trade_date >= start_date,
                    IndexDaily.trade_date <= end_date,
                ))
                .order_by(IndexDaily.trade_date)
            ).scalars().all()
            return list(records)
        finally:
            session.close()

    # === 数据清理 ===

    def cleanup_stale_data(self) -> Dict[str, int]:
        """清理过期数据"""
        stats = {}

        # 1. fund_valuation — 删除 2 天前
        session = self.get_session()
        try:
            cutoff = datetime.combine(date.today() - timedelta(days=2), datetime.min.time())
            active_codes = session.execute(
                select(FundValuation.fund_code).distinct()
                .where(FundValuation.valuation_time >= cutoff)
            ).scalars().all()
            if active_codes:
                old = session.execute(
                    select(FundValuation).where(and_(
                        FundValuation.valuation_time < cutoff,
                        FundValuation.fund_code.in_(active_codes),
                    ))
                ).scalars().all()
                stats['fund_valuation'] = len(old)
                for r in old:
                    session.delete(r)
                session.commit()
            else:
                stats['fund_valuation'] = 0
        except Exception as e:
            session.rollback()
            logger.error(f"清理 fund_valuation 失败: {e}")
            stats['fund_valuation'] = 0
        finally:
            session.close()

        # 2. intraday_alert_log — 删除 2 天前
        session = self.get_session()
        try:
            cutoff_date = date.today() - timedelta(days=2)
            old = session.execute(
                select(IntradayAlertLog).where(IntradayAlertLog.alert_date < cutoff_date)
            ).scalars().all()
            stats['intraday_alert_log'] = len(old)
            for r in old:
                session.delete(r)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"清理 intraday_alert_log 失败: {e}")
            stats['intraday_alert_log'] = 0
        finally:
            session.close()

        # 3. strategy_alerts — 删除 90 天前
        session = self.get_session()
        try:
            cutoff_date = date.today() - timedelta(days=90)
            old = session.execute(
                select(StrategyAlert).where(StrategyAlert.alert_date < cutoff_date)
            ).scalars().all()
            stats['strategy_alerts'] = len(old)
            for r in old:
                session.delete(r)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"清理 strategy_alerts 失败: {e}")
            stats['strategy_alerts'] = 0
        finally:
            session.close()

        # 4. fund_nav_daily — 删除 180 天前
        session = self.get_session()
        try:
            cutoff_date = date.today() - timedelta(days=180)
            old = session.execute(
                select(FundNavDaily).where(FundNavDaily.date < cutoff_date)
            ).scalars().all()
            stats['fund_nav_daily'] = len(old)
            for r in old:
                session.delete(r)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"清理 fund_nav_daily 失败: {e}")
            stats['fund_nav_daily'] = 0
        finally:
            session.close()

        # 5. index_intraday — 删除 2 天前
        session = self.get_session()
        try:
            cutoff = datetime.combine(date.today() - timedelta(days=2), datetime.min.time())
            active_codes = session.execute(
                select(IndexIntraday.index_code).distinct()
                .where(IndexIntraday.record_time >= cutoff)
            ).scalars().all()
            if active_codes:
                old = session.execute(
                    select(IndexIntraday).where(and_(
                        IndexIntraday.record_time < cutoff,
                        IndexIntraday.index_code.in_(active_codes),
                    ))
                ).scalars().all()
                stats['index_intraday'] = len(old)
                for r in old:
                    session.delete(r)
                session.commit()
            else:
                stats['index_intraday'] = 0
        except Exception as e:
            session.rollback()
            logger.error(f"清理 index_intraday 失败: {e}")
            stats['index_intraday'] = 0
        finally:
            session.close()

        # 6. index_daily — 删除 180 天前
        session = self.get_session()
        try:
            cutoff_date = date.today() - timedelta(days=180)
            old = session.execute(
                select(IndexDaily).where(IndexDaily.trade_date < cutoff_date)
            ).scalars().all()
            stats['index_daily'] = len(old)
            for r in old:
                session.delete(r)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"清理 index_daily 失败: {e}")
            stats['index_daily'] = 0
        finally:
            session.close()

        total = sum(stats.values())
        if total > 0:
            detail = ", ".join(f"{k}={v}" for k, v in stats.items() if v > 0)
            logger.info(f"数据清理完成，共删除 {total} 条（{detail}）")
        else:
            logger.info("数据清理完成，无过期数据")

        return stats


def get_db() -> DatabaseManager:
    """获取数据库管理器实例的快捷方式"""
    return DatabaseManager.get_instance()
