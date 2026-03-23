# -*- coding: utf-8 -*-
"""
===================================
基金估值助手 - 主入口
===================================

使用方式：
    python main.py              # 抓取一次当前估值并输出
    python main.py --schedule   # 调度模式（盘中轮询+收盘获取）
    python main.py --eod        # 仅获取收盘净值
    python main.py --web        # 启动 Web 服务
    python main.py --debug      # 调试模式
"""

import argparse
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def setup_logging(debug: bool = False, log_dir: str = "./logs") -> None:
    """配置日志系统（控制台 + 文件）"""
    level = logging.DEBUG if debug else logging.INFO

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now().strftime('%Y%m%d')
    log_file = log_path / f"fund_valuation_{today_str}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # 控制台
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(console)

    # 文件
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(file_handler)

    # 降低第三方库日志级别
    for lib in ['urllib3', 'sqlalchemy', 'werkzeug', 'schedule']:
        logging.getLogger(lib).setLevel(logging.WARNING)

    logging.info(f"日志初始化完成: {log_file}")


logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='基金估值助手',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python main.py                # 抓取一次估值
  python main.py --schedule     # 启动调度模式
  python main.py --eod          # 获取收盘净值
  python main.py --web          # 启动 Web 服务
  python main.py --web --schedule  # Web + 调度同时运行
  python main.py --debug        # 调试模式
        '''
    )

    parser.add_argument('--schedule', action='store_true',
                        help='启动调度模式（盘中轮询+收盘）')
    parser.add_argument('--eod', action='store_true',
                        help='仅获取收盘净值')
    parser.add_argument('--web', action='store_true',
                        help='启动 Web 服务')
    parser.add_argument('--debug', action='store_true',
                        help='调试模式')

    return parser.parse_args()


def main() -> int:
    """主入口"""
    args = parse_arguments()

    # 加载配置（必须在其他模块导入前）
    from src.config import get_config
    config = get_config()

    # 确保数据目录存在
    Path(config.sqlite_path).parent.mkdir(parents=True, exist_ok=True)

    # 设置日志
    setup_logging(debug=args.debug, log_dir=config.log_dir)

    logger.info("=" * 50)
    logger.info("基金估值助手 启动")
    logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    # 验证配置
    warnings = config.validate()
    for w in warnings:
        logger.warning(w)

    logger.info(f"关注基金: {', '.join(config.fund_list)}")

    try:
        from src.pipeline import FundValuationPipeline

        # 模式1: Web 服务
        if args.web:
            from src.web.app import create_app
            import threading

            app = create_app()

            if args.schedule:
                # Web + 调度同时运行
                logger.info("模式: Web + 调度同时运行")
                pipeline = FundValuationPipeline(config=config)

                from src.scheduler import FundScheduler
                scheduler = FundScheduler(
                    refresh_interval=config.refresh_interval,
                    schedule_times=config.schedule_times,
                )
                scheduler.set_tasks(
                    intraday=pipeline.intraday_task,
                    eod=pipeline.eod_task,
                )

                scheduler_thread = threading.Thread(
                    target=scheduler.run, daemon=True
                )
                scheduler_thread.start()

            elif args.eod:
                logger.info("模式: Web + EOD 收盘净值")
                pipeline = FundValuationPipeline(config=config)
                threading.Thread(target=pipeline.eod_task, daemon=True).start()

            logger.info(f"启动 Web 服务: http://{config.web_host}:{config.web_port}")
            app.run(host=config.web_host, port=config.web_port, debug=args.debug)
            return 0

        # 模式2: 调度模式
        if args.schedule:
            logger.info("模式: 调度模式")
            pipeline = FundValuationPipeline(config=config)

            from src.scheduler import FundScheduler
            scheduler = FundScheduler(refresh_interval=config.refresh_interval)
            scheduler.set_tasks(
                intraday=pipeline.intraday_task,
                eod=pipeline.eod_task,
            )
            scheduler.run()
            return 0

        # 模式3: 仅获取收盘净值
        if args.eod:
            logger.info("模式: 获取收盘净值")
            pipeline = FundValuationPipeline(config=config)
            pipeline.eod_task()
            return 0

        # 默认模式: 抓取一次估值
        pipeline = FundValuationPipeline(config=config)
        pipeline.run_once()
        return 0

    except KeyboardInterrupt:
        logger.info("用户中断，程序退出")
        return 130
    except Exception as e:
        logger.exception(f"程序执行失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
