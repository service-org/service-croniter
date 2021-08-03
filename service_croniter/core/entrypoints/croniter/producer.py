#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import eventlet
import typing as t

from logging import getLogger
from datetime import datetime
from croniter import croniter
from eventlet.green import time
from greenlet import GreenletExit
from eventlet.greenthread import GreenThread
from service_core.core.spawning import SpawningProxy
from service_core.core.decorator import AsFriendlyFunc
from service_core.core.service.entrypoint import Entrypoint
from service_core.core.service.extension import ShareExtension
from service_core.core.service.extension import StoreExtension

logger = getLogger(__name__)


class CronProducer(Entrypoint, ShareExtension, StoreExtension):
    """ 定时任务生产者类 """

    name = 'CronProducer'

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """ 初始化实例

        @param args  : 位置参数
        @param kwargs: 命名参数
        """
        self.gt_list = []
        self.stopped = False

        Entrypoint.__init__(self, *args, **kwargs)
        ShareExtension.__init__(self, *args, **kwargs)
        StoreExtension.__init__(self, *args, **kwargs)

    def start(self) -> None:
        """ 生命周期 - 启动阶段

        @return: None
        """
        self.gt_list = [self.spawn_timer_thread(e) for e in self.all_extensions]

    def stop(self) -> None:
        """ 生命周期 - 关闭阶段

        @return: None
        """
        self.stopped = True
        base_func = SpawningProxy(self.gt_list).wait
        exception = (GreenletExit,)
        wait_func = AsFriendlyFunc(base_func, all_exception=exception)
        wait_func()

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.stopped = True
        base_func = SpawningProxy(self.gt_list).kill
        exception = (GreenletExit,)
        kill_func = AsFriendlyFunc(base_func, all_exception=exception)
        kill_func()

    def spawn_timer_thread(self, extension: Entrypoint) -> GreenThread:
        """ 创建一个计时器协程

        @param extension: 入口对象
        @return: GreenThread
        """
        func = self.timer
        args, kwargs, tid = (extension,), {}, f'{self}.self_timer'
        return self.container.spawn_splits_thread(func, args, kwargs, tid=tid)

    def timer(self, extension: Entrypoint) -> None:
        """ 计时器协程的实现

        @param extension: 入口对象
        @return: None
        """
        exec_nxtime = None
        tid = f'{self}.consumer_handle_request'
        cron_option = extension.cron_option
        expr_format = extension.expr_format
        cron_option.setdefault('start_time', time.time())
        time_control = croniter(expr_format, **cron_option)
        while not self.stopped:
            try:
                # 当下次执行时间为None则说明首次运行,立即计算下次执行时间
                if exec_nxtime is None:
                    exec_nxtime = time_control.get_next()
                    exec_dttime = datetime.fromtimestamp(exec_nxtime)
                    mesg = f'{self.container.service.name}:{tid} next run at {exec_dttime}'
                    logger.debug(mesg)
                # 当当前时间大于等于预计算的下次执行时间则立即提交任务给hub
                if time.time() >= exec_nxtime:
                    self.container.spawn_splits_thread(extension.handle_request, tid=tid)
                    exec_nxtime = None
                eventlet.sleep(0.0001)
                # 优雅处理如ctrl + c, sys.exit, kill thread时的异常
            except (KeyboardInterrupt, SystemExit, GreenletExit):
                break
            except:
                # 应该避免其它未知异常中断当前计时器导致定时任务无法被调度
                logger.error(f'unexpected error while timer spawn', exc_info=True)
        logger.debug(f'{self}.self_timer has been graceful stopped')
