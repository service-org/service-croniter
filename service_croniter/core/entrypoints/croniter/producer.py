#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import eventlet
import typing as t

if t.TYPE_CHECKING:
    from service_core.core.service.entrypoint import BaseEntrypoint

    # 入口类型
    Entrypoint = t.TypeVar('Entrypoint', bound=BaseEntrypoint)

from datetime import datetime
from logging import getLogger
from croniter import croniter
from eventlet.green import time
from greenlet import GreenletExit
from eventlet.greenthread import GreenThread
from service_core.core.spawning import SpawningProxy
from service_core.core.decorator import AsFriendlyFunc
from service_core.core.service.extension import ShareExtension
from service_core.core.service.extension import StoreExtension
from service_core.core.service.entrypoint import BaseEntrypoint

logger = getLogger(__name__)


class CronProducer(BaseEntrypoint, ShareExtension, StoreExtension):
    """ 定时任务生产者类 """

    name = 'CronProducer'

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """ 初始化实例

        @param args  : 位置参数
        @param kwargs: 命名参数
        """
        self.gt_list = []
        self.stopped = False

        BaseEntrypoint.__init__(self, *args, **kwargs)
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
        wait_func = AsFriendlyFunc(base_func, all_exception=(GreenletExit,))
        wait_func()

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.stopped = True
        base_func = SpawningProxy(self.gt_list).kill
        kill_func = AsFriendlyFunc(base_func, all_exception=(GreenletExit,))
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
        tid = f'{self}.consumer_handle_request'
        cron_option = extension.cron_option
        expr_format = extension.expr_format
        exec_atonce = extension.exec_atonce
        cron_option.setdefault('start_time', time.time())
        time_control = croniter(expr_format, **cron_option)
        if not exec_atonce:
            exec_nxtime = time_control.get_next()
            exec_dttime = datetime.fromtimestamp(exec_nxtime)
            logger.debug(f'{self.container.service.name}:{tid} next run at {exec_dttime}')
        else:
            exec_nxtime = None
            self.container.spawn_splits_thread(extension.handle_request, tid=tid)
        while True:
            if self.stopped:
                break
            if exec_nxtime is None:
                exec_nxtime = time_control.get_next()
                exec_dttime = datetime.fromtimestamp(exec_nxtime)
                self.container.spawn_splits_thread(extension.handle_request, tid=tid)
                logger.debug(f'{self.container.service.name}:{tid} next run at {exec_dttime}')
                continue
            if time.time() >= exec_nxtime:
                exec_nxtime = None
            eventlet.sleep(0.01)
