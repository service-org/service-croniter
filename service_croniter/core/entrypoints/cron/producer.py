#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import time
import typing as t

if t.TYPE_CHECKING:
    from service_core.core.service.entrypoint import BaseEntrypoint

    # 入口类型
    Entrypoint = t.TypeVar('Entrypoint', bound=BaseEntrypoint)

from croniter import croniter
from greenlet import GreenletExit
from eventlet.greenthread import GreenThread
from service_core.core.spawning import SpawningProxy
from service_core.core.decorator import AsFriendlyFunc
from service_core.core.service.extension import ShareExtension
from service_core.core.service.extension import StoreExtension
from service_core.core.service.entrypoint import BaseEntrypoint


class CronProducer(BaseEntrypoint, ShareExtension, StoreExtension):
    """ 定时任务生产类 """

    name = 'cron-producer'

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """ 初始化实例

        @param args  : 位置参数
        @param kwargs: 命名参数
        """
        self.gt_list = []
        self.stopped = False

        ShareExtension.__init__(self, *args, **kwargs)
        StoreExtension.__init__(self, *args, **kwargs)
        BaseEntrypoint.__init__(self, *args, **kwargs)

    def start(self) -> None:
        """ 生命周期 - 启动阶段

        @return: None
        """
        self.gt_list = [self.create_producer_thread(e) for e in self.all_extensions]

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

    def create_producer_thread(self, extension: Entrypoint) -> GreenThread:
        """ 创建生产者协程

        @param extension: 入口对象
        @return: GreenThread
        """
        func = self.create_consumer_worker
        args, kwargs, tid = (extension,), {}, 'create_croniter_splits_thread'
        return self.container.spawn_splits_thread(func, args, kwargs, tid=tid)

    def create_consumer_worker(self, extension: Entrypoint) -> None:
        """ 触发消费者协程

        @param extension: 入口对象
        @return: None
        """
        run = extension.exec_atonce
        tid = 'create_croniter_worker_splits_thread'
        cron_option = extension.cron_option
        cron_option['start_time'] = time.time()
        itr = croniter('* * * * * 1', **extension.cron_option)
        run and self.container.spawn_splits_thread(extension.handle_worker, tid=tid)
        nxt = None if run else itr.get_next()
        from datetime import datetime
        while True:
            if self.stopped:
                break
            now = time.time()
            if nxt is None:
                nxt = itr.get_next()
                self.container.spawn_splits_thread(extension.handle_worker, tid=tid)
                continue
            print('!' * 100)
            print(now, nxt, now >= nxt)
            print(datetime.fromtimestamp(now), datetime.fromtimestamp(nxt))
            print('!' * 100)
            if now >= nxt:
                nxt = None
            time.sleep(1)
