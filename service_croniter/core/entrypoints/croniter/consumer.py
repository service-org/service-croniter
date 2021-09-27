#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import sys
import eventlet
import typing as t

from logging import getLogger
from eventlet.event import Event
from eventlet.greenthread import GreenThread
from service_core.core.context import WorkerContext
from service_croniter.constants import CRONITER_CONFIG_KEY
from service_core.core.service.entrypoint import Entrypoint

from .producer import CronProducer

logger = getLogger(__name__)


class CronConsumer(Entrypoint):
    """ 定时任务消费者类 """

    name = 'CronConsumer'

    producer = CronProducer()

    def __init__(
            self,
            expr_format: t.Text,
            crontab_options: t.Dict[t.Dict[t.Text, t.Any]] = None,
            **kwargs: t.Any
    ) -> None:
        """ 初始化实例

        @param expr_format: 时间表达式
        @param crontab_options: 计划配置
        """
        super(CronConsumer, self).__init__(**kwargs)
        self.expr_format = expr_format
        kwargs.setdefault('exec_timing', None)
        self.crontab_options = crontab_options or {}

    def setup(self) -> None:
        """ 生命周期 - 载入阶段

        @return: None
        """
        crontab_options = self.container.config.get(f'{CRONITER_CONFIG_KEY}.crontab_options', default={})
        # 防止YAML中声明值为None
        self.crontab_options = (crontab_options or {}) | self.crontab_options
        self.producer.reg_extension(self)

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.producer.del_extension(self)

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.producer.del_extension(self)

    @staticmethod
    def _link_results(gt: GreenThread, event: Event) -> None:
        """ 等待执行结果

        @param gt: 协程对象
        @param event: 事件
        @return: None
        """
        # fix: 此协程异常会导致收不到event最终内存溢出!
        try:
            context, results, excinfo = gt.wait()
        except BaseException:
            results, excinfo = None, sys.exc_info()
            context = eventlet.getcurrent().context
        event.send((context, results, excinfo))

    def handle_request(self) -> t.Tuple:
        """ 处理工作请求

        @return: t.Tuple
        """
        event = Event()
        tid = f'{self}.self_handle_request'
        gt = self.container.spawn_worker_thread(self, tid=tid)
        gt.link(self._link_results, event)
        # 注意: 协程异常会导致收不到event最终内存溢出!
        context, results, excinfo = event.wait()
        return (
            self.handle_result(context, results)
            if excinfo is None else
            self.handle_errors(context, excinfo)
        )

    def handle_result(self, context: WorkerContext, results: t.Any) -> t.Any:
        """ 处理正常结果

        @param context: 上下文对象
        @param results: 结果对象
        @return: t.Any
        """
        pass

    def handle_errors(self, context: WorkerContext, excinfo: t.Tuple) -> t.Any:
        """ 处理异常结果

        @param context: 上下文对象
        @param excinfo: 异常对象
        @return: t.Any
        """
        pass
