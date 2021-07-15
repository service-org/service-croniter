#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from service_core.core.context import WorkerContext

from eventlet.event import Event
from eventlet.greenthread import GreenThread
from service_core.core.service.entrypoint import BaseEntrypoint

from .producer import CronProducer


class CronConsumer(BaseEntrypoint):
    """ 定时任务消费者类 """

    name = 'CronConsumer'

    producer = CronProducer()

    def __init__(self, expr_format: t.Text, exec_atonce: bool = False, **cron_option: t.Any) -> None:
        """ 初始化实例

        @param expr_format: 时间表达式
        @param exec_atonce: 立即执行 ?
        @param cron_option: 其它的选项
        """
        self.exec_atonce = exec_atonce
        self.expr_format = expr_format
        self.cron_option = cron_option
        super(CronConsumer, self).__init__()

    def setup(self) -> None:
        """ 生命周期 - 载入阶段

        @return: None
        """
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
        """ 连接执行结果

        @param gt: 协程对象
        @param event: 事件
        @return: None
        """
        context, excinfo, results = gt.wait()
        event.send((context, excinfo, results))

    def handle_request(self) -> t.Tuple:
        """ 处理工作请求

        @return: t.Tuple
        """
        event = Event()
        tid = f'{self.name}.self_handle_request'
        gt = self.container.spawn_worker_thread(self, tid=tid)
        gt.link(self._link_results, event)
        context, excinfo, results = event.wait()
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
        return results

    def handle_errors(self, context: WorkerContext, excinfo: t.Tuple) -> t.Any:
        """ 处理异常结果

        @param context: 上下文对象
        @param excinfo: 异常对象
        @return: t.Any
        """
        return excinfo

