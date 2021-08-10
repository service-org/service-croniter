# 运行环境

|system |python | 
|:------|:------|      
|cross platform |3.9.16|

# 组件安装

```shell
pip install -U service-croniter 
```

# 服务配置

> config.yaml

# 入门案例

```yaml
├── config.yaml
├── facade.py
└── project
    ├── __init__.py
    └── service.py
```

> service.py

```python
#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from logging import getLogger
from service_croniter.core.entrypoints import croniter
from service_core.core.service import Service as BaseService

logger = getLogger(__name__)


class Service(BaseService):
    """ 微服务类 """

    # 微服务名称
    name = 'demo'
    # 微服务简介
    desc = 'demo'

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        # 此服务无需启动监听端口, 请初始化掉下面参数
        self.host = ''
        self.port = 0
        super(Service, self).__init__(*args, **kwargs)

    @croniter.cron('* * * * * */1')
    def test_croniter_every_second_with_exec_atonce(self) -> None:
        """ 测试每秒且立即执行

        doc: https://github.com/kiorky/croniter
        """
        logger.debug('yeah~ yeah~ yeah~, i am called ~')
```

> facade.py

```python
#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

from project import Service

service = Service()
```

# 运行服务

> core start facade --debug

# 运行调试

> core debug --port `port`
