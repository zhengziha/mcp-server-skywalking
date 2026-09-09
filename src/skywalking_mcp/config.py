"""运行时配置: 全部来自环境变量, 提供线上环境默认值."""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    base_url: str = field(
        default_factory=lambda: os.getenv("SKYWALKING_URL", "http://10.0.26.41:8080").rstrip("/")
    )
    timezone: str = field(default_factory=lambda: os.getenv("SKYWALKING_TZ", "Asia/Shanghai"))
    timeout: float = field(default_factory=lambda: float(os.getenv("SKYWALKING_TIMEOUT", "15")))
    # 链路(span)在 OAP 存储中的保留天数; 用于在查询更早窗口时提示"total 可能偏小".
    trace_retention_days: int = field(
        default_factory=lambda: int(os.getenv("SKYWALKING_TRACE_RETENTION_DAYS", "7"))
    )

    @property
    def graphql_url(self) -> str:
        return f"{self.base_url}/graphql"
