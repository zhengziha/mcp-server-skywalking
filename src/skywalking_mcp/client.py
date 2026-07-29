"""SkyWalking GraphQL 异步客户端与时间窗口(Duration)构造."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from . import queries
from .config import Config

# Duration.start/end 的格式必须与 step 严格对应(实测: 格式或时区不匹配会导致时间桶对不上)
STEP_FORMATS = {
    "SECOND": "%Y-%m-%d %H%M%S",
    "MINUTE": "%Y-%m-%d %H%M",
    "HOUR": "%Y-%m-%d %H",
    "DAY": "%Y-%m-%d",
}

STEP_DELTAS = {
    "SECOND": timedelta(seconds=1),
    "MINUTE": timedelta(minutes=1),
    "HOUR": timedelta(hours=1),
    "DAY": timedelta(days=1),
}

ABSOLUTE_TIME_FORMAT = "%Y-%m-%d %H:%M"


class SkyWalkingError(Exception):
    """OAP GraphQL 返回错误或请求失败."""


def build_duration(
    minutes: int = 30,
    step: str = "MINUTE",
    start_time: str | None = None,
    end_time: str | None = None,
    tz: str = "Asia/Shanghai",
) -> dict[str, str]:
    """构造 Duration: 优先使用绝对时间段(yyyy-MM-dd HH:mm), 否则取最近 minutes 分钟."""
    step = step.upper()
    if step not in STEP_FORMATS:
        raise ValueError(f"step 必须是 {sorted(STEP_FORMATS)} 之一, 收到: {step}")
    tzinfo = ZoneInfo(tz)
    if start_time or end_time:
        if not (start_time and end_time):
            raise ValueError("start_time 与 end_time 必须同时提供")
        try:
            start = datetime.strptime(start_time, ABSOLUTE_TIME_FORMAT).replace(tzinfo=tzinfo)
            end = datetime.strptime(end_time, ABSOLUTE_TIME_FORMAT).replace(tzinfo=tzinfo)
        except ValueError as e:
            raise ValueError(f"绝对时间格式必须为 yyyy-MM-dd HH:mm, 解析失败: {e}") from e
    else:
        end = datetime.now(tzinfo)
        start = end - timedelta(minutes=minutes)
    if start >= end:
        raise ValueError(f"start_time 必须早于 end_time: {start} >= {end}")
    fmt = STEP_FORMATS[step]
    return {"start": start.strftime(fmt), "end": end.strftime(fmt), "step": step}


def duration_bucket_labels(duration: dict[str, str]) -> list[str]:
    """按 Duration 生成时间桶标签序列(闭区间), 用于给 trend 数值对齐时间轴."""
    step = duration["step"]
    fmt = STEP_FORMATS[step]
    start = datetime.strptime(duration["start"], fmt)
    end = datetime.strptime(duration["end"], fmt)
    delta = STEP_DELTAS[step]
    labels: list[str] = []
    cur = start
    while cur <= end and len(labels) < 10000:
        labels.append(cur.strftime(fmt))
        cur += delta
    return labels


class SkyWalkingClient:
    def __init__(self, config: Config | None = None):
        self.config = config or Config()

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                resp = await client.post(
                    self.config.graphql_url,
                    json={"query": query, "variables": variables},
                )
                resp.raise_for_status()
        except httpx.HTTPError as e:
            raise SkyWalkingError(f"请求 SkyWalking OAP 失败 ({self.config.graphql_url}): {e}") from e
        body = resp.json()
        if body.get("errors"):
            messages = "; ".join(err.get("message", str(err)) for err in body["errors"])
            raise SkyWalkingError(f"GraphQL 错误: {messages}")
        return body.get("data") or {}

    async def get_all_services(self, duration: dict[str, str]) -> list[dict[str, str]]:
        data = await self.graphql(queries.GET_ALL_SERVICES, {"duration": duration})
        return data.get("getAllServices") or []

    async def search_endpoint(
        self, keyword: str, service_id: str, limit: int = 20
    ) -> list[dict[str, str]]:
        data = await self.graphql(
            queries.SEARCH_ENDPOINT,
            {"keyword": keyword, "serviceId": service_id, "limit": limit},
        )
        return data.get("searchEndpoint") or []

    async def read_metrics_values(
        self, metric_name: str, entity: dict[str, Any], duration: dict[str, str]
    ) -> list[int]:
        """返回该指标按时间桶的数值序列."""
        data = await self.graphql(
            queries.READ_METRICS_VALUES,
            {"condition": {"name": metric_name, "entity": entity}, "duration": duration},
        )
        values = (data.get("readMetricsValues") or {}).get("values") or {}
        return [v.get("value") or 0 for v in values.get("values") or []]

    async def read_labeled_metrics_values(
        self,
        metric_name: str,
        labels: list[str],
        entity: dict[str, Any],
        duration: dict[str, str],
    ) -> dict[str, list[int]]:
        """返回 {label: 数值序列}, 用于 endpoint_percentile 等多值指标."""
        data = await self.graphql(
            queries.READ_LABELED_METRICS_VALUES,
            {
                "condition": {"name": metric_name, "entity": entity},
                "labels": labels,
                "duration": duration,
            },
        )
        result: dict[str, list[int]] = {}
        for item in data.get("readLabeledMetricsValues") or []:
            values = (item.get("values") or {}).get("values") or []
            result[item.get("label")] = [v.get("value") or 0 for v in values]
        return result

    async def query_basic_traces(self, condition: dict[str, Any]) -> dict[str, Any]:
        data = await self.graphql(queries.QUERY_BASIC_TRACES, {"condition": condition})
        return data.get("queryBasicTraces") or {"traces": [], "total": 0}

    async def query_trace(self, trace_id: str) -> list[dict[str, Any]]:
        data = await self.graphql(queries.QUERY_TRACE, {"traceId": trace_id})
        return (data.get("queryTrace") or {}).get("spans") or []
