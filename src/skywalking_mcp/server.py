"""SkyWalking MCP Server: 注册工具并以 stdio 运行."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

try:  # mcp >= 2.0
    from mcp.server import MCPServer as FastMCP
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP

from . import analysis
from .client import (
    STEP_FORMATS,
    SkyWalkingClient,
    SkyWalkingError,
    build_duration,
    duration_bucket_labels,
)
from .config import Config

mcp = FastMCP("skywalking")

# stdio 模式下避免 httpx 请求日志刷屏
logging.getLogger("httpx").setLevel(logging.WARNING)

_config = Config()
_client = SkyWalkingClient(_config)

# endpoint_percentile 的标签 0..4 依次对应 p50/p75/p90/p95/p99
PERCENTILE_LABELS = {"0": "p50", "1": "p75", "2": "p90", "3": "p95", "4": "p99"}
ENDPOINT_SEARCH_CONCURRENCY = 10
TRACE_STATES = {"ALL", "SUCCESS", "ERROR"}

# 分钟粒度性能查询的桶数上限: 超过后 OAP 可能报原始 GraphQL 错误, 自动降为 HOUR/DAY
MINUTE_BUCKET_MAX = 500
# 超过该分钟数(≈20.8 天)直接降为 DAY 粒度
MINUTE_DAY_THRESHOLD = 30000


def _duration(
    minutes: int,
    step: str = "MINUTE",
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, str]:
    return build_duration(
        minutes=minutes,
        step=step,
        start_time=start_time,
        end_time=end_time,
        tz=_config.timezone,
    )


def _ts_to_local(ms: int | str) -> str:
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return dt.astimezone(ZoneInfo(_config.timezone)).strftime("%Y-%m-%d %H:%M:%S")


def _span_minutes(duration: dict[str, str]) -> int:
    """Duration 跨度的分钟数."""
    fmt = STEP_FORMATS.get(duration.get("step", "MINUTE"), STEP_FORMATS["MINUTE"])
    try:
        start = datetime.strptime(duration["start"], fmt)
        end = datetime.strptime(duration["end"], fmt)
    except (ValueError, KeyError):
        return 0
    return max(0, int((end - start).total_seconds() / 60))


def _coarser_duration(
    duration: dict[str, str],
) -> tuple[dict[str, str], int] | None:
    """性能查询自动降粒度: MINUTE 桶数 > 上限时升为 HOUR(跨度过长则 DAY)。

    Returns (new_duration, minutes_per_bucket_scale) 或 None(无需调整)。
    桶内假设为"每桶总调用量", 除以 scale 可得等效每分钟, 供汇总/趋势使用。
    """
    if duration["step"] not in ("MINUTE", "HOUR"):
        return None
    minutes = _span_minutes(duration)
    step = duration["step"]
    if step == "MINUTE":
        if minutes <= MINUTE_BUCKET_MAX:
            return None
        # 对齐小时边界最多多出 1~2 桶, 接近 ~500 桶上限时直接跨到 DAY
        target = "HOUR" if minutes < MINUTE_DAY_THRESHOLD else "DAY"
    else:  # HOUR
        # HOUR 窗口的分钟数本就是 60 的整数倍, 直接按桶数判断
        if minutes // 60 <= MINUTE_BUCKET_MAX:
            return None
        target = "DAY"

    old_fmt = STEP_FORMATS[step]
    new_fmt = STEP_FORMATS[target]
    start = datetime.strptime(duration["start"], old_fmt)
    end = datetime.strptime(duration["end"], old_fmt)
    if target == "HOUR":
        start = start.replace(minute=0, second=0, microsecond=0)
        if end.minute or end.second or end.microsecond:
            end = end.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            end = end.replace(second=0, microsecond=0)
    else:  # DAY
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        if end.hour or end.minute or end.second or end.microsecond:
            end = end.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    new_duration = {
        "start": start.strftime(new_fmt),
        "end": end.strftime(new_fmt),
        "step": target,
    }
    scale = {"HOUR": 60, "DAY": 1440}[target]
    return new_duration, scale


def _trace_retention_note(duration: dict[str, str]) -> str | None:
    """当查询窗口早于链路保留期时给出提示, 避免把 total 偏小误判为"没被调用过"."""
    days = max(1, getattr(_config, "trace_retention_days", 7))
    try:
        start = datetime.strptime(
            duration["start"], STEP_FORMATS.get(duration["step"], STEP_FORMATS["MINUTE"])
        )
    except (ValueError, KeyError):
        return None
    tzinfo = ZoneInfo(_config.timezone)
    start_local = start.replace(tzinfo=tzinfo)
    if start_local >= datetime.now(tzinfo) - timedelta(days=days):
        return None
    return (
        f"注意: SkyWalking 链路(span)通常只保留近 {days} 天, "
        f"{start_local:%Y-%m-%d %H:%M} 之前的数据可能已被清理, total/采样可能偏小; "
        f"若确认那段时间确有调用, 需先核对 OAP 存储 TTL"
    )


def _friendly_error(e: Exception) -> str:
    """把 OAP 原始 GraphQL 错误包装成带方向的提示."""
    text = str(e)
    if "GraphQL 错误" not in text:
        return text
    return (
        f"{text} | 提示: 原始 GraphQL 错误常源于 窗口x粒度 超 OAP 限制或指标不存在。"
        f"分钟粒度已自动在窗口 > {MINUTE_BUCKET_MAX} 分钟时降为 HOUR/DAY; "
        f"若窗口很短仍报错, 多为端点/服务不存在或非 Entry 端点, 请先用 search_endpoints 确认。"
    )


async def _resolve_service(service_name: str, duration: dict[str, str]) -> dict[str, str]:
    """按名称解析服务, 优先精确匹配, 其次唯一的子串匹配."""
    services = await _client.get_all_services(duration)
    for svc in services:
        if svc["name"] == service_name:
            return svc
    matched = [s for s in services if service_name.lower() in s["name"].lower()]
    if len(matched) == 1:
        return matched[0]
    if matched:
        names = ", ".join(s["name"] for s in matched[:10])
        raise SkyWalkingError(f"服务名 '{service_name}' 匹配到多个服务, 请指定其一: {names}")
    raise SkyWalkingError(f"未找到服务 '{service_name}', 可先调用 list_services 查看可用服务")


async def _search_endpoints_impl(
    keyword: str,
    service_name: str,
    duration: dict[str, str],
    limit: int,
) -> list[dict[str, str]]:
    if service_name:
        svc = await _resolve_service(service_name, duration)
        endpoints = await _client.search_endpoint(keyword, svc["id"], limit)
        return [
            {
                "service_name": svc["name"],
                "service_id": svc["id"],
                "endpoint_id": ep["id"],
                "endpoint_name": ep["name"],
            }
            for ep in endpoints
        ]

    # 未指定服务时对全部服务有界并发搜索
    services = await _client.get_all_services(duration)
    sem = asyncio.Semaphore(ENDPOINT_SEARCH_CONCURRENCY)

    async def search_one(svc: dict[str, str]) -> list[dict[str, str]]:
        async with sem:
            try:
                endpoints = await _client.search_endpoint(keyword, svc["id"], limit)
            except SkyWalkingError:
                return []
        return [
            {
                "service_name": svc["name"],
                "service_id": svc["id"],
                "endpoint_id": ep["id"],
                "endpoint_name": ep["name"],
            }
            for ep in endpoints
        ]

    results = await asyncio.gather(*(search_one(svc) for svc in services))
    flat = [item for sub in results for item in sub]
    return flat[: max(limit, 1) * 5]


def _weighted_avg(values: list[int], weights: list[int]) -> float | None:
    pairs = [(v, w) for v, w in zip(values, weights) if w > 0]
    if not pairs:
        return None
    total_w = sum(w for _, w in pairs)
    return round(sum(v * w for v, w in pairs) / total_w, 2)


async def _endpoint_performance_impl(
    endpoint_name: str,
    service_name: str,
    duration: dict[str, str],
) -> dict[str, Any]:
    # 窗口过大时自动把 MINUTE 粒度降为 HOUR/DAY, 避免 OAP 直接抛原始 GraphQL 错误
    # (scale=每桶等效分钟数, 供把桶内总量换算成每分钟口径)。
    orig_minutes = _span_minutes(duration)
    orig_step = duration["step"]
    escalated = _coarser_duration(duration)
    if escalated is not None:
        duration, scale = escalated
    else:
        scale = 1
    entity = {
        "scope": "Endpoint",
        "serviceName": service_name,
        "normal": True,
        "endpointName": endpoint_name,
    }
    cpm_task = _client.read_metrics_values("endpoint_cpm", entity, duration)
    avg_task = _client.read_metrics_values("endpoint_avg", entity, duration)
    sla_task = _client.read_metrics_values("endpoint_sla", entity, duration)
    pct_task = _client.read_labeled_metrics_values(
        "endpoint_percentile", list(PERCENTILE_LABELS), entity, duration
    )
    cpm, avg, sla, pct = await asyncio.gather(
        cpm_task, avg_task, sla_task, pct_task, return_exceptions=True
    )
    if isinstance(cpm, BaseException):
        raise cpm if isinstance(cpm, SkyWalkingError) else SkyWalkingError(str(cpm))
    avg = [] if isinstance(avg, BaseException) else avg
    sla = [] if isinstance(sla, BaseException) else sla
    percentiles_raw = {} if isinstance(pct, BaseException) else pct

    timestamps = duration_bucket_labels(duration)
    n = min(len(timestamps), len(cpm)) if cpm else 0
    active_buckets = sum(1 for v in cpm if v > 0)
    total_calls = sum(cpm)

    avg_resp = _weighted_avg(avg, cpm) if avg else None
    sla_avg = _weighted_avg(sla, cpm) if sla else None

    percentile_summary: dict[str, Any] = {}
    percentile_trend: dict[str, list[int]] = {}
    for label, name in PERCENTILE_LABELS.items():
        series = percentiles_raw.get(label) or []
        if series:
            percentile_trend[name] = series[:n] if n else series
            avg_p = _weighted_avg(series, cpm)
            percentile_summary[name] = {
                "avg_ms": avg_p,
                "max_ms": max(series),
            }

    summary = {
        "avg_resp_time_ms": avg_resp,
        "max_resp_time_ms": max(avg) if avg else None,
        # 桶粒度被放大时除以 scale 得到"等效每分钟"均摊吞吐, 便于与 MINUTE 口径直接比较
        "throughput_cpm_avg": (
            round(total_calls / (active_buckets * scale), 2) if active_buckets else 0
        ),
        "total_calls": total_calls,
        "success_rate_pct": round(sla_avg / 100, 2) if sla_avg is not None else None,
        "percentiles": percentile_summary or None,
        "active_buckets": active_buckets,
        "total_buckets": len(cpm),
    }

    result: dict[str, Any] = {
        "service": service_name,
        "endpoint": endpoint_name,
        "window": duration,
        "summary": summary,
        "trend": {
            "timestamps": timestamps[:n],
            # 桶粒度放大后按 scale 换算为等效每分钟桶值
            "throughput_cpm": [round(v / scale, 2) for v in cpm[:n]],
            "avg_resp_time_ms": avg[:n],
            "success_rate_pct": [round(v / 100, 2) for v in sla[:n]],
            "percentiles": percentile_trend or None,
        },
    }
    if escalated is not None:
        result["note"] = (
            f"查询窗口 {orig_minutes} 分钟超过 {orig_step} 粒度桶数上限(约 {MINUTE_BUCKET_MAX}), "
            f"已自动把桶粒度降为 {duration['step']}(每桶 {scale} 分钟)。趋势/汇总里的吞吐量已换算为"
            f"等效每分钟口径; 若需精确原始桶值, 请按 ≤{MINUTE_BUCKET_MAX} 个桶拆分窗口分段查询。"
        )
    if active_buckets == 0:
        result["hint"] = (
            "该时间范围内无调用数据。注意: 端点指标只在其作为 Entry span 的服务上产生, "
            "请确认 endpoint_name 是入口端点(可用 search_endpoints 确认), 且服务名/时间范围正确。"
        )
    return result


async def _search_slow_traces_impl(
    service_name: str,
    endpoint_name: str,
    duration: dict[str, str],
    min_trace_duration_ms: int,
    limit: int,
    trace_state: str,
) -> dict[str, Any]:
    trace_state = (trace_state or "ALL").upper()
    if trace_state not in TRACE_STATES:
        raise ValueError(f"trace_state 必须是 {sorted(TRACE_STATES)} 之一")
    condition: dict[str, Any] = {
        "queryDuration": duration,
        "traceState": trace_state,
        "queryOrder": "BY_DURATION",
        "paging": {"pageNum": 1, "pageSize": max(1, min(limit, 100)), "needTotal": True},
    }
    if service_name:
        svc = await _resolve_service(service_name, duration)
        condition["serviceId"] = svc["id"]
    if endpoint_name:
        condition["endpointName"] = endpoint_name
    if min_trace_duration_ms > 0:
        condition["minTraceDuration"] = min_trace_duration_ms

    data = await _client.query_basic_traces(condition)
    traces = [
        {
            "trace_id": (t.get("traceIds") or [None])[0],
            "duration_ms": t.get("duration"),
            "start_time": _ts_to_local(t["start"]) if t.get("start") else None,
            "is_error": t.get("isError", False),
            "endpoint_names": t.get("endpointNames"),
        }
        for t in data.get("traces") or []
    ]
    result: dict[str, Any] = {"total": data.get("total", len(traces)), "traces": traces}
    retention = _trace_retention_note(duration)
    if retention:
        result["note"] = retention
    return result


# ---------------------------------------------------------------- MCP tools


@mcp.tool()
async def list_services(keyword: str = "", minutes: int = 10080) -> dict[str, Any]:
    """列出 SkyWalking 服务, 可按关键字过滤。用于把接口 URL/系统映射到具体服务。"""
    try:
        duration = _duration(minutes)
        services = await _client.get_all_services(duration)
        if keyword:
            services = [s for s in services if keyword.lower() in s["name"].lower()]
        return {"total": len(services), "services": services}
    except (SkyWalkingError, ValueError) as e:
        return {"error": _friendly_error(e)}


@mcp.tool()
async def search_endpoints(
    keyword: str,
    service_name: str = "",
    minutes: int = 10080,
    limit: int = 20,
) -> dict[str, Any]:
    """按关键字(通常是接口 URL 路径)搜索端点。指定 service_name 时只搜该服务, 否则并发搜索全部服务。"""
    try:
        duration = _duration(minutes)
        endpoints = await _search_endpoints_impl(keyword, service_name, duration, limit)
        result: dict[str, Any] = {"total": len(endpoints), "endpoints": endpoints}
        if not endpoints:
            result["hint"] = "未找到端点, 可尝试缩短关键字(如只用路径最后一段), 或确认时间范围内有流量"
        return result
    except (SkyWalkingError, ValueError) as e:
        return {"error": _friendly_error(e)}


@mcp.tool()
async def get_endpoint_performance(
    endpoint_name: str,
    service_name: str,
    minutes: int = 10080,
    step: str = "MINUTE",
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    """查询端点性能指标: 平均响应/吞吐量/成功率/百分位(p50~p99), 返回汇总+趋势。

    endpoint_name 须为 Entry 端点(如 {POST}/login/checkToken, 可用 search_endpoints 获取)。
    分钟粒度窗口 >约500 分钟自动降为 HOUR/DAY 桶(吞吐换算为等效每分钟), 传默认 7 天也不会报错。
    start_time/end_time 同时提供时(yyyy-MM-dd HH:mm, 东八区)覆盖 minutes。
    """
    try:
        duration = _duration(minutes, step, start_time, end_time)
        return await _endpoint_performance_impl(endpoint_name, service_name, duration)
    except (SkyWalkingError, ValueError) as e:
        return {"error": _friendly_error(e)}


@mcp.tool()
async def search_slow_traces(
    service_name: str,
    endpoint_name: str = "",
    minutes: int = 10080,
    min_trace_duration_ms: int = 0,
    limit: int = 10,
    trace_state: str = "ALL",
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    """按耗时降序查询链路(trace), 用于找出最慢的请求样本。

    链路通常只保留近 7 天, 查询更早窗口时响应附 note 提示保留期, 避免把 total 偏小
    误判为"没被调用"。min_trace_duration_ms 过滤最小耗时(毫秒, 0 不限)。
    start_time/end_time 同时提供时(yyyy-MM-dd HH:mm, 东八区)覆盖 minutes。
    """
    try:
        duration = _duration(minutes, "SECOND", start_time, end_time)
        return await _search_slow_traces_impl(
            service_name, endpoint_name, duration, min_trace_duration_ms, limit, trace_state
        )
    except (SkyWalkingError, ValueError) as e:
        return {"error": _friendly_error(e)}


@mcp.tool()
async def analyze_trace(trace_id: str) -> dict[str, Any]:
    """分析一条链路的耗时分布: 构建 span 树, 计算每个 span 的自耗时, 找出耗时热点。

    返回: 总耗时/主要耗时路径(critical_path)/自耗时 Top span/空档分析(gaps)/
    按服务与组件聚合/结论(findings)。trace_id 可由 search_slow_traces 获取。
    """
    try:
        spans = await _client.query_trace(trace_id)
        return analysis.analyze_trace_spans(trace_id, spans)
    except (SkyWalkingError, ValueError) as e:
        return {"error": _friendly_error(e)}


@mcp.tool()
async def analyze_endpoint(
    url: str,
    service_name: str = "",
    minutes: int = 10080,
    min_trace_duration_ms: int = 0,
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    """一站式接口分析(主入口): 输入接口 URL, 自动定位端点 → 查询性能指标 → 抓取最慢链路 → 分析耗时热点。

    url 传 URL 或路径(如 /login/checkToken)。start_time/end_time 同时提供时
    (yyyy-MM-dd HH:mm, 东八区)覆盖 minutes; min_trace_duration_ms 过滤慢链路(毫秒, 0 不限)。
    """
    try:
        duration = _duration(minutes, "MINUTE", start_time, end_time)

        # 1. URL -> 候选端点
        path = urlparse(url).path if "://" in url else url.split("?")[0]
        path = path or url
        candidates = await _resolve_endpoint_candidates(path, service_name, duration)
        if not candidates:
            return {
                "error": f"未找到与 '{path}' 匹配的端点",
                "hint": "可先用 search_endpoints 尝试更短的关键字, 或用 list_services 确认服务名",
            }

        # 2. 选出有流量的 Entry 端点并取性能指标
        resolved, performance = await _pick_endpoint_with_traffic(candidates, duration)

        # 3. 最慢链路 + 热点分析
        trace_duration = _duration(minutes, "SECOND", start_time, end_time)
        slow = await _search_slow_traces_impl(
            resolved["service_name"],
            resolved["endpoint_name"],
            trace_duration,
            min_trace_duration_ms,
            5,
            "ALL",
        )
        trace_analysis = None
        if slow["traces"] and slow["traces"][0].get("trace_id"):
            spans = await _client.query_trace(slow["traces"][0]["trace_id"])
            trace_analysis = analysis.analyze_trace_spans(slow["traces"][0]["trace_id"], spans)

        findings = _endpoint_findings(resolved, performance, slow, trace_analysis)
        result = {
            "resolved_endpoint": resolved,
            "candidates": candidates[:10],
            "performance": performance,
            "slowest_traces": slow["traces"],
            "trace_analysis": trace_analysis,
            "findings": findings,
        }
        retention = _trace_retention_note(duration)
        if retention:
            result["note"] = retention
        return result
    except (SkyWalkingError, ValueError) as e:
        return {"error": _friendly_error(e)}


# ------------------------------------------------- analyze_endpoint helpers


def _score_candidate(endpoint_name: str, path: str) -> int:
    """入口型端点(如 {POST}/xxx、GET:/xxx)优先, 网关转发型(Balancer/...)靠后."""
    name_lower = endpoint_name.lower()
    score = 0
    if path.lower() in name_lower:
        score += 2
    if endpoint_name.startswith("{") or ":" in endpoint_name.split("/")[0]:
        score += 1
    if name_lower.startswith("balancer"):
        score -= 2
    return score


async def _resolve_endpoint_candidates(
    path: str, service_name: str, duration: dict[str, str]
) -> list[dict[str, str]]:
    """用完整路径搜索端点, 无结果时退化为更短的路径片段."""
    segments = [seg for seg in path.split("/") if seg]
    keywords = [path]
    if len(segments) >= 2:
        keywords.append("/" + "/".join(segments[-2:]))
    if segments:
        keywords.append("/" + segments[-1])
    seen_keywords = set()
    for keyword in keywords:
        if keyword in seen_keywords:
            continue
        seen_keywords.add(keyword)
        candidates = await _search_endpoints_impl(keyword, service_name, duration, 20)
        if candidates:
            candidates.sort(
                key=lambda c: _score_candidate(c["endpoint_name"], path), reverse=True
            )
            return candidates
    return []


async def _pick_endpoint_with_traffic(
    candidates: list[dict[str, str]], duration: dict[str, str]
) -> tuple[dict[str, str], dict[str, Any]]:
    """在候选端点中选第一个时间范围内有调用量的, 都没有则取评分最高者."""
    fallback: tuple[dict[str, str], dict[str, Any]] | None = None
    for candidate in candidates[:5]:
        perf = await _endpoint_performance_impl(
            candidate["endpoint_name"], candidate["service_name"], duration
        )
        if perf["summary"]["total_calls"] > 0:
            return candidate, perf
        if fallback is None:
            fallback = (candidate, perf)
    assert fallback is not None
    return fallback


def _endpoint_findings(
    resolved: dict[str, str],
    performance: dict[str, Any],
    slow: dict[str, Any],
    trace_analysis: dict[str, Any] | None,
) -> list[str]:
    findings: list[str] = [
        f"接口解析为服务 {resolved['service_name']} 的端点 {resolved['endpoint_name']}"
    ]
    summary = performance["summary"]
    if summary["total_calls"] > 0:
        pct_desc = ""
        if summary.get("percentiles"):
            p99 = summary["percentiles"].get("p99")
            if p99:
                pct_desc = f", p99 {p99['avg_ms']}ms(峰值 {p99['max_ms']}ms)"
        findings.append(
            f"时间范围内共调用 {summary['total_calls']} 次, 平均响应 {summary['avg_resp_time_ms']}ms"
            f", 成功率 {summary['success_rate_pct']}%{pct_desc}"
        )
    else:
        findings.append("时间范围内该端点无调用量, 指标为空")
    if slow["traces"]:
        slowest = slow["traces"][0]
        findings.append(
            f"最慢链路耗时 {slowest['duration_ms']}ms (trace_id={slowest['trace_id']})"
        )
    if trace_analysis and trace_analysis.get("findings"):
        findings.extend(trace_analysis["findings"])
    return findings


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
