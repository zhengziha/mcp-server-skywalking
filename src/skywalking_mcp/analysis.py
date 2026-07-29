"""链路(Trace)分析: 构建全局 span 树, 计算自耗时并找出耗时热点.

纯函数实现, 不依赖网络, 便于单独测试.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

TAG_VALUE_MAX_LEN = 200
TOP_SPAN_LIMIT = 10
FINDING_SELF_PCT_THRESHOLD = 10.0


def _span_key(segment_id: str, span_id: int) -> tuple[str, int]:
    return (segment_id, span_id)


def _build_nodes(spans: list[dict[str, Any]]) -> tuple[dict, list[dict]]:
    """索引 span 并建立父子关系: segment 内用 parentSpanId, 跨 segment 用 refs."""
    nodes: dict[tuple[str, int], dict[str, Any]] = {}
    for span in spans:
        node = dict(span)
        node["children"] = []
        node["duration"] = max(0, (span.get("endTime") or 0) - (span.get("startTime") or 0))
        nodes[_span_key(span["segmentId"], span["spanId"])] = node

    roots: list[dict[str, Any]] = []
    for node in nodes.values():
        parent = None
        if node.get("parentSpanId", -1) >= 0:
            parent = nodes.get(_span_key(node["segmentId"], node["parentSpanId"]))
        if parent is None:
            for ref in node.get("refs") or []:
                parent = nodes.get(_span_key(ref["parentSegmentId"], ref["parentSpanId"]))
                if parent is not None:
                    break
        if parent is not None and parent is not node:
            parent["children"].append(node)
            node["_has_parent"] = True
        else:
            roots.append(node)
    return nodes, roots


def _compute_self_time(node: dict[str, Any]) -> None:
    """自耗时 = 自身时长 - 子 span 区间在自身区间内的覆盖时长(区间合并去重)."""
    intervals = sorted(
        (max(c["startTime"], node["startTime"]), min(c["endTime"], node["endTime"]))
        for c in node["children"]
    )
    covered = 0
    cur_start = cur_end = None
    for s, e in intervals:
        if e <= s:
            continue
        if cur_start is None:
            cur_start, cur_end = s, e
        elif s <= cur_end:
            cur_end = max(cur_end, e)
        else:
            covered += cur_end - cur_start
            cur_start, cur_end = s, e
    if cur_start is not None:
        covered += cur_end - cur_start
    node["self_time"] = max(0, node["duration"] - covered)


def _critical_path(root: dict[str, Any]) -> list[dict[str, Any]]:
    """从根 span 出发, 每层选择耗时最长的子 span, 得到主要耗时路径."""
    path = []
    node = root
    seen = set()
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        path.append(node)
        node = max(node["children"], key=lambda c: c["duration"], default=None)
    return path


def _pct(part: float, total: float) -> float:
    return round(part * 100.0 / total, 2) if total > 0 else 0.0


def _span_brief(node: dict[str, Any], total_duration: int) -> dict[str, Any]:
    tags = {
        t["key"]: (t.get("value") or "")[:TAG_VALUE_MAX_LEN]
        for t in (node.get("tags") or [])
    }
    return {
        "service": node.get("serviceCode"),
        "endpoint": node.get("endpointName"),
        "span_type": node.get("type"),
        "component": node.get("component"),
        "layer": node.get("layer"),
        "peer": node.get("peer") or None,
        "start_time": node.get("startTime"),
        "duration_ms": node["duration"],
        "self_time_ms": node["self_time"],
        "self_pct": _pct(node["self_time"], total_duration),
        "is_error": node.get("isError", False),
        "tags": tags or None,
    }


def analyze_trace_spans(trace_id: str, spans: list[dict[str, Any]]) -> dict[str, Any]:
    """对一条完整链路做结构化耗时分析, 输出可供大模型推理的报告."""
    if not spans:
        return {"trace_id": trace_id, "error": "链路不存在或没有 span 数据"}

    nodes, roots = _build_nodes(spans)
    for node in nodes.values():
        _compute_self_time(node)

    all_start = min(n["startTime"] for n in nodes.values())
    all_end = max(n["endTime"] for n in nodes.values())
    total_duration = max(1, all_end - all_start)

    # 主要耗时路径: 从最长的根 span 出发
    main_root = max(roots, key=lambda n: n["duration"], default=None)
    critical = _critical_path(main_root) if main_root else []

    # 按自耗时排序找热点 span
    by_self = sorted(nodes.values(), key=lambda n: n["self_time"], reverse=True)
    top_spans = [_span_brief(n, total_duration) for n in by_self[:TOP_SPAN_LIMIT]]

    # 按服务聚合自耗时
    service_agg: dict[str, int] = defaultdict(int)
    for n in nodes.values():
        service_agg[n.get("serviceCode") or "unknown"] += n["self_time"]
    by_service = [
        {"service": svc, "self_time_ms": ms, "pct": _pct(ms, total_duration)}
        for svc, ms in sorted(service_agg.items(), key=lambda kv: kv[1], reverse=True)
    ]

    # 按 layer/component 聚合(定位 DB/Cache/HTTP/MQ 类耗时)
    layer_agg: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"ms": 0, "count": 0})
    for n in nodes.values():
        key = (n.get("layer") or "Unknown", n.get("component") or "Unknown")
        layer_agg[key]["ms"] += n["self_time"]
        layer_agg[key]["count"] += 1
    by_layer_component = [
        {
            "layer": layer,
            "component": component,
            "span_count": agg["count"],
            "self_time_ms": agg["ms"],
            "pct": _pct(agg["ms"], total_duration),
        }
        for (layer, component), agg in sorted(
            layer_agg.items(), key=lambda kv: kv[1]["ms"], reverse=True
        )
    ]

    errors = [_span_brief(n, total_duration) for n in nodes.values() if n.get("isError")]

    findings = _build_findings(top_spans, by_service, by_layer_component, errors, total_duration)

    return {
        "trace_id": trace_id,
        "total_duration_ms": total_duration,
        "span_count": len(nodes),
        "service_count": len(service_agg),
        "has_error": bool(errors),
        "critical_path": [_span_brief(n, total_duration) for n in critical],
        "top_self_time_spans": top_spans,
        "by_service": by_service,
        "by_layer_component": by_layer_component,
        "errors": errors,
        "findings": findings,
    }


def _build_findings(
    top_spans: list[dict[str, Any]],
    by_service: list[dict[str, Any]],
    by_layer_component: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    total_duration: int,
) -> list[str]:
    """生成可读结论, 帮助大模型快速抓住耗时热点."""
    findings: list[str] = []
    for span in top_spans:
        if span["self_pct"] < FINDING_SELF_PCT_THRESHOLD:
            break
        peer = f", 目标 {span['peer']}" if span.get("peer") else ""
        findings.append(
            f"耗时热点: 服务 {span['service']} 的 [{span['component']}] {span['endpoint']}"
            f"{peer} 自身耗时 {span['self_time_ms']}ms, 占整条链路 {span['self_pct']}%"
        )
    if by_service:
        top_svc = by_service[0]
        findings.append(
            f"服务维度: {top_svc['service']} 自身耗时合计 {top_svc['self_time_ms']}ms,"
            f" 占比 {top_svc['pct']}% 最高"
        )
    for item in by_layer_component:
        if item["layer"] in ("Database", "Cache", "MQ") and item["pct"] >= FINDING_SELF_PCT_THRESHOLD:
            findings.append(
                f"{item['layer']} 访问({item['component']}) 共 {item['span_count']} 次,"
                f" 合计耗时 {item['self_time_ms']}ms, 占比 {item['pct']}%, 关注慢查询或 N+1 调用"
            )
    for span in errors:
        findings.append(
            f"错误 span: 服务 {span['service']} 的 {span['endpoint']} ({span['component']}) 发生异常"
        )
    if not findings:
        findings.append(f"链路总耗时 {total_duration}ms, 无单点占比超过 {FINDING_SELF_PCT_THRESHOLD}% 的显著热点")
    return findings
