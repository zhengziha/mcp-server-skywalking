"""链路(Trace)分析: 构建全局 span 树, 计算自耗时并找出耗时热点.

纯函数实现, 不依赖网络, 便于单独测试.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

TAG_VALUE_MAX_LEN = 200
TOP_SPAN_LIMIT = 10
FINDING_SELF_PCT_THRESHOLD = 10.0
# 空档(gap)分析: 某 span 内部未被任何"子 span"覆盖的区间, 通常对应未埋点/本地处理/等待。
GAP_MIN_MS = 2
GAP_REPORT_LIMIT = 8
GAP_FINDING_MIN_MS = 20


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


def _merged_child_intervals(node: dict[str, Any]) -> list[tuple[int, int]]:
    """子 span 区间(裁剪进 node 自身区间)合并去重后的覆盖段, 升序."""
    intervals = sorted(
        (max(c["startTime"], node["startTime"]), min(c["endTime"], node["endTime"]))
        for c in node["children"]
    )
    merged: list[tuple[int, int]] = []
    for s, e in intervals:
        if e <= s:
            continue
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def _compute_self_time(node: dict[str, Any]) -> None:
    """自耗时 = 自身时长 - 子 span 区间在自身区间内的覆盖时长(区间合并去重)."""
    covered = sum(e - s for s, e in _merged_child_intervals(node))
    node["self_time"] = max(0, node["duration"] - covered)


def _span_label(node: dict[str, Any]) -> str:
    endpoint = node.get("endpointName")
    return f"{node.get('serviceCode')}/{endpoint or node.get('spanId')}"


def _node_gap_report(node: dict[str, Any]) -> list[dict[str, Any]]:
    """单个 span 的空档: 该 span 区间内未被其任何子 span 覆盖的窗口.

    空档区域 = span 自身执行但没有任何子调用/探测点的区间, 通常对应本地处理、
    未埋点代码或等待; leading=起点到第一个子 span, trailing=最后一个子 span 到结束。
    """
    if not node["children"]:
        return []  # 叶子 span 的自身耗时不算"空档"
    s0, e0 = node["startTime"], node["endTime"]
    if s0 is None or e0 is None or node["duration"] <= 0:
        return []
    gaps: list[tuple[int, int]] = []
    cursor = s0
    for cs, ce in _merged_child_intervals(node):
        if cs > cursor:
            gaps.append((cursor, cs))
        cursor = max(cursor, ce)
    if cursor < e0:
        gaps.append((cursor, e0))

    label = _span_label(node)
    out: list[dict[str, Any]] = []
    for gs, ge in gaps:
        dur = ge - gs
        if dur < GAP_MIN_MS:
            continue
        # 相邻"子 span"锚点: after=结束 <= gs 中结束最晚, before=开始 >= ge 中开始最早
        after: tuple[dict[str, Any], int] | None = None
        before: tuple[dict[str, Any], int] | None = None
        for ch in node["children"]:
            chs, che = ch["startTime"], ch["endTime"]
            if chs is None or che is None:
                continue
            if che <= gs and (after is None or che > after[1]):
                after = (ch, che)
            if chs >= ge and (before is None or chs < before[1]):
                before = (ch, chs)
        after_desc = f"{_span_label(after[0])} 结束" if after else f"{label} 起点"
        before_desc = (
            f"{_span_label(before[0])} 开始" if before else f"{label} 结束"
        )
        if gs <= s0:
            region = "leading"
        elif ge >= e0:
            region = "trailing"
        else:
            region = "inner"
        out.append(
            {
                "parent": label,
                "region": region,
                "start_time": gs,
                "end_time": ge,
                "duration_ms": dur,
                "after": after_desc,
                "before": before_desc,
            }
        )
    return out


def _collect_gaps(
    nodes: dict[tuple[str, int], dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    """汇总所有父 span 的空档.

    Returns (按时长降序的报告Top, 全部符合条件空档的总个数, 总时长)。
    各父 span 的空档区间在时间轴互不重叠（父级 gap 位于其子 span 覆盖范围之外，
    而孙级 gap 又都落在这些子 span 内部），因此总时长可直接累加无重复。
    """
    reports: list[dict[str, Any]] = []
    total_count = 0
    total_uncovered = 0
    for node in nodes.values():
        if not node["children"]:
            continue
        for gap in _node_gap_report(node):
            reports.append(gap)
            total_count += 1
            total_uncovered += gap["duration_ms"]
    reports.sort(key=lambda g: g["duration_ms"], reverse=True)
    return reports[:GAP_REPORT_LIMIT], total_count, total_uncovered


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

    # 空档分析: 各父 span 内未被任何子 span 覆盖的时间窗口(未埋点/本地处理/等待)
    gap_report, gap_count, gap_total_ms = _collect_gaps(nodes)
    gap_stats: dict[str, Any] | None = None
    if gap_report:
        gap_stats = {
            "gap_count": gap_count,
            "reported": len(gap_report),
            "reported_uncovered_ms": gap_total_ms,
            "pct": _pct(gap_total_ms, total_duration),
        }

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
    if gap_report and gap_report[0]["duration_ms"] >= GAP_FINDING_MIN_MS:
        big = gap_report[0]
        findings.insert(
            0,
            f"时间空档: {big['parent']} 内 {big['after']} 到 {big['before']} 之间存在 "
            f"{big['duration_ms']}ms 未被任何子 span 覆盖(占全链路 {_pct(big['duration_ms'], total_duration)}%),"
            f" 疑似本地处理/未埋点代码/等待, 建议用日志毫秒时间戳人工对表",
        )

    return {
        "trace_id": trace_id,
        "total_duration_ms": total_duration,
        "span_count": len(nodes),
        "service_count": len(service_agg),
        "has_error": bool(errors),
        "critical_path": [_span_brief(n, total_duration) for n in critical],
        "top_self_time_spans": top_spans,
        "gaps": gap_report or None,
        "gap_stats": gap_stats,
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
