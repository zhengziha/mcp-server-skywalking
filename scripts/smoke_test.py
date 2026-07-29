"""针对线上 SkyWalking 环境的全链路自测脚本: 依次调用 6 个 MCP 工具函数."""

import asyncio
import json
import sys

sys.path.insert(0, "src")

from skywalking_mcp import server


def show(title, data, max_len=1200):
    text = json.dumps(data, ensure_ascii=False, default=str)
    print(f"\n===== {title} =====")
    print(text[:max_len] + ("..." if len(text) > max_len else ""))
    assert "error" not in data, f"{title} 返回错误: {data.get('error')}"


async def main():
    r1 = await server.list_services(keyword="auth")
    show("list_services(keyword=auth)", r1)
    assert any(s["name"] == "ddjk-auth-server" for s in r1["services"])

    r2 = await server.search_endpoints("/login/checkToken", service_name="ddjk-auth-server")
    show("search_endpoints(/login/checkToken @ ddjk-auth-server)", r2)
    assert r2["total"] > 0

    r3 = await server.get_endpoint_performance(
        "{POST}/login/checkToken", "ddjk-auth-server", minutes=60
    )
    show("get_endpoint_performance", r3, max_len=800)
    assert r3["summary"]["total_calls"] > 0, "端点指标应有调用量"

    r4 = await server.search_slow_traces("ddjk-auth-server", minutes=30, limit=3)
    show("search_slow_traces", r4)
    assert r4["traces"], "应能查到链路"

    trace_id = r4["traces"][0]["trace_id"]
    r5 = await server.analyze_trace(trace_id)
    show(f"analyze_trace({trace_id})", r5, max_len=2000)
    assert r5["span_count"] > 0 and r5["findings"]

    r6 = await server.analyze_endpoint("/login/checkToken", minutes=60)
    show("analyze_endpoint(/login/checkToken)", r6, max_len=2500)
    assert r6["resolved_endpoint"]["endpoint_name"]
    assert r6["performance"]["summary"]["total_calls"] > 0

    print("\n所有工具自测通过")


if __name__ == "__main__":
    asyncio.run(main())
