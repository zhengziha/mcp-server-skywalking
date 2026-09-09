# SkyWalking MCP Server

基于 [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) 的 Apache SkyWalking 查询服务,让大模型能够直接查询线上接口的性能指标、抓取慢链路并分析链路耗时热点,所有结果以结构化 JSON 返回,可供大模型进行性能分析与瓶颈定位。

## 功能特性

- **接口性能查询**:按接口 URL 查询平均响应时间、吞吐量(CPM)、成功率、百分位耗时(p50/p75/p90/p95/p99),含汇总与分钟级趋势
- **慢链路检索**:按耗时降序检索链路(trace),支持按服务/端点/最小耗时过滤
- **链路耗时分析**:跨 segment 构建全局 span 树,计算每个 span 的自耗时(自身时长减去子调用覆盖时长),自动找出耗时热点
- **一站式分析**:输入接口 URL 即可自动完成 端点定位 → 性能查询 → 慢链路抓取 → 热点分析 全流程
- **灵活时间范围**:支持"最近 N 分钟"相对窗口与 `start_time`/`end_time` 绝对时间段(用于回溯历史故障)

## 环境要求

- Python 3.10+
- SkyWalking OAP 8.0(GraphQL 查询协议),默认地址 `http://10.0.26.41:8080`

## 安装

```bash
cd mcp-server-skywalking
python3 -m venv .venv
.venv/bin/pip install -e .
```

## 配置

通过环境变量配置(参考 `.env.example`):

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `SKYWALKING_URL` | `http://10.0.26.41:8080` | SkyWalking OAP/UI 地址(GraphQL 端点为 `{URL}/graphql`) |
| `SKYWALKING_TZ` | `Asia/Shanghai` | OAP 服务端时区,必须与服务端一致,否则指标时间桶对不上 |
| `SKYWALKING_TIMEOUT` | `15` | HTTP 请求超时(秒) |

## MCP 客户端接入

在 MCP 客户端(Qoder / Claude Desktop / Cursor 等)中添加 stdio 配置:

```json
{
  "mcpServers": {
    "skywalking": {
      "command": "/绝对路径/mcp-server-skywalking/.venv/bin/skywalking-mcp",
      "env": {
        "SKYWALKING_URL": "http://10.0.26.41:8080",
        "SKYWALKING_TZ": "Asia/Shanghai"
      }
    }
  }
}
```

也可以用 `python -m skywalking_mcp` 方式启动:

```json
{
  "mcpServers": {
    "skywalking": {
      "command": "/绝对路径/mcp-server-skywalking/.venv/bin/python",
      "args": ["-m", "skywalking_mcp"]
    }
  }
}
```

## 工具列表

> 时间参数约定:所有含时间范围的工具默认使用 `minutes=30`(最近 N 分钟);`get_endpoint_performance` / `search_slow_traces` / `analyze_endpoint` 额外支持 `start_time` / `end_time`(格式 `yyyy-MM-dd HH:mm`,东八区),同时提供时覆盖 `minutes`,用于回溯历史时间段。

### 1. `analyze_endpoint` — 一站式接口分析(主入口)

输入接口 URL,自动完成:定位 Entry 端点 → 查询性能指标 → 抓取最慢链路 → 分析耗时热点。

| 参数 | 类型 | 说明 |
|------|------|------|
| `url` | string | 接口 URL 或路径,如 `/login/checkToken` |
| `service_name` | string | 可选,已知所属服务时可加速定位 |
| `minutes` | int | 相对时间窗口,默认 30 |
| `min_trace_duration_ms` | int | 慢链路过滤阈值(毫秒),0 不限制 |
| `start_time` / `end_time` | string | 可选绝对时间段 |

返回:`resolved_endpoint`(解析到的端点)、`performance`(性能汇总+趋势)、`slowest_traces`(最慢链路列表)、`trace_analysis`(热点分析)、`findings`(可读结论)。

### 2. `list_services` — 列出服务

按名称关键字过滤 SkyWalking 中的服务,用于把接口/系统映射到具体服务。

### 3. `search_endpoints` — 搜索端点

按关键字(接口路径)搜索端点。指定 `service_name` 只搜该服务,否则并发搜索全部服务。

### 4. `get_endpoint_performance` — 端点性能指标

返回结构化性能数据:

```json
{
  "summary": {
    "avg_resp_time_ms": 18.8,
    "throughput_cpm_avg": 122.31,
    "total_calls": 7216,
    "success_rate_pct": 100.0,
    "percentiles": {"p50": {...}, "p75": {...}, "p90": {...}, "p95": {...}, "p99": {"avg_ms": 354.06, "max_ms": 990}}
  },
  "trend": {"timestamps": [...], "throughput_cpm": [...], "avg_resp_time_ms": [...], "percentiles": {...}}
}
```

> **自动降粒度**:分钟(MINUTE)桶窗口超过约 500 分钟时,自动升级为 HOUR/DAY 粒度并把吞吐换算成
> 等效每分钟口径(响应带 `note` 说明)。直接传默认 7 天不会再把 OAP 的原始 GraphQL 错误抛给模型。

### 5. `search_slow_traces` — 检索慢链路

按耗时降序返回链路样本:`[{trace_id, duration_ms, start_time, is_error, endpoint_names}]`。

> **保留期提示**:链路(span)通常只保留近 N 天(默认 7,可用 `SKYWALKING_TRACE_RETENTION_DAYS` 调整)。
> 查询更早窗口时响应会带 `note` 提示保留期,避免把 `total` 偏小误判成"这段时间真没被调用过"。

### 6. `analyze_trace` — 链路耗时热点分析

对单条链路做结构化分析:

- `total_duration_ms` / `span_count` / `service_count` / `has_error`:链路概览
- `critical_path`:主要耗时路径(从根 span 逐层选择耗时最长的子 span)
- `top_self_time_spans`:自耗时 Top 10 的 span(含服务、组件、peer、耗时占比、tags)
- `gaps` / `gap_stats`:**空档分析** —— 每个父 span 内未被任何子 span 覆盖的时间区间
  (含起止时刻、时长、前后相邻 span),对应未埋点/本地处理/等待;最大的空档会进入 `findings`
- `by_service`:按服务聚合自耗时
- `by_layer_component`:按 Layer/组件聚合(定位 DB/Cache/HTTP/MQ 类耗时)
- `errors`:错误 span 列表
- `findings`:可读结论,如"耗时热点: 服务 X 的 [Mysql] SELECT... 自身耗时 800ms, 占整条链路 45%"、空档定位

## 使用示例

配置好 MCP 客户端后,直接向大模型提问:

- "分析一下 `/login/checkToken` 这个接口的性能瓶颈"
- "查一下 ddjk-auth-server 最近 1 小时最慢的 5 条链路"
- "昨天 14:00 到 15:00 `/order/create` 接口的 p99 是多少?"
- "分析 trace `56f79c5d...` 的耗时分布,哪一步最慢?"

## 项目结构

```
mcp-server-skywalking/
├── pyproject.toml               # 依赖与打包配置, 提供 skywalking-mcp 命令
├── .env.example                 # 环境变量示例
├── scripts/
│   └── smoke_test.py            # 针对线上环境的全链路自测脚本
└── src/skywalking_mcp/
    ├── config.py                # 环境变量配置
    ├── queries.py               # SkyWalking 8.0 GraphQL 查询语句
    ├── client.py                # 异步 GraphQL 客户端 + Duration 时间窗口构造
    ├── analysis.py              # 链路树构建与自耗时/热点分析(纯函数)
    └── server.py                # MCP 工具注册与 stdio 启动入口
```

## 本地自测

```bash
.venv/bin/python scripts/smoke_test.py
```

脚本会依次调用 6 个工具并对真实数据做断言,输出 `所有工具自测通过` 即为正常。

## 实现要点(避坑)

- **端点指标只在 Entry 端点所在服务上产生**:网关的 `Balancer/...` 是 Local span,查不到指标;必须定位到下游真正处理请求的 Entry 端点(如 `{POST}/login/checkToken`)。`analyze_endpoint` 已内置该解析逻辑。
- **Duration 时间格式与 step 严格对应**:`MINUTE` 为 `yyyy-MM-dd HHmm`(分钟不带冒号),`HOUR` 为 `yyyy-MM-dd HH`,`DAY` 为 `yyyy-MM-dd`。
- **时区必须与 OAP 服务端一致**:用 UTC 传参不会报错,但指标时间桶对不上会全为 0。
- **指标单位**:`endpoint_sla` 为万分比(10000 = 100%),`endpoint_avg` 为毫秒,`endpoint_percentile` 标签 `0..4` 依次对应 p50/p75/p90/p95/p99。
