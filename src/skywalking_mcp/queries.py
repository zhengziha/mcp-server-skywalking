"""SkyWalking 8.0 GraphQL 查询语句常量(字段名以线上实测为准)."""

GET_ALL_SERVICES = """
query ($duration: Duration!) {
  getAllServices(duration: $duration) {
    id
    name
  }
}
"""

SEARCH_ENDPOINT = """
query ($keyword: String!, $serviceId: ID!, $limit: Int!) {
  searchEndpoint(keyword: $keyword, serviceId: $serviceId, limit: $limit) {
    id
    name
  }
}
"""

READ_METRICS_VALUES = """
query ($condition: MetricsCondition!, $duration: Duration!) {
  readMetricsValues(condition: $condition, duration: $duration) {
    label
    values {
      values {
        value
      }
    }
  }
}
"""

READ_LABELED_METRICS_VALUES = """
query ($condition: MetricsCondition!, $labels: [String!]!, $duration: Duration!) {
  readLabeledMetricsValues(condition: $condition, labels: $labels, duration: $duration) {
    label
    values {
      values {
        value
      }
    }
  }
}
"""

QUERY_BASIC_TRACES = """
query ($condition: TraceQueryCondition) {
  queryBasicTraces(condition: $condition) {
    traces {
      segmentId
      endpointNames
      duration
      start
      isError
      traceIds
    }
    total
  }
}
"""

QUERY_TRACE = """
query ($traceId: ID!) {
  queryTrace(traceId: $traceId) {
    spans {
      traceId
      segmentId
      spanId
      parentSpanId
      refs {
        traceId
        parentSegmentId
        parentSpanId
        type
      }
      serviceCode
      startTime
      endTime
      endpointName
      type
      peer
      component
      isError
      layer
      tags {
        key
        value
      }
    }
  }
}
"""
