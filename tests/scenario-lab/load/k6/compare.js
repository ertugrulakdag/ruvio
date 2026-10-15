import { check, group } from "k6";
import exec from "k6/execution";
import http from "k6/http";
import { Rate } from "k6/metrics";

const baseUrl = __ENV.TARGET_URL || "http://127.0.0.1:8080";
const rate = Number(__ENV.COMPARE_RATE_RPS || 200);
const stageSeconds = Number(__ENV.COMPARE_STAGE_SECONDS || 300);
const gapSeconds = Number(__ENV.COMPARE_GAP_SECONDS || 60);
const preAllocatedVUs = Number(__ENV.COMPARE_PREALLOCATED_VUS || 500);
const maxVUs = Number(__ENV.COMPARE_MAX_VUS || 2500);
const runId = __ENV.TEST_RUN || `compare-${Date.now()}`;
const requestSuccess = new Rate("scenario_request_success");

if (!Number.isInteger(rate) || rate < 1 || rate > 1000) {
  throw new Error("COMPARE_RATE_RPS must be between 1 and 1000 iterations per second.");
}
if (!Number.isInteger(stageSeconds) || stageSeconds < 30 || stageSeconds > 600) {
  throw new Error("COMPARE_STAGE_SECONDS must be between 30 and 600 seconds.");
}
if (!Number.isInteger(gapSeconds) || gapSeconds < 0 || gapSeconds > 600) {
  throw new Error("COMPARE_GAP_SECONDS must be between 0 and 600 seconds.");
}
if (!Number.isInteger(preAllocatedVUs) || !Number.isInteger(maxVUs) ||
    preAllocatedVUs < 1 || maxVUs < preAllocatedVUs || maxVUs > 5000) {
  throw new Error("Comparison VUs must satisfy 1 <= preallocated <= max <= 5000.");
}

const selectedBackend = __ENV.COMPARE_BACKEND || "both";
if (!["both", "ruvio", "redis"].includes(selectedBackend)) {
  throw new Error("COMPARE_BACKEND must be both, ruvio, or redis.");
}
const backendScenarios = selectedBackend === "both" ? ["ruvio", "redis"] : [selectedBackend];
const scenarios = Object.fromEntries(backendScenarios.map((backend, index) => [
  `${backend}_comparison`,
  {
    executor: "constant-arrival-rate",
    exec: "mixedWorkload",
    startTime: `${index * (stageSeconds + gapSeconds)}s`,
    rate,
    timeUnit: "1s",
    duration: `${stageSeconds}s`,
    preAllocatedVUs,
    maxVUs,
    tags: { backend, load_stage: "steady" },
  },
]));

http.setResponseCallback(http.expectedStatuses(200, 201, 204, 409, 425, 429));

export const options = {
  scenarios,
  discardResponseBodies: false,
  thresholds: {
    scenario_request_success: ["rate>0.98"],
    checks: ["rate==1"],
    ...Object.fromEntries(backendScenarios.flatMap((backend) => [
      [`http_req_duration{backend:${backend}}`, ["p(95)<1000"]],
      [`scenario_request_success{backend:${backend}}`, ["rate==1"]],
    ])),
  },
};

function requestId(backend, suffix) {
  return `${runId}-${backend}-${exec.scenario.name}-${__VU}-${__ITER}-${suffix}`;
}

function send(backend, operation, method, path, body, extraHeaders = {}) {
  const id = requestId(backend, operation);
  const params = {
    headers: {
      "X-Lab-Backend": backend,
      "X-Lab-Request-ID": id,
      "X-Lab-Test-Run": runId,
      "X-Lab-Stage": "steady",
      "X-Lab-Run": `${runId}-${backend}-${__VU}`,
      ...extraHeaders,
    },
    tags: { backend, operation, name: operation },
    timeout: "30s",
  };
  const response = method === "GET"
    ? http.get(`${baseUrl}${path}`, params)
    : http.post(`${baseUrl}${path}`, body, params);
  requestSuccess.add(response.status > 0 && response.status < 500, { backend, operation });

  if (response.status >= 500 || response.timings.duration >= 1000) {
    const traceId = Object.entries(response.headers)
      .find(([name]) => name.toLowerCase() === "x-lab-trace-id")?.[1] || "";
    console.log(JSON.stringify({
      event: "slow-or-failed-request",
      run_id: runId,
      backend,
      operation,
      request_id: id,
      trace_id: traceId,
      status: response.status,
      duration_ms: response.timings.duration,
    }));
  }
  return response;
}

export function mixedWorkload() {
  const backend = exec.scenario.name.startsWith("redis_") ? "redis" : "ruvio";
  const choice = Math.random();
  if (choice < 0.4) {
    group("output-cache", () => {
      const response = send(backend, "output_cache", "GET", `/bench/catalog/stress-${__VU % 100}`);
      check(response, { "catalog request succeeds": (result) => result.status === 200 });
    });
    return;
  }

  if (choice < 0.7) {
    group("idempotency", () => {
      const id = requestId(backend, "idempotency");
      const headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": id,
      };
      const payload = JSON.stringify({ sku: "lab-widget", quantity: 1 });
      const first = send(backend, "idempotency_create", "POST", "/bench/orders", payload, headers);
      const replay = send(backend, "idempotency_replay", "POST", "/bench/orders", payload, headers);
      check([first, replay], {
        "first order and replay succeed": (responses) =>
          responses[0].status === 201 &&
          responses[1].status === 201 &&
          responses[0].body === responses[1].body,
      });
    });
    return;
  }

  group("rate-limiting", () => {
    const response = send(backend, "rate_limit", "GET", "/bench/rate-limited");
    check(response, {
      "rate-limit response is accepted or rejected": (result) =>
        result.status === 200 || result.status === 429,
    });
  });
}
