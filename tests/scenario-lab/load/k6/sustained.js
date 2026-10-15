import { check, sleep } from "k6";
import http from "k6/http";

const baseUrl = __ENV.TARGET_URL || "http://127.0.0.1:8080";
const runId = `load-${Math.random().toString(16).slice(2)}`;
const rateRun = `${runId}-rate`;
const injectPartition = __ENV.ENABLE_RUVIO_PARTITION === "true";
const faultControlUrl = __ENV.FAULT_CONTROL_URL || "";
const faultDuration = injectPartition ? parseDuration(__ENV.FAULT_DURATION || "30s") : 0;
const faultAfter = injectPartition ? parseDuration(__ENV.FAULT_AFTER || "5m") : 0;
const testDuration = injectPartition ? parseDuration(__ENV.DURATION || "30m") : 0;
const p95CatalogThreshold = injectPartition ? "p(95)<2500" : "p(95)<250";
const p95OrdersThreshold = injectPartition ? "p(95)<2500" : "p(95)<500";
const p99LatencyThreshold = injectPartition ? "p(99)<12000" : "p(99)<750";

if (injectPartition && faultAfter + faultDuration >= testDuration) {
  throw new Error("DURATION must extend beyond FAULT_AFTER plus FAULT_DURATION.");
}

export const options = {
  scenarios: {
    catalog_reads: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.CATALOG_RPS || 80),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30m",
      preAllocatedVUs: 20,
      maxVUs: 100,
      exec: "catalogReads",
    },
    order_retries: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.ORDER_RPS || 5),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30m",
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: "orderRetries",
    },
    rate_limit_probes: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE_PROBE_RPS || 5),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30m",
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: "rateLimitProbes",
    },
    ...(injectPartition
      ? {
          ruvio_partition: {
            executor: "per-vu-iterations",
            vus: 1,
            iterations: 1,
            startTime: `${faultAfter}s`,
            exec: "partitionRuvio",
          },
        }
      : {}),
  },
  thresholds: {
    checks: [injectPartition ? "rate>0.9" : "rate>0.99"],
    http_req_failed: [injectPartition ? "rate<0.15" : "rate<0.02"],
    "http_req_duration{endpoint:catalog}": [p95CatalogThreshold, p99LatencyThreshold],
    "http_req_duration{endpoint:orders}": [p95OrdersThreshold, p99LatencyThreshold],
  },
};

export function catalogReads() {
  const response = http.get(`${baseUrl}/api/catalog/lab-widget`, {
    tags: { endpoint: "catalog" },
  });
  check(response, { "catalog returns 200": (r) => r.status === 200 });
}

export function orderRetries() {
  const key = `order-${Math.random().toString(16).slice(2)}-${__VU}-${__ITER}`;
  const params = {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": key,
      "X-Lab-Run": runId,
    },
    tags: { endpoint: "orders" },
  };
  const payload = JSON.stringify({ sku: "lab-widget", quantity: 1 });
  const first = http.post(`${baseUrl}/api/orders`, payload, params);
  const replay = http.post(`${baseUrl}/api/orders`, payload, params);

  check([first, replay], {
    "order was created": (responses) => responses[0].status === 201,
    "retry replays the created order": (responses) =>
      responses[1].status === 201 && responses[1].body === responses[0].body,
  });
}

export function rateLimitProbes() {
  const response = http.get(`${baseUrl}/api/rate-limited`, {
    headers: { "X-Lab-Run": rateRun },
    tags: { endpoint: "rate_limit" },
  });
  check(response, {
    "rate limiter returns accepted or limited": (r) => r.status === 200 || r.status === 429,
  });
  sleep(0.01);
}

export function partitionRuvio() {
  if (!faultControlUrl) {
    throw new Error("FAULT_CONTROL_URL is required when Ruvio partition injection is enabled.");
  }

  const controlUrl = `${faultControlUrl.replace(/\/$/, "")}/proxies/ruvio`;
  const headers = { "Content-Type": "application/json" };
  const disable = http.post(controlUrl, JSON.stringify({ enabled: false }), { headers });
  check(disable, { "Toxiproxy accepted the partition": (r) => r.status === 200 });

  sleep(faultDuration);

  const enable = http.post(controlUrl, JSON.stringify({ enabled: true }), { headers });
  check(enable, { "Toxiproxy restored the Ruvio connection": (r) => r.status === 200 });

  sleep(3);
  const readiness = http.get(`${baseUrl}/health/ready`);
  check(readiness, {
    "ASP.NET Core reconnected to Ruvio after the partition": (r) =>
      r.status === 200 && r.json("ruvio") === "PONG",
  });
}

function parseDuration(value) {
  const match = /^([1-9][0-9]*)(s|m|h)$/.exec(value);
  if (!match) {
    throw new Error("FAULT_DURATION must be a positive integer followed by s, m, or h.");
  }

  const amount = Number(match[1]);
  return amount * ({ s: 1, m: 60, h: 3600 }[match[2]]);
}
