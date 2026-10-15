import { check, group } from "k6";
import exec from "k6/execution";
import http from "k6/http";

const baseUrl = __ENV.TARGET_URL || "http://127.0.0.1:8080";
const runId = `stress-${Date.now()}`;
const stageNames = [
  "baseline",
  "ramp-100",
  "ramp-200",
  "ramp-400",
  "ramp-600",
  "ramp-800",
  "peak-1200",
  "peak-1600",
  "peak-2000",
  "peak-2400",
  "peak-3000",
  "stepdown-300",
  "recovery-1600",
  "repeak-3000",
];
const defaultRates = [50, 100, 200, 400, 600, 800, 1200, 1600, 2000, 2400, 3000, 300, 1600, 3000];
const rates = (__ENV.STRESS_RATES || defaultRates.join(",")).split(",").map(Number);
const stageSeconds = Number(__ENV.STRESS_STAGE_SECONDS || 120);
const preAllocatedVUs = Number(__ENV.STRESS_PREALLOCATED_VUS || 100);
const maxVUs = Number(__ENV.STRESS_MAX_VUS || 2500);

if (rates.length !== stageNames.length || rates.some((rate) => !Number.isInteger(rate) || rate < 1 || rate > 3000)) {
  throw new Error("STRESS_RATES must contain 14 comma-separated integer rates between 1 and 3000.");
}
if (!Number.isInteger(stageSeconds) || stageSeconds < 30 || stageSeconds > 600) {
  throw new Error("STRESS_STAGE_SECONDS must be an integer between 30 and 600.");
}
if (!Number.isInteger(preAllocatedVUs) || !Number.isInteger(maxVUs) ||
    preAllocatedVUs < 1 || maxVUs < preAllocatedVUs || maxVUs > 5000) {
  throw new Error("Stress VU configuration must satisfy 1 <= preallocated <= max <= 5000.");
}

const scenarios = Object.fromEntries(stageNames.map((name, index) => [
  `stage_${index + 1}`,
  {
    executor: "constant-arrival-rate",
    exec: "mixedWorkload",
    startTime: `${index * stageSeconds}s`,
    rate: rates[index],
    timeUnit: "1s",
    duration: `${stageSeconds}s`,
    preAllocatedVUs,
    maxVUs,
    tags: { load_stage: name, load_rate_rps: String(rates[index]) },
  },
]));

http.setResponseCallback(http.expectedStatuses(200, 201, 204, 409, 429));

export const options = {
  scenarios,
  thresholds: {
    checks: ["rate>0.98"],
    http_req_failed: ["rate<0.02"],
  },
};

export function mixedWorkload() {
  const choice = Math.random();

  if (choice < 0.4) {
    group("output-cache", () => {
      const response = http.get(`${baseUrl}/api/catalog/stress-${__VU % 100}`);
      check(response, { "catalog request succeeds": (result) => result.status === 200 });
    });
    return;
  }

  if (choice < 0.7) {
    group("idempotency", () => {
      const key = `${runId}-${exec.scenario.name}-${__VU}-${__ITER}`;
      const params = {
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": key,
          "X-Lab-Run": runId,
        },
      };
      const payload = JSON.stringify({ sku: "lab-widget", quantity: 1 });
      const first = http.post(`${baseUrl}/api/orders`, payload, params);
      const replay = http.post(`${baseUrl}/api/orders`, payload, params);
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
    const response = http.get(`${baseUrl}/api/rate-limited`, {
      headers: { "X-Lab-Run": `${runId}-${__VU}` },
    });
    check(response, {
      "rate-limit response is accepted or rejected": (result) =>
        result.status === 200 || result.status === 429,
    });
  });
}
