import { check, group, sleep } from "k6";
import http from "k6/http";

const baseUrl = __ENV.TARGET_URL || "http://127.0.0.1:8080";
const runId = `smoke-${Math.random().toString(16).slice(2)}`;

http.setResponseCallback(http.expectedStatuses(200, 201, 204, 409, 429));

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    checks: ["rate==1"],
    http_req_failed: ["rate<0.05"],
    http_req_duration: ["p(95)<1000"],
  },
};

export default function () {
  group("readiness", () => {
    const response = http.get(`${baseUrl}/health/ready`);
    check(response, {
      "API and Ruvio are ready": (r) => r.status === 200,
    });
  });

  group("output cache", () => {
    http.post(`${baseUrl}/api/catalog/invalidate`);
    const before = http.get(`${baseUrl}/_lab/state`).json("catalogOriginReads");
    const first = http.get(`${baseUrl}/api/catalog/lab-widget`);
    const second = http.get(`${baseUrl}/api/catalog/lab-widget`);
    const after = http.get(`${baseUrl}/_lab/state`).json("catalogOriginReads");
    check([first, second], {
      "catalog responses succeed": (responses) => responses.every((r) => r.status === 200),
      "output cache replays the same representation": (responses) =>
        responses[0].body === responses[1].body,
    });
    check(after - before, {
      "second catalog request is served from output cache": (originCalls) => originCalls === 1,
    });
  });

  group("HTTP idempotency", () => {
    const before = http.get(`${baseUrl}/_lab/state`).json("orderEffects");
    const key = `smoke-${Math.random().toString(16).slice(2)}`;
    const headers = {
      "Content-Type": "application/json",
      "Idempotency-Key": key,
      "X-Lab-Run": runId,
    };
    const payload = JSON.stringify({ sku: "lab-widget", quantity: 1 });
    const first = http.post(`${baseUrl}/api/orders`, payload, { headers });
    const replay = http.post(`${baseUrl}/api/orders`, payload, { headers });
    const conflict = http.post(
      `${baseUrl}/api/orders`,
      JSON.stringify({ sku: "different-widget", quantity: 1 }),
      { headers },
    );

    check([first, replay, conflict], {
      "first order is created": (responses) => responses[0].status === 201,
      "duplicate replays the original order": (responses) =>
        responses[1].status === 201 && responses[1].body === responses[0].body,
      "changed request is rejected": (responses) => responses[2].status === 409,
    });
    const after = http.get(`${baseUrl}/_lab/state`).json("orderEffects");
    check(after - before, {
      "duplicate request creates exactly one side effect": (effects) => effects === 1,
    });
  });

  group("distributed rate limiting", () => {
    const headers = { "X-Lab-Run": `${runId}-rate` };
    const responses = http.batch(
      Array.from({ length: 22 }, () => ({
        method: "GET",
        url: `${baseUrl}/api/rate-limited`,
        params: { headers },
      })),
    );
    const accepted = responses.filter((response) => response.status === 200).length;
    const rejected = responses.filter((response) => response.status === 429).length;

    check({ accepted, rejected, responses }, {
      "rate limiter accepts its configured budget": (result) => result.accepted === 20,
      "rate limiter rejects excess requests": (result) => result.rejected === 2,
      "rate-limit probes only return expected statuses": (result) =>
        result.responses.every((response) => response.status === 200 || response.status === 429),
    });
  });

  sleep(0.1);
}
