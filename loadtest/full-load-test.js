// Full load test: sustained traffic on the two autoscaled services, Position Ingestion and Train Registry.
// Each virtual user is a train reporting its position, then a passenger looking the train up.
//
// Run:      k6 run -e BASE_URL=http://<alb-address> loadtest/full-load-test.js
// Baseline: k6 run -e BASE_URL=http://<alb-address> -e PROFILE=baseline loadtest/full-load-test.js
//           (one user for a minute, to see response times without load)
// PEAK_VUS sets the number of users at the peak (default 60).
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;
const PEAK_VUS = Number(__ENV.PEAK_VUS || 60);
const TRAINS = ['12951', '12002', '22691', '12626'];

const PROFILES = {
  full: [
    { duration: '2m', target: Math.round(PEAK_VUS / 3) }, // warm up: pods and connections ready
    { duration: '3m', target: PEAK_VUS }, // ramp to the peak: the autoscalers add pods here
    { duration: '10m', target: PEAK_VUS }, // sustained peak; one pod is deleted in the middle of it
    { duration: '2m', target: 0 }, // ramp down: the autoscalers remove pods after 60 s
  ],
  baseline: [{ duration: '1m', target: 1 }],
};

export const options = {
  stages: PROFILES[__ENV.PROFILE || 'full'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
    // Listed so the summary shows each endpoint on its own line
    'http_req_duration{name:POST /api/positions}': ['p(95)<500'],
    'http_req_duration{name:GET /api/trains/{number}}': ['p(95)<500'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'max'],
};

export default function () {
  const train = TRAINS[Math.floor(Math.random() * TRAINS.length)];

  const report = {
    train_number: train,
    section: `S${1 + Math.floor(Math.random() * 10)}`,
    latitude: 20 + Math.random() * 8,
    longitude: 73 + Math.random() * 4,
    speed_kmph: 60 + Math.random() * 70,
  };
  const position = http.post(`${BASE_URL}/api/positions`, JSON.stringify(report), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /api/positions' },
  });
  check(position, { 'position accepted (202)': (r) => r.status === 202 });

  const lookup = http.get(`${BASE_URL}/api/trains/${train}`, {
    tags: { name: 'GET /api/trains/{number}' },
  });
  check(lookup, { 'train found (200)': (r) => r.status === 200 });

  sleep(0.2);
}
