// Short load test: enough traffic for both autoscalers to add pods.
// Run: k6 run -e BASE_URL=http://<alb-address> loadtest/autoscaling-smoke-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;
const TRAINS = ['12951', '12002', '22691', '12626'];

export const options = {
  stages: [
    { duration: '30s', target: 20 }, // warm up
    { duration: '2m', target: 40 }, // hold enough load to pass 60% CPU
    { duration: '30s', target: 0 }, // ramp down
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
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