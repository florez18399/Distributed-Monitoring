const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;
const APP_NAME = process.env.APP_NAME || 'mock-app';
const ZONE_ID = process.env.ZONE_ID || 'unknown-zone';
const SCENARIO = process.env.SCENARIO || 'NORMAL';
const EGRESS_HOST = process.env.EGRESS_HOST || 'envoy';
const EGRESS_PORT = process.env.EGRESS_PORT || '9001';

app.use(express.json());

// --- Configuration ---
const ERROR_RATE = parseFloat(process.env.ERROR_RATE || '0.05');
const LATENCY_BASE = parseInt(process.env.LATENCY_BASE || '100');
const LATENCY_VARIANCE = parseInt(process.env.LATENCY_VARIANCE || '200');

// --- Downstream calls config ---
let DOWNSTREAM_CALLS = [];
try {
  DOWNSTREAM_CALLS = JSON.parse(process.env.DOWNSTREAM_CALLS || '[]');
} catch (e) {
  console.error(`[${APP_NAME}] Error parsing DOWNSTREAM_CALLS:`, e.message);
}

// --- Mock Data ---
const mockProducts = [
  { id: 1, name: `${APP_NAME} Item A`, category: 'Electronics', price: 100 },
  { id: 2, name: `${APP_NAME} Item B`, category: 'Homeware', price: 20 },
  { id: 3, name: `${APP_NAME} Item C`, category: 'Apparel', price: 50 }
];

const mockOrders = [
  { id: 'ORD-001', product: `${APP_NAME} Item A`, qty: 2, status: 'confirmed' },
  { id: 'ORD-002', product: `${APP_NAME} Item C`, qty: 1, status: 'pending' }
];

// --- Downstream call helper ---
async function callDownstream(reqId) {
  const results = [];
  for (const call of DOWNSTREAM_CALLS) {
    if (Math.random() < call.probability) {
      const url = `http://${EGRESS_HOST}:${EGRESS_PORT}${call.path}`;
      try {
        const start = Date.now();
        const resp = await fetch(url, {
          headers: {
            'X-REQUEST-ID': reqId || '',
            'X-CONSUMER-ID': `${ZONE_ID}/${APP_NAME}-1`
          },
          signal: AbortSignal.timeout(5000)
        });
        const duration = Date.now() - start;
        results.push({
          target: call.target,
          status: resp.status,
          duration_ms: duration
        });
        console.log(`[${ZONE_ID}/${APP_NAME}] -> ${call.target} ${resp.status} (${duration}ms)`);
      } catch (e) {
        results.push({ target: call.target, status: 'ERROR', error: e.message });
        console.log(`[${ZONE_ID}/${APP_NAME}] -> ${call.target} ERROR: ${e.message}`);
      }
    }
  }
  return results;
}

// --- Behavior Middleware ---
const behaviorMiddleware = (req, res, next) => {
  let latency = LATENCY_BASE + Math.floor(Math.random() * LATENCY_VARIANCE);
  let shouldFail = Math.random() < ERROR_RATE;

  switch (SCENARIO.toUpperCase()) {
    case 'DEGRADED':
      latency += 3000;
      break;
    case 'CHAOS':
      shouldFail = Math.random() < 0.5;
      latency = 50;
      break;
    case 'BURSTY':
      if (Math.random() < 0.1) latency += 5000;
      break;
  }

  if (shouldFail && !req.path.includes('health')) {
    return setTimeout(() => {
      console.log(`[${ZONE_ID}/${APP_NAME}] [FAILURE] ${req.method} ${req.path} - Scenario: ${SCENARIO}`);
      res.status(500).json({ error: 'Internal Server Error', scenario: SCENARIO, app: APP_NAME });
    }, latency / 2);
  }

  setTimeout(next, latency);
};

app.use(behaviorMiddleware);

// --- Routes ---
const router = express.Router();

router.get('/', (req, res) => {
  res.json({ message: `Welcome to ${APP_NAME}`, zone: ZONE_ID, scenario: SCENARIO });
});

router.get('/products', async (req, res) => {
  console.log(`[${ZONE_ID}/${APP_NAME}] Listing products`);
  const downstream = await callDownstream(req.headers['x-request-id']);
  res.json({ products: mockProducts, downstream_calls: downstream });
});

router.get('/checkout', async (req, res) => {
  console.log(`[${ZONE_ID}/${APP_NAME}] Processing checkout`);
  const downstream = await callDownstream(req.headers['x-request-id']);
  res.json({ orders: mockOrders, status: 'processed', downstream_calls: downstream });
});

router.get('/validate', async (req, res) => {
  console.log(`[${ZONE_ID}/${APP_NAME}] Validating request`);
  const downstream = await callDownstream(req.headers['x-request-id']);
  res.json({ valid: true, user: req.headers['x-consumer-id'] || 'anonymous', downstream_calls: downstream });
});

router.get('/api/health', (req, res) => {
  res.json({ status: 'UP', app: APP_NAME, zone: ZONE_ID, scenario: SCENARIO });
});

router.get('/api/error', (req, res) => {
  res.status(500).json({ status: 'CRITICAL', message: 'Simulated endpoint error' });
});

const APP_PATH = process.env.APP_PATH || `/${APP_NAME}`;
app.use(APP_PATH, router);

app.listen(PORT, () => {
  console.log(`${APP_NAME} (${SCENARIO}) listening on port ${PORT} at path ${APP_PATH}`);
  console.log(`Downstream calls configured: ${JSON.stringify(DOWNSTREAM_CALLS)}`);
});
