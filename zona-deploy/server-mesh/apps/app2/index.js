const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;
const router = express.Router();

// Middleware para parsear JSON en el request body
app.use(express.json());

// --- Base de datos simulada (en memoria) ---
let mockProducts = [
  { id: 1, name: 'Laptop Pro', category: 'Electronics', price: 1500, stock: 50 },
  { id: 2, name: 'Coffee Mug', category: 'Homeware', price: 15, stock: 200 },
  { id: 3, name: 'Running Shoes', category: 'Apparel', price: 80, stock: 120 },
  { id: 4, name: 'Quantum Computer', category: 'Electronics', price: 99999, stock: 1 }
];
let nextProductId = 5;

// --- Función de utilidad para simular aleatoriedad ---
// Simula una latencia de red aleatoria
const simulateLatency = (req, res, next) => {
  const delay = Math.floor(Math.random() * 750) + 100; // Latencia entre 100ms y 850ms
  setTimeout(next, delay);
};

// --- Rutas de la API ---
// 1. GET / (Ruta base)
router.get('/', (req, res) => {
  res.send('Welcome to the Mock Product API!');
});

// 2. GET /products (Obtener todos los productos)
//    Prueba con query parameters: /products?category=Electronics&minPrice=50&maxPrice=1600
router.get('/products', simulateLatency, (req, res) => {
  const { category, minPrice, maxPrice } = req.query;
  let results = [...mockProducts]; // Copiamos para no modificar el original

  let logMessages = ['[GET /products]'];

  // Filtrar por categoría
  if (category) {
    results = results.filter(
      p => p.category.toLowerCase() === category.toLowerCase()
    );
    logMessages.push(`Filtering by category: ${category}`);
  }

  // Filtrar por precio mínimo
  if (minPrice) {
    results = results.filter(
      p => p.price >= parseFloat(minPrice)
    );
    logMessages.push(`Filtering by minPrice: ${minPrice}`);
  }

  // Filtrar por precio máximo
  if (maxPrice) {
    results = results.filter(
      p => p.price <= parseFloat(maxPrice)
    );
    logMessages.push(`Filtering by maxPrice: ${maxPrice}`);
  }


  if (logMessages.length === 1) {
    logMessages.push('Returning all products');
  }
  
  console.log(logMessages.join(' | '));
  res.json(results);
});

// 3. GET /products/:id (Obtener un producto por ID)
router.get('/products/:id', simulateLatency, (req, res) => {
  const productId = parseInt(req.params.id, 10);
  const product = mockProducts.find(p => p.id === productId);

  if (product) {
    console.log(`[GET /products/:id] Found product ${productId}`);
    res.json(product);
  } else {
    console.log(`[GET /products/:id] Product ${productId} not found`);
    res.status(404).json({ error: 'Product not found' });
  }
});

// 4. POST /products (Crear un nuevo producto)
//    Usa un request body: { "name": "New Gadget", "price": 99.99, "category": "Electronics" }
router.post('/products', simulateLatency, (req, res) => {
  const { name, price, category } = req.body;

  if (!name || !price || !category) {
    console.log('[POST /products] Bad request, missing fields');
    return res.status(400).json({ error: 'Missing required fields: name, price, category' });
  }

  // Simulación de aleatoriedad: asignamos un stock inicial aleatorio
  const randomStock = Math.floor(Math.random() * 1000) + 1;

  const newProduct = {
    id: nextProductId++,
    name,
    category,
    price: parseFloat(price),
    stock: randomStock
  };

  mockProducts.push(newProduct);
  console.log(`[POST /products] Created new product ${newProduct.id} with stock ${randomStock}`);
  res.status(201).json(newProduct);
});

// 5. PUT /products/:id (Actualizar un producto)
//    Usa un request body: { "price": 120.50 }
router.put('/products/:id', simulateLatency, (req, res) => {
  const productId = parseInt(req.params.id, 10);
  const productIndex = mockProducts.findIndex(p => p.id === productId);

  if (productIndex === -1) {
    console.log(`[PUT /products/:id] Product ${productId} not found`);
    return res.status(404).json({ error: 'Product not found' });
  }

  // Actualizar solo los campos proporcionados
  const originalProduct = mockProducts[productIndex];
  const updatedProduct = {
    ...originalProduct,
    ...req.body,
    id: originalProduct.id // Asegurarse de que el ID no cambie
  };

  mockProducts[productIndex] = updatedProduct;
  console.log(`[PUT /products/:id] Updated product ${productId}`);
  res.json(updatedProduct);
});

// 6. DELETE /products/:id (Eliminar un producto)
router.delete('/products/:id', simulateLatency, (req, res) => {
  const productId = parseInt(req.params.id, 10);
  const productIndex = mockProducts.findIndex(p => p.id === productId);

  if (productIndex === -1) {
    console.log(`[DELETE /products/:id] Product ${productId} not found`);
    return res.status(404).json({ error: 'Product not found' });
  }

  mockProducts.splice(productIndex, 1);
  console.log(`[DELETE /products/:id] Deleted product ${productId}`);
  res.status(200).json({ message: 'Product deleted successfully' });
});

// 7. GET /api/health (Simulación de estado aleatorio)
router.get('/api/health', (req, res) => {
  const statuses = [
    { code: 200, status: "OK", message: "All systems nominal." },
    { code: 200, status: "DEGRADED", message: "Experiencing high latency on database." },
    { code: 503, status: "UNAVAILABLE", message: "Payment service is down." }
  ];

  // Elegir un estado aleatorio
  const randomHealth = statuses[Math.floor(Math.random() * statuses.length)];
  
  console.log(`[GET /api/health] Reporting status: ${randomHealth.status}`);
  res.status(randomHealth.code).json(randomHealth);
});

// 8. GET /api/secure-data (Simulación de lectura de Headers)
//    Prueba enviando un header: 'x-api-key: mysecretkey123'
router.get('/api/secure-data', simulateLatency, (req, res) => {
  const apiKey = req.headers['x-api-key'];

  console.log(`[GET /api/secure-data] Attempting access with API key: ${apiKey}`);

  // Validamos el API key
  if (!apiKey || apiKey !== 'mysecretkey123') {
    console.log('[GET /api/secure-data] Access denied. Invalid or missing API key.');
    // 401 Unauthorized es apropiado aquí
    return res.status(401).json({ error: 'Unauthorized. Missing or invalid API key.' });
  }

  console.log('[GET /api/secure-data] Access granted.');
  // Si la validación es exitosa, enviamos los datos "secretos"
  res.json({
    message: 'This is super secret data, accessed successfully.',
    timestamp: Date.now(),
    user: 'mock-user-from-key'
  });
});

const APP_PATH = process.env.APP_PATH || '/app_2';
app.use(APP_PATH, router);

// Iniciar el servidor
app.listen(PORT, () => {
  console.log(`Express server listening on port ${PORT}`);
  console.log('--- API Endpoints ---');
  console.log(`GET    http://localhost:${PORT}${APP_PATH}/products`);
  console.log(`GET    http://localhost:${PORT}${APP_PATH}/products?category=...&minPrice=...`);
  console.log(`GET    http://localhost:${PORT}${APP_PATH}/products/1`);
  console.log(`POST   http://localhost:${PORT}${APP_PATH}/products`);
  console.log(`PUT    http://localhost:${PORT}${APP_PATH}/products/1`);
  console.log(`DELETE http://localhost:${PORT}${APP_PATH}/products/1`);
  console.log(`GET    http://localhost:${PORT}${APP_PATH}/api/health`);
  console.log(`GET    http://localhost:${PORT}${APP_PATH}/api/secure-data (requires 'x-api-key' header)`);
  console.log('---------------------');
});