# Usamos una imagen base ligera de Python
FROM python:3.11-slim

# Directorio de trabajo en el contenedor
WORKDIR /app

# Copiamos solo los requirements primero para aprovechar la caché de capas de Docker
COPY requirements.txt .

# Instalamos las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código
COPY . .

# Exponemos el puerto
EXPOSE 5000

# Usamos Gunicorn como servidor WSGI para producción
# -w 4: 4 workers (ajustable según los cores de tu servidor)
# -b 0.0.0.0:5000: bind al puerto 5000
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:create_app()"]