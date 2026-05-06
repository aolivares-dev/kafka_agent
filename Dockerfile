FROM alpine

ENV WORKERS=10

RUN apk add --no-cache python3 py3-pip

WORKDIR /app

# Crear el entorno virtual primero
RUN python -m venv .venv

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

# Copiar solo requirements.txt primero (para aprovechar cache de Docker)
COPY requirements.txt /app/requirements.txt

# Instalar dependencias (esta capa se cachea si requirements.txt no cambia)
RUN .venv/bin/pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copiar el código de la aplicación al final (esta capa se reconstruye con cada cambio)
COPY *.py /app/

CMD [".venv/bin/gunicorn", "-w 10",  "--bind", "0.0.0.0:80", "-t 300", "main:app"]
