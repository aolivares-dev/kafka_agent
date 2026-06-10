Contenido planeado para el .md
# Kafka Agent - Curl Examples

## Producer

Publica un mensaje CloudEvents en un tópico Kafka:

```bash
curl -X POST http://localhost:9091/producer \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "mi-topico",
    "cloud_event_id": "evt-001",
    "type": "com.ejemplo.evento",
    "headers": {
      "customheader": "valor1"
    },
    "message": {
      "key": "value",
      "foo": "bar"
    }
  }'
## Listener

Escucha un tópico y retorna el primer mensaje encontrado:

```bash
curl -X POST http://localhost:9091/listener \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "mi-topico",
    "group": "mi-grupo-consumidor",
    "max_polls": 3
  }'
```

## Consumer v1
Busca un mensaje por su cloud_event_id (ce_id header):
curl -X POST http://localhost:9091/consumer \
  -H "Content-Type: application/json" \
  -d '{
    "cloud_event_id": "evt-001",
    "topic": "mi-topico",
    "group": "mi-grupo-consumidor"
  }'
Consumer v2
Busca mensajes por un header personalizado:
curl -X POST http://localhost:9091/v2/consumer \
  -H "Content-Type: application/json" \
  -d '{
    "header_key": "customheader",
    "header_value": "valor1",
    "topic": "mi-topico",
    "group": "mi-grupo-consumidor",
    "max_polls": 5
  }'
Notas
- max_polls es opcional en v2 (default: 3, configurable via env CONSUMER_RETRY_ATTEMPS)
- message puede enviarse como payload (el código acepta ambos)
- Ejecutar contenedor: docker compose up -d desde ./kafka_agent/