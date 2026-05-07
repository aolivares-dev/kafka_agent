import os
import time
from flask import Flask, logging
from flask import request
import kafka_producer
import kafka_consumer
import kafka_admin
app = Flask(__name__)
logger = logging.create_logger(app)

@app.route("/producer", methods=['GET', 'POST'])
def producer():
    code = 400
    body = None
    if request.method == "POST":
        body = request.get_json()
        logger.info(f"Cuerpo de la solicitud {body}")
        message = body.get("message") or body.get("payload")
        code = kafka_producer.send_message(body["cloud_event_id"],
                                           body["topic"],
                                           message,
                                           body["headers"],
                                           body["type"])

        logger.info('Publicacion exitosa')
    return {
        "status": {
            "code": code,
            "message": "Publicacion exitosa" if code == 200 else "Error al publicar"
        },
        "data": body
    }

@app.route("/consumer", methods=['GET', 'POST'])
def consumer():
    if request.method == "POST":
        retry = int(os.getenv("CONSUMER_RETRY_ATTEMPS"))
        body = request.get_json()
        result = kafka_consumer.get_message(body["cloud_event_id"],
                                           body["topic"],
                                           body["group"],
                                           retry)

        logger.info("Resultado obtenido del consumer de Kafka")

        # Si el resultado es None, devolvemos una respuesta vacía
        if result is None:
            return {
                "data": None
            }

        # Si hay resultado, devolvemos directamente la estructura ya formateada
        return result

    # Si no es POST, devolvemos una respuesta vacía
    return {
        "data": None
    }

@app.route("/v2/consumer", methods=['GET', 'POST'])
def consumer_v2():
    """
    Endpoint v2 del consumer.
    
    Lógica:
    1. Recibe topic, header_key, header_value, lookback_ms (tiempo hacia atrás)
    2. Calcula el timestamp = now - lookback_ms
    3. Lee TODOS los mensajes del tópico desde ese timestamp (sin filtro)
    4. Ordena los mensajes del más reciente al más antiguo
    5. Filtra en memoria por header_key == header_value
    6. Retorna los mensajes filtrados
    
    Body esperado:
    {
        "topic": "nombre-del-topico",
        "header_key": "ce_id",
        "header_value": "uuid-del-evento",
        "lookback_ms": 60000,                 // tiempo hacia atrás en ms (default 60s)
        "max_polls": 3                        // opcional, default env
    }
    
    Nota: NO usa consumer groups, hace assign() directo a las particiones.
    """
    start_time = time.time()
    logger.info("Entrando al consumer v2")
    
    if request.method == "POST":
        default_retry = int(os.getenv("CONSUMER_RETRY_ATTEMPS", "3"))
        body = request.get_json()
        logger.info(f"Request body: {body}")
        
        topic = body.get("topic")
        header_key = body.get("header_key")
        header_value = body.get("header_value")
        lookback_ms = int(body.get("lookback_ms", 60000))
        max_polls = int(body.get("max_polls", default_retry))
        
        # Calcular timestamp desde donde leer: ahora - lookback_ms
        from_timestamp_ms = int(time.time() * 1000) - lookback_ms
        
        logger.info(f"Buscando en topic={topic}, header={header_key}={header_value}, lookback={lookback_ms}ms, from_ts={from_timestamp_ms}")
        
        result = kafka_consumer.get_message_v2(
            header_key=header_key,
            header_value=header_value,
            topic=topic,
            from_timestamp_ms=from_timestamp_ms,
            max_polls=max_polls
        )

        # Calcular tiempo de respuesta
        elapsed_time = time.time() - start_time
        elapsed_time_ms = round(elapsed_time * 1000, 2)
        
        logger.info(f"Consumer v2 completado - Tiempo: {elapsed_time_ms}ms")

        if result is None:
            return {
                "data": [],
                "response_time_ms": elapsed_time_ms
            }

        return {
            "data": result,
            "response_time_ms": elapsed_time_ms
        }

    # Si no es POST, devolvemos una respuesta vacía
    return {
        "data": None
    }

@app.route("/clean-topics", methods=['POST'])
def clean_topics():
    """
    Endpoint para limpiar tópicos de Kafka.
    Si la lista de tópicos está vacía, elimina todos los tópicos.
    
    Body esperado:
    {
        "body": {
            "topics": ["topico-1", "topico-2", ...]
        }
    }
    """
    logger.info("Iniciando limpieza de tópicos")
    
    request_data = request.get_json() if request.is_json else {}
    body = request_data.get("body", {})
    topics = body.get("topics", [])
    
    # Validar que topics sea una lista
    if not isinstance(topics, list):
        return {
            "status": {
                "code": 400,
                "message": "El campo 'topics' debe ser una lista de strings"
            },
            "data": None
        }, 400
    
    # Validar que todos los elementos de la lista sean strings
    if topics and not all(isinstance(topic, str) for topic in topics):
        return {
            "status": {
                "code": 400,
                "message": "Todos los elementos de 'topics' deben ser strings"
            },
            "data": None
        }, 400
    
    # Si la lista está vacía, se limpiarán todos los tópicos
    if not topics:
        logger.info("Lista de tópicos vacía. Se limpiarán todos los tópicos")
    
    result = kafka_admin.delete_topics(topics if topics else None)
    
    status_code = result["status"]["code"]
    return result, status_code
