import os
from flask import Flask, logging
from flask import request
import kafka_producer
import kafka_consumer
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
    logger.info("Entrando al consumer v2")
    if request.method == "POST":
        retry = int(os.getenv("CONSUMER_RETRY_ATTEMPS"))
        body = request.get_json()
        print(body)
        result = kafka_consumer.get_message_v2(body.get("header_key"),
                                           body.get("header_value"),
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

