import logging
import json
import os

from kafka import KafkaProducer

from headerclass import to_tuple
from cloudevents.kafka import conversion
from cloudevents.http import event
from typing import Dict

import aws_utils


def send_message(cloud_id: str, topic: str, message: str, _headers: Dict, type: str):
    """_summary_
    metodo para publicar un mensaje en kafka.
    :type topic: object
    :param _headers:
    :param message:
    :param topic:
    :param cloud_id:
    :param type
    """
    try:
        _headers.update({"id": cloud_id, "datacontenttype": "application/json", "type": type, "source": "source"})

        # Obtener bootstrap servers
        bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")

        # Configuración según el entorno
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"

        logging.info(f"Modo de operación: {'LOCAL' if is_local else 'PRODUCTION'}")
        logging.info(f"Bootstrap servers: {bootstrap_servers}")

        if is_local:
            # Configuración para Kafka local sin SSL
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks=1,
                request_timeout_ms=30000,
                retries=5,
                metadata_max_age_ms=999999999,
                linger_ms=0
            )
        else:
            # Configuración para Kafka en AWS con SSL
            producer = KafkaProducer(
                ssl_check_hostname=False,
                security_protocol="SSL",
                bootstrap_servers=bootstrap_servers,
                acks=1,
                request_timeout_ms=30000,
                retries=5,
                metadata_max_age_ms=999999999,
                linger_ms=0
            )

        ce_event = event.CloudEvent.create(attributes=_headers, data=json.dumps(message))

        from_convertion = conversion.to_binary(event=ce_event, data_marshaller=str)

        header_list = to_tuple(from_convertion.headers)

        producer.send(topic, value=from_convertion.value, headers=header_list)
        producer.flush()

        logging.info(f"Mensaje enviado exitosamente al topic: {topic}")

        return 200
    except Exception as e:
        logging.error(f"Error al enviar el mensaje: {e}", exc_info=True)
        return 400
