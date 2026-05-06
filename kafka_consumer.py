import aws_utils
import logging
import json
import os
from kafka import KafkaConsumer
from typing import Dict, Any, Optional, List


def get_message(cloud_event_id: str, topic: str, group: str, max_polls=10) -> Dict[str, Any]:
    """
    Obtiene un mensaje específico de Kafka por su cloud_event_id.

    :param cloud_event_id: id del evento a buscar
    :param topic: topico de Kafka
    :param group: grupo consumidor
    :param max_polls: número máximo de intentos de poll
    :return: Un diccionario con la estructura {data: {key, value, headers}} si se encuentra el mensaje,
             o None si no se encuentra o hay un error
    """
    # Configuración de logging
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    logger = logging.getLogger(__name__)

    consumer = None

    try:
        logger.info(f"Buscando mensaje con cloud_event_id: {cloud_event_id} en topic: {topic}")

        bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
        
        # Configuración según el entorno
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
        
        kafka_config = {
            "bootstrap_servers": bootstrap_servers,
            "auto_offset_reset": 'earliest',
            "request_timeout_ms": 30000,
            "group_id": group,
            "fetch_max_wait_ms": 500,
            "fetch_min_bytes": 1,
            "max_partition_fetch_bytes": 1048576
        }
        
        if not is_local:
            # Agregar configuración SSL solo para producción
            kafka_config["ssl_check_hostname"] = False
            kafka_config["security_protocol"] = "SSL"
        
        consumer = KafkaConsumer(**kafka_config)
        consumer.subscribe([topic])

        poll_count = 0
        while poll_count < max_polls:
            poll_count += 1
            logger.info(f"Poll intento {poll_count}/{max_polls}")

            records = consumer.poll(timeout_ms=10000)
            if not records:
                logger.info("No se encontraron registros en este poll")
                continue

            for topic_partition, consumer_records in records.items():
                for message in consumer_records:
                    # Procesamos todos los headers
                    headers = {}
                    for key, value in message.headers:
                        if value is not None:
                            try:
                                headers[key] = value.decode('utf-8')
                            except UnicodeDecodeError:
                                # Si no se puede decodificar como utf-8, guardamos el valor binario
                                headers[key] = value
                        else:
                            headers[key] = None

                    # Verificamos si este es el mensaje que buscamos
                    if headers.get("ce_id") == cloud_event_id:
                        try:
                            # Decodificamos el valor binario a string
                            value_str = message.value.decode('utf-8')
                            # Parseamos el string a un objeto JSON
                            body_obj = json.loads(value_str)

                            # Construimos la respuesta con la estructura solicitada
                            key_str = message.key.decode('utf-8') if message.key else ""

                            response = {
                                "data": {
                                    "key": key_str,
                                    "value": body_obj,
                                    "headers": headers
                                }
                            }

                            logger.info(f"Mensaje encontrado con cloud_event_id: {cloud_event_id}")
                            return response
                        except UnicodeDecodeError as decode_err:
                            logger.error(f"Error al decodificar el mensaje: {str(decode_err)}")
                            return None
                        except json.JSONDecodeError as json_err:
                            logger.error(f"Error al parsear el mensaje como JSON: {str(json_err)}")
                            return None

        logger.warning(
            f"No se encontró el mensaje con cloud_event_id: {cloud_event_id} después de {max_polls} intentos")
        return None

    except Exception as e:
        logger.error(f"Error al procesar mensajes de Kafka: {str(e)}", exc_info=True)
        return None
    finally:
        if consumer:
            try:
                consumer.close(autocommit=False)
                logger.info("Consumer cerrado correctamente")
            except Exception as e:
                logger.error(f"Error al cerrar el consumer: {str(e)}", exc_info=True)


def get_message_v2(header_key: str, header_value: str, topic: str, group: str, max_polls=10) -> List[Dict[str, Any]]:
    """
    Obtiene un mensaje específico de Kafka por una cabecera.

    :param header_name: nombre de la cabecera a buscar
    :param header_value: valor de la cabecera a buscar
    :param topic: topico de Kafka
    :param group: grupo consumidor
    :param max_polls: número máximo de intentos de poll
    :return: Un diccionario con la estructura {data: {key, value, headers}} si se encuentra el mensaje,
             o None si no se encuentra o hay un error
    """
    # Configuración de logging
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    logger = logging.getLogger(__name__)

    consumer = None

    try:
        logger.info(f"Buscando mensaje con {header_key}: {header_value} en topic: {topic}")

        bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
        
        # Configuración según el entorno
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
        
        kafka_config = {
            "bootstrap_servers": bootstrap_servers,
            "auto_offset_reset": 'earliest',
            "request_timeout_ms": 30000,
            "group_id": group,
            "fetch_max_wait_ms": 500,
            "fetch_min_bytes": 1,
            "max_partition_fetch_bytes": 1048576
        }
        
        if not is_local:
            # Agregar configuración SSL solo para producción
            kafka_config["ssl_check_hostname"] = False
            kafka_config["security_protocol"] = "SSL"
        
        consumer = KafkaConsumer(**kafka_config)
        consumer.subscribe([topic])

        poll_count = 0

        response = []
        while poll_count < max_polls:
            poll_count += 1
            logger.info(f"Poll intento {poll_count}/{max_polls}")

            records = consumer.poll(timeout_ms=10000)
            if not records:
                logger.info("No se encontraron registros en este poll")
                continue

            for topic_partition, consumer_records in records.items():
                for message in consumer_records:
                    # Procesamos todos los headers
                    headers = {}
                    for key, value in message.headers:
                        if value is not None:
                            try:
                                headers[key] = value.decode('utf-8')
                            except UnicodeDecodeError:
                                # Si no se puede decodificar como utf-8, guardamos el valor binario
                                headers[key] = value
                        else:
                            headers[key] = None

                    # Verificamos si este es el mensaje que buscamos
                    if headers.get(f"{header_key}") == header_value:
                        try:
                            # Decodificamos el valor binario a string
                            value_str = message.value.decode('utf-8')
                            # Parseamos el string a un objeto JSON
                            body_obj = json.loads(value_str)

                            # Construimos la respuesta con la estructura solicitada
                            key_str = message.key.decode('utf-8') if message.key else ""

                            consumer_response = {
                                "data": {
                                    "key": key_str,
                                    "value": body_obj,
                                    "headers": headers
                                }
                            }

                            logger.info(f"Mensaje encontrado con {header_key}: {header_value}")
                            response.append(consumer_response)
                        except UnicodeDecodeError as decode_err:
                            logger.error(f"Error al decodificar el mensaje: {str(decode_err)}")
                            return None
                        except json.JSONDecodeError as json_err:
                            logger.error(f"Error al parsear el mensaje como JSON: {str(json_err)}")
                            return None
            if len(response) > 0:
                return response
        logger.warning(f"No se encontró el mensaje con {header_key}: {header_value} después de {max_polls} intentos")
        return response

    except Exception as e:
        logger.error(f"Error al procesar mensajes de Kafka: {str(e)}", exc_info=True)
        return None
    finally:
        if consumer:
            try:
                consumer.close(autocommit=False)
                logger.info("Consumer cerrado correctamente")
            except Exception as e:
                logger.error(f"Error al cerrar el consumer: {str(e)}", exc_info=True)
