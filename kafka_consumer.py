import json
import logging
import os
import time
from typing import Dict, Any, List, Set

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

import aws_utils


def _setup_logger() -> logging.Logger:
    """Configura y retorna un logger."""
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    return logging.getLogger(__name__)


def _get_kafka_config(is_local: bool) -> dict:
    """
    Retorna la configuración base de Kafka (sin group_id, para assign directo).
    """
    bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
    
    config = {
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": 'latest',
        "enable_auto_commit": False,
        "request_timeout_ms": 60000,
        "fetch_max_wait_ms": 1000,
        "fetch_min_bytes": 1,
        "max_partition_fetch_bytes": 1048576,
        "max_poll_records": 500
    }
    
    if not is_local:
        config["ssl_check_hostname"] = False
        config["security_protocol"] = "SSL"
    
    return config


def _decode_headers(message_headers: List) -> Dict[str, Any]:
    """Decodifica los headers de un mensaje de Kafka."""
    headers = {}
    for key, value in message_headers:
        if value is not None:
            try:
                headers[key] = value.decode('utf-8')
            except UnicodeDecodeError:
                headers[key] = value
        else:
            headers[key] = None
    return headers


def _parse_message_value(message_value: bytes) -> Any:
    """Decodifica y parsea el valor de un mensaje."""
    value_str = message_value.decode('utf-8')
    return json.loads(value_str)


def _build_response(message_key: bytes, message_value: Any, headers: Dict[str, Any]) -> Dict[str, Any]:
    """Construye la respuesta con la estructura esperada."""
    key_str = message_key.decode('utf-8') if message_key else ""
    return {
        "data": {
            "key": key_str,
            "value": message_value,
            "headers": headers
        }
    }


def get_message(cloud_event_id: str, topic: str, group: str, max_polls=3) -> Dict[str, Any]:
    """
    Obtiene un mensaje especifico de Kafka por su cloud_event_id.
    (Endpoint legacy /consumer)
    """
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    logger = logging.getLogger(__name__)

    consumer = None

    try:
        logger.info(f"Buscando mensaje con cloud_event_id: {cloud_event_id} en topic: {topic}")

        bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
        
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
        
        kafka_config = {
            "bootstrap_servers": bootstrap_servers,
            "auto_offset_reset": 'earliest',
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 5000,
            "request_timeout_ms": 30000,
            "group_id": group,
            "fetch_max_wait_ms": 500,
            "fetch_min_bytes": 1,
            "max_partition_fetch_bytes": 1048576
        }
        
        if not is_local:
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
                    headers = {}
                    for key, value in message.headers:
                        if value is not None:
                            try:
                                headers[key] = value.decode('utf-8')
                            except UnicodeDecodeError:
                                headers[key] = value
                        else:
                            headers[key] = None

                    if headers.get("ce_id") == cloud_event_id:
                        try:
                            value_str = message.value.decode('utf-8')
                            body_obj = json.loads(value_str)
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

        logger.warning(f"No se encontro el mensaje con cloud_event_id: {cloud_event_id} despues de {max_polls} intentos")
        return None

    except Exception as e:
        logger.error(f"Error al procesar mensajes de Kafka: {str(e)}", exc_info=True)
        return None
    finally:
        if consumer:
            try:
                consumer.close(autocommit=True)
                logger.info("Consumer cerrado correctamente")
            except Exception as e:
                logger.error(f"Error al cerrar el consumer: {str(e)}", exc_info=True)


def get_message_v2(header_key: str, header_value: str, topic: str, from_timestamp_ms: int, max_polls: int = 3) -> List[Dict[str, Any]]:
    """
    Endpoint v2 del consumer.
    
    Lógica:
    1. Usa assign() directo (sin rebalance) para obtener particiones
    2. Usa offsets_for_times() para posicionarse en from_timestamp_ms
    3. Lee TODOS los mensajes desde ese timestamp sin filtrar
    4. Ordena los mensajes del más reciente al más antiguo (top = más reciente)
    5. Filtra en memoria por header_key == header_value
    6. Retorna los mensajes filtrados (más recientes primero)

    :param header_key: nombre del header a filtrar (ej: "ce_id")
    :param header_value: valor del header a buscar
    :param topic: tópico de Kafka
    :param from_timestamp_ms: epoch millis desde donde empezar a leer
    :param max_polls: número máximo de polls
    :return: Lista de mensajes filtrados, ordenados del más reciente al más antiguo
    """
    logger = _setup_logger()
    consumer = None

    try:
        logger.info(f"v2 consumer: topic={topic}, filter={header_key}={header_value}, from_ts={from_timestamp_ms}, max_polls={max_polls}")
        
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
        kafka_config = _get_kafka_config(is_local)
        
        consumer = KafkaConsumer(**kafka_config)
        
        # Obtener particiones directamente (sin rebalance, instantáneo)
        topic_partitions_ids = consumer.partitions_for_topic(topic)
        
        if not topic_partitions_ids:
            logger.error(f"No se encontraron particiones para el tópico: {topic}")
            return []
        
        partitions = [TopicPartition(topic, p) for p in topic_partitions_ids]
        consumer.assign(partitions)
        
        logger.info(f"Particiones asignadas: {[p.partition for p in partitions]}")
        
        # Posicionar cada partición en el offset correspondiente al timestamp
        timestamps_to_search = {tp: from_timestamp_ms for tp in partitions}
        offsets_for_times = consumer.offsets_for_times(timestamps_to_search)
        
        for tp, offset_and_timestamp in offsets_for_times.items():
            if offset_and_timestamp is not None:
                consumer.seek(tp, offset_and_timestamp.offset)
                logger.info(f"Partición {tp.partition}: seek a offset {offset_and_timestamp.offset} (ts={offset_and_timestamp.timestamp})")
            else:
                # No hay mensajes desde ese timestamp, ir al final (no leerá nada)
                consumer.seek_to_end(tp)
                logger.info(f"Partición {tp.partition}: sin mensajes desde ts={from_timestamp_ms}, seek to end")
        
        # Leer TODOS los mensajes desde ese punto
        all_messages = []
        for poll_count in range(1, max_polls + 1):
            records = consumer.poll(timeout_ms=5000)
            
            if not records:
                logger.info(f"Poll {poll_count}/{max_polls}: sin registros, finalizando lectura")
                break
            
            batch_count = 0
            for topic_partition, consumer_records in records.items():
                for message in consumer_records:
                    batch_count += 1
                    headers = _decode_headers(message.headers)
                    
                    try:
                        body_obj = _parse_message_value(message.value)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    
                    # Guardar mensaje con su timestamp para ordenar después
                    all_messages.append({
                        "timestamp": message.timestamp,
                        "response": _build_response(message.key, body_obj, headers),
                        "headers": headers
                    })
            
            logger.info(f"Poll {poll_count}/{max_polls}: {batch_count} mensajes leídos, total acumulado: {len(all_messages)}")
        
        logger.info(f"Total mensajes leídos del tópico: {len(all_messages)}")
        
        # Ordenar del más reciente al más antiguo (top = más reciente)
        all_messages.sort(key=lambda m: m["timestamp"], reverse=True)
        
        # Filtrar en memoria por header_key == header_value
        filtered = []
        for msg in all_messages:
            if msg["headers"].get(header_key) == header_value:
                filtered.append(msg["response"])
        
        logger.info(f"Filtrado: {len(filtered)} de {len(all_messages)} mensajes coinciden con {header_key}={header_value}")
        
        return filtered

    except Exception as e:
        logger.error(f"Error en get_message_v2: {str(e)}", exc_info=True)
        return []
    finally:
        if consumer:
            try:
                consumer.close()
                logger.info("Consumer cerrado correctamente")
            except Exception as e:
                logger.error(f"Error al cerrar el consumer: {str(e)}", exc_info=True)
