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


def _get_kafka_config(group: str, is_local: bool, enable_auto_commit: bool = True) -> dict:
    """
    Retorna la configuración base de Kafka.
    
    :param group: Grupo consumidor
    :param is_local: Si es entorno local
    :param enable_auto_commit: Si habilitar auto-commit
    :return: Diccionario con configuración de Kafka
    """
    bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
    
    config = {
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": 'earliest',
        "enable_auto_commit": enable_auto_commit,
        "request_timeout_ms": 60000,
        "group_id": group,
        "fetch_max_wait_ms": 1000,
        "fetch_min_bytes": 1,
        "max_partition_fetch_bytes": 1048576,
        "max_poll_records": 1000
    }
    
    if enable_auto_commit:
        config["auto_commit_interval_ms"] = 5000
    
    if not is_local:
        config["ssl_check_hostname"] = False
        config["security_protocol"] = "SSL"
    
    return config


def _decode_headers(message_headers: List) -> Dict[str, Any]:
    """
    Decodifica los headers de un mensaje de Kafka.
    
    :param message_headers: Lista de tuplas (key, value) de headers
    :return: Diccionario con headers decodificados
    """
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
    """
    Decodifica y parsea el valor de un mensaje.
    
    :param message_value: Valor del mensaje en bytes
    :return: Objeto JSON parseado
    :raises: UnicodeDecodeError, json.JSONDecodeError
    """
    value_str = message_value.decode('utf-8')
    return json.loads(value_str)


def _build_response(message_key: bytes, message_value: Any, headers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Construye la respuesta con la estructura esperada.
    
    :param message_key: Key del mensaje
    :param message_value: Value del mensaje (ya parseado)
    :param headers: Headers del mensaje
    :return: Diccionario con estructura de respuesta
    """
    key_str = message_key.decode('utf-8') if message_key else ""
    return {
        "data": {
            "key": key_str,
            "value": message_value,
            "headers": headers
        }
    }


def _seek_to_recent_messages(consumer: KafkaConsumer, partitions: Set[TopicPartition], 
                             messages_to_look_back: int, logger: logging.Logger) -> None:
    """
    Posiciona el consumer para leer mensajes recientes hacia atrás.
    
    :param consumer: Consumer de Kafka
    :param partitions: Particiones asignadas
    :param messages_to_look_back: Cantidad de mensajes a retroceder
    :param logger: Logger para mensajes
    """
    consumer.seek_to_end()
    end_offsets = consumer.end_offsets(partitions)
    
    for partition in partitions:
        end_offset = end_offsets[partition]
        new_offset = max(0, end_offset - messages_to_look_back)
        logger.info(f"Partición {partition.partition}: Retrocediendo desde offset {end_offset} a {new_offset}")
        consumer.seek(partition, new_offset)


def get_message(cloud_event_id: str, topic: str, group: str, max_polls=3) -> Dict[str, Any]:
    """
    Obtiene un mensaje especifico de Kafka por su cloud_event_id.

    :param cloud_event_id: id del evento a buscar
    :param topic: topico de Kafka
    :param group: grupo consumidor
    :param max_polls: numero maximo de intentos de poll
    :return: Un diccionario con la estructura {data: {key, value, headers}} si se encuentra el mensaje,
             o None si no se encuentra o hay un error
    """
    # Configuracion de logging
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    logger = logging.getLogger(__name__)

    consumer = None

    try:
        logger.info(f"Buscando mensaje con cloud_event_id: {cloud_event_id} en topic: {topic}")

        bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
        
        # Configuracion segun el entorno
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
            # Agregar configuracion SSL solo para produccion
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
            f"No se encontro el mensaje con cloud_event_id: {cloud_event_id} despues de {max_polls} intentos")
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


def get_message_v2(header_key: str, header_value: str, topic: str, group: str, max_polls=3, initial_wait_seconds=0) -> List[Dict[str, Any]]:
    """
    Obtiene mensajes de Kafka por una cabecera específica.
    Estrategia: primero busca en mensajes recientes (historial), luego espera mensajes nuevos.

    :param header_key: nombre de la cabecera a buscar
    :param header_value: valor de la cabecera a buscar
    :param topic: topico de Kafka
    :param group: grupo consumidor
    :param max_polls: numero maximo de intentos de poll
    :param initial_wait_seconds: tiempo de espera inicial antes del primer poll (para dar tiempo a que el mensaje se propague)
    :return: Lista de mensajes encontrados o None si hay error
    """
    logger = _setup_logger()
    consumer = None

    try:
        logger.info(f"Buscando mensaje con {header_key}: {header_value} en topic: {topic}")
        
        # Configurar entorno y consumer
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
        kafka_config = _get_kafka_config(group, is_local, enable_auto_commit=False)
        
        # Usar assign() en lugar de subscribe() para evitar el rebalance del grupo
        # que puede tardar varios segundos. Obtenemos las particiones del tópico directamente.
        consumer = KafkaConsumer(**kafka_config)
        
        # Obtener particiones del tópico sin necesidad de rebalance
        topic_partitions = consumer.partitions_for_topic(topic)
        
        if not topic_partitions:
            # Fallback: intentar con subscribe si no se obtienen particiones directamente
            logger.warning(f"No se obtuvieron particiones directamente para {topic}, usando subscribe()")
            consumer.subscribe([topic])
            max_wait_time = 10
            wait_interval = 0.5
            elapsed = 0
            partitions = set()
            while not partitions and elapsed < max_wait_time:
                consumer.poll(timeout_ms=100)
                partitions = consumer.assignment()
                if not partitions:
                    time.sleep(wait_interval)
                    elapsed += wait_interval
            if not partitions:
                logger.error(f"No se pudieron asignar particiones después de {max_wait_time} segundos")
                return None
        else:
            # Asignar particiones directamente (sin rebalance, mucho más rápido)
            partitions = {TopicPartition(topic, p) for p in topic_partitions}
            consumer.assign(list(partitions))
            logger.info(f"Particiones asignadas directamente (sin rebalance): {[p.partition for p in partitions]}")
        
        logger.info(f"Particiones listas: {[p.partition for p in partitions]}")
        
        # Espera inicial antes de consumir, para dar tiempo a que el mensaje se propague en Kafka
        if initial_wait_seconds > 0:
            logger.info(f"Esperando {initial_wait_seconds}s antes del primer poll (initial_wait_seconds)")
            time.sleep(initial_wait_seconds)
        
        # Fase 1: Buscar en mensajes recientes (historial)
        # Retrocedemos N mensajes desde el final para buscar si el mensaje ya existe
        messages_to_look_back = kafka_config["max_poll_records"]
        _seek_to_recent_messages(consumer, partitions, messages_to_look_back, logger)
        
        # Buscar mensajes - los polls ahora leen tanto historial como mensajes nuevos
        # porque después de consumir el historial, el consumer sigue leyendo mensajes
        # que lleguen al tópico (no se detiene en el end_offset capturado)
        response = []
        for poll_count in range(1, max_polls + 1):
            logger.info(f"Poll intento {poll_count}/{max_polls}")
            
            # Usar timeout más largo para dar tiempo a que lleguen mensajes nuevos
            records = consumer.poll(timeout_ms=7000)
            if not records:
                logger.info("No se encontraron registros en este poll")
                continue
            
            # Procesar mensajes
            messages_processed = 0
            for topic_partition, consumer_records in records.items():
                for message in consumer_records:
                    messages_processed += 1
                    headers = _decode_headers(message.headers)
                    
                    if headers.get(header_key) == header_value:
                        try:
                            body_obj = _parse_message_value(message.value)
                            consumer_response = _build_response(message.key, body_obj, headers)
                            
                            logger.info(f"Mensaje encontrado con {header_key}: {header_value} en offset {message.offset}")
                            response.append(consumer_response)
                            
                        except (UnicodeDecodeError, json.JSONDecodeError) as err:
                            logger.error(f"Error al procesar mensaje: {str(err)}")
                            return None
            
            logger.info(f"Poll {poll_count}: procesados {messages_processed} mensajes, encontrados {len(response)}")
            
            if response:
                return response
        
        logger.warning(f"No se encontro el mensaje con {header_key}: {header_value} despues de {max_polls} intentos")
        return response

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
