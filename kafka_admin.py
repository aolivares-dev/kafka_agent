import logging
import os
from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer
from typing import List, Optional
import aws_utils


def delete_topics(topics: Optional[List[str]] = None) -> dict:
    """
    Elimina los registros de los tópicos especificados o de todos los tópicos si no se especifica ninguno.
    
    :param topics: Lista de nombres de tópicos a limpiar. Si es None o vacía, limpia todos los tópicos.
    :return: Diccionario con el resultado de la operación
    """
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    logger = logging.getLogger(__name__)
    
    admin_client = None
    consumer = None
    
    try:
        logger.info("Iniciando proceso de limpieza de tópicos")
        
        # Obtener bootstrap servers
        bootstrap_servers = aws_utils.get_param("/config/all/common/kafka/boostrap-servers-tls").split(",")
        
        # Configuración según el entorno
        is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
        
        logger.info(f"Modo de operación: {'LOCAL' if is_local else 'PRODUCTION'}")
        logger.info(f"Bootstrap servers: {bootstrap_servers}")
        
        # Configuración del admin client
        admin_config = {
            "bootstrap_servers": bootstrap_servers,
            "request_timeout_ms": 30000
        }
        
        # Configuración del consumer para listar tópicos
        consumer_config = {
            "bootstrap_servers": bootstrap_servers,
            "request_timeout_ms": 30000,
            "group_id": "admin-cleanup-group"
        }
        
        if not is_local:
            # Agregar configuración SSL solo para producción
            admin_config["ssl_check_hostname"] = False
            admin_config["security_protocol"] = "SSL"
            consumer_config["ssl_check_hostname"] = False
            consumer_config["security_protocol"] = "SSL"
        
        # Crear admin client
        admin_client = KafkaAdminClient(**admin_config)
        
        # Si no se especifican tópicos o la lista está vacía, obtener todos los tópicos disponibles
        if topics is None or len(topics) == 0:
            consumer = KafkaConsumer(**consumer_config)
            all_topics = consumer.topics()
            # Filtrar tópicos internos de Kafka
            topics = [topic for topic in all_topics if not topic.startswith('__')]
            logger.info(f"No se especificaron tópicos. Se limpiarán todos los tópicos: {topics}")
        else:
            logger.info(f"Tópicos a limpiar: {topics}")
        
        if not topics:
            return {
                "status": {
                    "code": 200,
                    "message": "No hay tópicos para limpiar"
                },
                "data": {
                    "topics_deleted": []
                }
            }
        
        # Eliminar los tópicos
        result = admin_client.delete_topics(topics, timeout_ms=30000)
        
        # Esperar a que se completen las eliminaciones
        deleted_topics = []
        failed_topics = []
        
        for topic, future in result.items():
            try:
                future.result()  # Esperar a que se complete la operación
                deleted_topics.append(topic)
                logger.info(f"Tópico '{topic}' eliminado exitosamente")
            except Exception as e:
                failed_topics.append({"topic": topic, "error": str(e)})
                logger.error(f"Error al eliminar el tópico '{topic}': {str(e)}")
        
        if failed_topics:
            return {
                "status": {
                    "code": 207,  # Multi-Status
                    "message": "Algunos tópicos no pudieron ser eliminados"
                },
                "data": {
                    "topics_deleted": deleted_topics,
                    "topics_failed": failed_topics
                }
            }
        
        return {
            "status": {
                "code": 200,
                "message": "Tópicos eliminados exitosamente"
            },
            "data": {
                "topics_deleted": deleted_topics
            }
        }
        
    except Exception as e:
        logger.error(f"Error al eliminar tópicos: {str(e)}", exc_info=True)
        return {
            "status": {
                "code": 500,
                "message": f"Error al eliminar tópicos: {str(e)}"
            },
            "data": {
                "topics_deleted": []
            }
        }
    finally:
        if admin_client:
            try:
                admin_client.close()
                logger.info("Admin client cerrado correctamente")
            except Exception as e:
                logger.error(f"Error al cerrar el admin client: {str(e)}")
        
        if consumer:
            try:
                consumer.close()
                logger.info("Consumer cerrado correctamente")
            except Exception as e:
                logger.error(f"Error al cerrar el consumer: {str(e)}")
