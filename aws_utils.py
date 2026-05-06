import os


def get_client(resource: str):
    import boto3

    return boto3.client(resource, region_name=os.getenv("AWS_REGION"))


def get_param(param_name: str) -> str:
    ssm = get_client("ssm")

    value = ssm.get_parameter(Name=param_name)

    return value["Parameter"]["Value"]
import os
from dotenv import load_dotenv


def get_client(resource: str):
    import boto3

    load_dotenv()
    return boto3.client(resource, region_name=os.getenv("AWS_REGION"))


def get_param(param_name: str) -> str:
    load_dotenv()
    
    # Para desarrollo local, usar variable de entorno si existe
    is_local = os.getenv("ENV") == "local" or os.getenv("USE_LOCAL_KAFKA") == "true"
    
    if is_local:
        # Mapeo de parámetros a variables de entorno locales
        param_mapping = {
            "/config/all/common/kafka/boostrap-servers-tls": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        }
        
        if param_name in param_mapping:
            return param_mapping[param_name]
    
    # Para producción, usar AWS SSM
    ssm = get_client("ssm")
    value = ssm.get_parameter(Name=param_name)
    return value["Parameter"]["Value"]
