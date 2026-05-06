from enum import Enum


class HeadersEnum(Enum):
    ip = ("ip", "ip de la peticion.")
    node = ("node", "node de la peticion.")
    date = ("date", "date de la peticion.")
    api_key = ("apikey", "api key de la peticion.")
    client_id = ("clientid", "client id de la peticion.")
    client_type = ("clienttype", "client type de la peticion.")
    trace_id = ("traceid", "trace id de la peticion.")
    role = ("role", "role de la peticion.")
    partition_key = ("partitionkey", "partition key de la peticion")
    content_type = ("content-type", "content type de la peticion.")
    specversion = ("specversion", "specversion de cloud event.")
    type = ("type", "tipo del evento.")
    source = ("source", "source del evento.")
    datacontenttype = ("datacontenttype", "datacontenttype")
    
    
    def __init__(self, key: str, description: str):
        self.key = key
        self.description = description