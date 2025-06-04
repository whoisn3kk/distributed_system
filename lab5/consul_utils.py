# Пример утилит для работы с Consul
import consul
import logging
import atexit
import socket # Для получения hostname/IP
import uuid

logger = logging.getLogger(__name__) # Используйте логгер вашего сервиса

CONSUL_HOST = "localhost" # Адрес агента Consul
CONSUL_PORT = 8500

# Глобальный клиент Consul для каждого сервиса
try:
    consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)
except Exception as e:
    logger.error(f"Failed to initialize Consul client: {e}")
    consul_client = None

def get_service_id(service_name, port):
    # Генерируем уникальный ID для экземпляра сервиса
    # Можно использовать hostname, но для локального запуска на одном хосте порт + UUID надежнее
    return f"{service_name}-{port}-{str(uuid.uuid4())[:8]}"


def register_service(service_name, service_port, health_check_path="/health", is_http_service=True):
    if not consul_client:
        logger.error("Consul client not available. Cannot register service.")
        return None

    host_ip = socket.gethostbyname(socket.gethostname()) # или "127.0.0.1" для локальной регистрации
    service_id = get_service_id(service_name, service_port)
    
    check_definition = None
    if is_http_service and health_check_path:
        check_definition = {
            "name": f"{service_name} HTTP Check on port {service_port}",
            "http": f"http://{host_ip}:{service_port}{health_check_path}",
            "interval": "10s",
            "timeout": "5s",
            "deregistercriticalserviceafter": "30s" # Correct key for auto-deregistration
        }
    elif not is_http_service:
        check_definition = {
            "name": f"{service_name} TCP Check on port {service_port}",
            "tcp": f"{host_ip}:{service_port}",
            "interval": "10s",
            "timeout": "5s",
            "deregistercriticalserviceafter": "30s"
        }


    try:
        consul_client.agent.service.register(
            name=service_name,
            service_id=service_id,
            address=host_ip, # Адрес, на котором сервис доступен
            port=service_port,
            check=check_definition,
            tags=[service_name, "python"]
        )
        logger.info(f"Service '{service_name}' registered with Consul. ID: {service_id}, Address: {host_ip}:{service_port}")
        
        # Зарегистрировать функцию дерегистрации при выходе
        atexit.register(deregister_service, service_id)
        return service_id
    except Exception as e:
        logger.error(f"Failed to register service {service_name} with Consul: {e}")
        return None

def deregister_service(service_id):
    if not consul_client:
        logger.warning("Consul client not available. Cannot deregister service.")
        return
    try:
        consul_client.agent.service.deregister(service_id=service_id)
        logger.info(f"Service ID '{service_id}' deregistered from Consul.")
    except Exception as e:
        logger.error(f"Failed to deregister service {service_id} from Consul: {e}")


def get_config_from_kv(key, default=None):
    if not consul_client:
        logger.error("Consul client not available. Cannot fetch config from KV.")
        return default
    try:
        index, data = consul_client.kv.get(key)
        if data is not None:
            value = data['Value'].decode('utf-8')
            logger.info(f"Fetched config for key '{key}' from Consul KV: {value}")
            return value
        else:
            logger.warning(f"Key '{key}' not found in Consul KV. Using default: {default}")
            return default
    except Exception as e:
        logger.error(f"Error fetching key '{key}' from Consul KV: {e}")
        return default

def discover_services(service_name):
    if not consul_client:
        logger.error("Consul client not available. Cannot discover services.")
        return []
    try:
        # Получаем только здоровые экземпляры сервиса
        index, services_data = consul_client.health.service(service_name, passing=True)
        addresses = []
        for entry in services_data:
            # 'ServiceAddress' может быть пустым, если Consul Agent не знает IP, тогда используем 'Address'
            address = entry['Service']['Address']
            if not address: # Если Service.Address пуст, используем Node.Address
                address = entry['Node']['Address']
            port = entry['Service']['Port']
            addresses.append(f"{address}:{port}")
        
        logger.info(f"Discovered {len(addresses)} healthy instances for service '{service_name}': {addresses}")
        return addresses
    except Exception as e:
        logger.error(f"Error discovering service '{service_name}' from Consul: {e}")
        return []

# Ключи конфигурации в Consul KV (примеры)
KAFKA_BROKERS_KV_KEY = "config/kafka/brokers"
KAFKA_MESSAGES_TOPIC_KV_KEY = "config/kafka/messages_topic"
KAFKA_MESSAGES_GROUP_KV_KEY = "config/kafka/messages_consumer_group"
HAZELCAST_MEMBERS_KV_KEY = "config/hazelcast/members"
HAZELCAST_CLUSTER_NAME_KV_KEY = "config/hazelcast/cluster_name"